//go:build e2e

/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"context"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient"
)

// PodOption customizes a test pod built by NewTestPod.
type PodOption func(*corev1.Pod)

// NewTestPod creates a test pod targeting the test node group. By default it requests 5% of a
// single node's available CPU and 2% of its available memory (~600m CPU, ~655Mi memory on KWOK);
// opts customize it further and are applied in order.
func NewTestPod(name, namespace string, opts ...PodOption) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app": name,
			},
		},
		Spec: corev1.PodSpec{
			TerminationGracePeriodSeconds: new(int64),
			Containers: []corev1.Container{
				{
					Name:  "fake-container",
					Image: "fake-image",
				},
			},
			NodeSelector: map[string]string{
				testCfg.NodeGroupLabelKey: testCfg.NodeGroup,
			},
			Tolerations: testCfg.Tolerations,
		},
	}
	WithResourceFraction(0.05, 0.02)(pod)
	for _, opt := range opts {
		opt(pod)
	}
	return pod
}

// WithResourceFraction makes the pod request the specified fractions (0.0 < fraction <= 1.0)
// of a single node's available CPU (NodeCPU) and memory (NodeMemory).
func WithResourceFraction(cpuFraction, memFraction float64) PodOption {
	return WithResources(testCfg.CalculateCPURequest(cpuFraction), testCfg.CalculateMemoryRequest(memFraction))
}

// WithResources makes the pod request the given CPU and memory.
func WithResources(cpu, memory string) PodOption {
	return func(pod *corev1.Pod) {
		pod.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(cpu),
			corev1.ResourceMemory: resource.MustParse(memory),
		}
	}
}

// WithLabel sets the given label on the pod.
func WithLabel(key, value string) PodOption {
	return func(pod *corev1.Pod) {
		pod.Labels[key] = value
	}
}

// WithSafeToEvict marks the pod as safe to evict for Cluster Autoscaler.
func WithSafeToEvict() PodOption {
	return func(pod *corev1.Pod) {
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		pod.Annotations["cluster-autoscaler.kubernetes.io/safe-to-evict"] = "true"
	}
}

// WithPriorityClass sets the PriorityClassName of the pod.
func WithPriorityClass(priorityClassName string) PodOption {
	return func(pod *corev1.Pod) {
		pod.Spec.PriorityClassName = priorityClassName
	}
}

// WithHostPort makes the pod bind the given host port, so that at most one such pod can run on a node.
func WithHostPort(hostPort int32) PodOption {
	return func(pod *corev1.Pod) {
		pod.Spec.Containers[0].Ports = []corev1.ContainerPort{
			{
				ContainerPort: hostPort,
				HostPort:      hostPort,
			},
		}
	}
}

// WithAntiAffinity labels the pod labelKey=labelVal and adds required pod anti-affinity to other pods
// with the same label, so that at most one such pod can run on a node.
func WithAntiAffinity(labelKey, labelVal string) PodOption {
	return func(pod *corev1.Pod) {
		pod.Labels[labelKey] = labelVal
		pod.Spec.Affinity = &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
					{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								labelKey: labelVal,
							},
						},
						TopologyKey: corev1.LabelHostname,
					},
				},
			},
		}
	}
}

// WithEmptyDir makes the pod mount an EmptyDir volume.
func WithEmptyDir() PodOption {
	return func(pod *corev1.Pod) {
		pod.Spec.Volumes = []corev1.Volume{
			{
				Name: "empty-volume",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			},
		}
		pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{
			{
				Name:      "empty-volume",
				MountPath: "/scratch",
			},
		}
	}
}

// WithSchedulerName makes the pod be scheduled by the given scheduler.
func WithSchedulerName(schedulerName string) PodOption {
	return func(pod *corev1.Pod) {
		pod.Spec.SchedulerName = schedulerName
	}
}

// CleanUpNodeGroup deletes all fake nodes for the given nodeGroup on KWOK and waits until count is 0.
func CleanUpNodeGroup(ctx context.Context, client klient.Client, nodeGroup string) error {
	if testCfg.Provider != ProviderKWOK {
		return WaitForNodeCount(ctx, client, nodeGroup, 0, testCfg.ScaleDownTimeout)
	}
	nodeList := &corev1.NodeList{}
	err := client.Resources().List(ctx, nodeList)
	if err != nil {
		return err
	}
	for _, node := range nodeList.Items {
		if node.Labels[testCfg.NodeGroupLabelKey] == nodeGroup {
			n := node
			_ = client.Resources().Delete(ctx, &n)
		}
	}
	return WaitForNodeCount(ctx, client, nodeGroup, 0, 30*time.Second)
}

// TeardownPodAndNodeGroup deletes the test pods, waits for them to be deleted,
// and waits for Cluster Autoscaler to scale down the node group.
func TeardownPodAndNodeGroup(ctx context.Context, client klient.Client, pods []*corev1.Pod, nodeGroup string) {
	for _, pod := range pods {
		if pod != nil {
			_ = client.Resources().Delete(ctx, pod)
		}
	}
	_ = WaitForPodsDeleted(ctx, client, pods, testCfg.PodDeletionTimeout)
	if testCfg.Provider == ProviderKWOK {
		// Allow CA to scale down the empty node naturally, then force-clean any leftover fake nodes.
		_ = WaitForNodeCount(ctx, client, nodeGroup, 0, 45*time.Second)
		_ = CleanUpNodeGroup(ctx, client, nodeGroup)
	} else {
		// For non-KWOK cloud providers, wait for CA to scale down naturally using the provider-configured timeout.
		_ = WaitForNodeCount(ctx, client, nodeGroup, 0, testCfg.ScaleDownTimeout)
	}
}

// CountNodeGroupNodes returns the number of nodes currently matching the nodeGroup.
func CountNodeGroupNodes(ctx context.Context, client klient.Client, nodeGroup string) (int, error) {
	nodeList := &corev1.NodeList{}
	err := client.Resources().List(ctx, nodeList)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, node := range nodeList.Items {
		if node.Labels[testCfg.NodeGroupLabelKey] == nodeGroup {
			count++
		}
	}
	return count, nil
}

const (
	expendablePriorityClassName = "expendable-priority"
	highPriorityClassName       = "high-priority"
	expendablePriorityValue     = int32(-15)
	highPriorityValue           = int32(1000)
)

// EnsurePriorityClasses creates the expendable and high priority classes for testing.
func EnsurePriorityClasses(ctx context.Context, client klient.Client) error {
	for name, val := range map[string]int32{
		expendablePriorityClassName: expendablePriorityValue,
		highPriorityClassName:       highPriorityValue,
	} {
		pc := &schedulingv1.PriorityClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
			Value: val,
		}
		_ = client.Resources().Create(ctx, pc)
	}
	return nil
}

// DeletePriorityClasses cleans up priority classes created for testing.
func DeletePriorityClasses(ctx context.Context, client klient.Client) {
	for _, name := range []string{expendablePriorityClassName, highPriorityClassName} {
		pc := &schedulingv1.PriorityClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
		}
		_ = client.Resources().Delete(ctx, pc)
	}
}

// NewTestReplicaSet creates a test ReplicaSet requesting the specified CPU and memory fractions
// per replica with the safe-to-evict annotation.
func NewTestReplicaSet(name, namespace string, replicas int32, cpuFraction, memFraction float64, labelKey, labelVal string) *appsv1.ReplicaSet {
	labels := map[string]string{
		labelKey: labelVal,
	}
	cpu := testCfg.CalculateCPURequest(cpuFraction)
	memory := testCfg.CalculateMemoryRequest(memFraction)
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
					Annotations: map[string]string{
						"cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
					},
				},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: new(int64),
					Containers: []corev1.Container{
						{
							Name:  "fake-container",
							Image: "fake-image",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse(cpu),
									corev1.ResourceMemory: resource.MustParse(memory),
								},
							},
						},
					},
					NodeSelector: map[string]string{
						testCfg.NodeGroupLabelKey: testCfg.NodeGroup,
					},
					Tolerations: testCfg.Tolerations,
				},
			},
		},
	}
}

// DeletePodsWithLabel deletes all pods in namespace matching the given label key and value.
func DeletePodsWithLabel(ctx context.Context, client klient.Client, namespace, labelKey, labelVal string) error {
	podList := &corev1.PodList{}
	err := client.Resources(namespace).List(ctx, podList)
	if err != nil {
		return err
	}
	for _, pod := range podList.Items {
		if pod.Labels[labelKey] == labelVal {
			p := pod
			_ = client.Resources().Delete(ctx, &p)
		}
	}
	return nil
}
