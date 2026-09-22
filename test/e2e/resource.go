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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient"
)

// NewTestPod creates a baseline test pod requesting 5% of a single node's available CPU
// and 2% of a single node's available memory (~600m CPU, ~655Mi memory on KWOK).
func NewTestPod(name, namespace string) *corev1.Pod {
	return NewTestPodWithResourceFraction(name, namespace, 0.05, 0.02)
}

// NewTestPodWithResourceFraction creates a test pod requesting the specified fractions
// (0.0 < fraction <= 1.0) of a single node's available CPU (NodeCPU) and memory (NodeMemory).
func NewTestPodWithResourceFraction(name, namespace string, cpuFraction, memFraction float64) *corev1.Pod {
	return NewTestPodWithResources(
		name,
		namespace,
		testCfg.CalculateCPURequest(cpuFraction),
		testCfg.CalculateMemoryRequest(memFraction),
	)
}

// NewTestPodWithResources creates a test pod configuration with custom CPU and memory requests.
func NewTestPodWithResources(name, namespace, cpu, memory string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app": name,
			},
		},
		Spec: corev1.PodSpec{
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
