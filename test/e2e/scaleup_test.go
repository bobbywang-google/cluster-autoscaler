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
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/e2e-framework/klient/k8s"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

func TestClusterAutoscaling(t *testing.T) {
	pod := NewTestPod("fake-pod", testEnv.EnvConf().Namespace())

	scaleUpFeature := features.New("Cluster Autoscaler Scale Up").
		Assess("scale up when a pod is pending", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			// Create the pending pod
			err = client.Resources().Create(ctx, pod)
			if err != nil {
				t.Fatalf("failed to create pod: %v", err)
			}

			// Wait for TriggeredScaleUp event
			err = wait.For(func(ctx context.Context) (done bool, err error) {
				events := &corev1.EventList{}
				err = client.Resources(pod.Namespace).List(ctx, events)
				if err != nil {
					return false, err
				}
				for _, event := range events.Items {
					if event.InvolvedObject.Name == pod.Name && event.Reason == "TriggeredScaleUp" {
						return true, nil
					}
				}
				return false, nil
			}, wait.WithTimeout(testCfg.PodSchedulingTimeout), wait.WithContext(ctx))
			if err != nil {
				t.Fatalf("TriggeredScaleUp event not found: %v", err)
			}

			// Wait for the pod to be scheduled
			err = wait.For(conditions.New(client.Resources()).ResourceMatch(pod, func(object k8s.Object) bool {
				p := object.(*corev1.Pod)
				return p.Spec.NodeName != ""
			}), wait.WithTimeout(testCfg.PodSchedulingTimeout), wait.WithContext(ctx))
			if err != nil {
				t.Fatalf("pod not scheduled: %v", err)
			}

			// Verify new node is created
			nodeList := &corev1.NodeList{}
			err = wait.For(func(ctx context.Context) (done bool, err error) {
				err = client.Resources().List(ctx, nodeList)
				if err != nil {
					return false, err
				}
				for _, node := range nodeList.Items {
					if node.Labels[testCfg.NodeGroupLabelKey] == testCfg.NodeGroup {
						return true, nil
					}
				}
				return false, nil
			}, wait.WithTimeout(testCfg.NodeReadyTimeout), wait.WithContext(ctx))
			if err != nil {
				t.Fatalf("%s node not created: %v", testCfg.NodeGroup, err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			TeardownPodAndNodeGroup(ctx, client, []*corev1.Pod{pod}, testCfg.NodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, scaleUpFeature)
}

func TestScaleUpPendingPodTooLarge(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()

	// Pod requests 110% of a single node's memory, so it cannot fit on any node in the node group
	pod := NewTestPodWithResourceFraction("too-large-pod", ns, 0.05, 1.1)

	feature := features.New("Scale Up Pending Pod Too Large").
		Assess("do not increase cluster size if pending pod is too large", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			err = client.Resources().Create(ctx, pod)
			if err != nil {
				t.Fatalf("failed to create pod: %v", err)
			}

			// Cluster Autoscaler should evaluate the pod and decide not to scale up
			err = WaitForPodEvent(ctx, client, pod, "NotTriggerScaleUp", testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("NotTriggerScaleUp event not found: %v", err)
			}

			err = WaitForNodeCountConsistently(ctx, client, testCfg.NodeGroup, 0, testCfg.NoScaleUpWindow)
			if err != nil {
				t.Fatalf("cluster scaled up for a pod that does not fit on any node: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			TeardownPodAndNodeGroup(ctx, client, []*corev1.Pod{pod}, testCfg.NodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

func TestScaleUpNoAdditionalScaleUpsDuringProcessing(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()

	// 3 pods requesting 60% CPU each, so no two pods fit on the same node and exactly 3 nodes are needed
	var pods []*corev1.Pod
	for _, name := range []string{"no-extra-scale-up-pod-1", "no-extra-scale-up-pod-2", "no-extra-scale-up-pod-3"} {
		pods = append(pods, NewTestPodWithResourceFraction(name, ns, 0.60, 0.02))
	}

	feature := features.New("Scale Up No Additional Scale Ups During Processing").
		Assess("do not trigger additional scale-ups during processing scale-up", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			for _, pod := range pods {
				err = client.Resources().Create(ctx, pod)
				if err != nil {
					t.Fatalf("failed to create pod %s: %v", pod.Name, err)
				}
			}

			err = WaitForNodesReady(ctx, client, testCfg.NodeGroup, 3, testCfg.NodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 3 nodes: %v", err)
			}

			err = WaitForPodsScheduled(ctx, client, pods, testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("pods were not scheduled: %v", err)
			}

			// All pods are scheduled, so Cluster Autoscaler should not add any more nodes
			err = WaitForNodeCountConsistently(ctx, client, testCfg.NodeGroup, 3, testCfg.NoScaleUpWindow)
			if err != nil {
				t.Fatalf("unexpected additional scale-up: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			TeardownPodAndNodeGroup(ctx, client, pods, testCfg.NodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

func TestScaleUpHostPortConflict(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()

	// Both pods are small enough to share a node, but they bind the same host port,
	// so each of them needs its own node
	pod1 := NewTestPodWithHostPort("host-port-pod-1", ns, 8080)
	pod2 := NewTestPodWithHostPort("host-port-pod-2", ns, 8080)
	pods := []*corev1.Pod{pod1, pod2}

	feature := features.New("Scale Up Host Port Conflict").
		Assess("increase cluster size if pods are pending due to host port conflict", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			for _, pod := range pods {
				err = client.Resources().Create(ctx, pod)
				if err != nil {
					t.Fatalf("failed to create pod %s: %v", pod.Name, err)
				}
			}

			err = WaitForNodesReady(ctx, client, testCfg.NodeGroup, 2, testCfg.NodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 2 nodes: %v", err)
			}

			err = WaitForPodsScheduled(ctx, client, pods, testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("pods were not scheduled: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			TeardownPodAndNodeGroup(ctx, client, pods, testCfg.NodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

func TestScaleUpPodAntiAffinity(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()

	// All pods are small enough to share a node, but their mutual anti-affinity
	// allows at most one of them per node
	existingPod := NewTestPodWithAntiAffinity("anti-affinity-pod", ns, "anti-affinity", "yes")
	extraPod1 := NewTestPodWithAntiAffinity("anti-affinity-extra-pod-1", ns, "anti-affinity", "yes")
	extraPod2 := NewTestPodWithAntiAffinity("anti-affinity-extra-pod-2", ns, "anti-affinity", "yes")
	extraPods := []*corev1.Pod{extraPod1, extraPod2}

	feature := features.New("Scale Up Pod Anti Affinity").
		Assess("increase cluster size if pods are pending due to pod anti-affinity", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			// Start with one anti-affinity pod on a single node
			err = client.Resources().Create(ctx, existingPod)
			if err != nil {
				t.Fatalf("failed to create pod %s: %v", existingPod.Name, err)
			}

			err = WaitForNodesReady(ctx, client, testCfg.NodeGroup, 1, testCfg.NodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 1 node: %v", err)
			}

			err = WaitForPodScheduled(ctx, client, existingPod, testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("pod %s was not scheduled: %v", existingPod.Name, err)
			}

			// Each extra pod has anti-affinity to the existing pod and to each other, so it needs a new node
			for _, pod := range extraPods {
				err = client.Resources().Create(ctx, pod)
				if err != nil {
					t.Fatalf("failed to create pod %s: %v", pod.Name, err)
				}
			}

			err = WaitForNodesReady(ctx, client, testCfg.NodeGroup, 3, testCfg.NodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 3 nodes: %v", err)
			}

			err = WaitForPodsScheduled(ctx, client, extraPods, testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("extra pods were not scheduled: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			TeardownPodAndNodeGroup(ctx, client, []*corev1.Pod{existingPod, extraPod1, extraPod2}, testCfg.NodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}

func TestScaleUpEmptyDirVolume(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()

	// The EmptyDir pod is small enough to share a node with the existing pod,
	// but its anti-affinity forces it onto a new node
	existingPod := NewTestPodWithAntiAffinity("empty-dir-anti-affinity-pod", ns, "anti-affinity-empty-dir", "yes")
	emptyDirPod := NewTestPodWithEmptyDirAndAntiAffinity("empty-dir-pod", ns, "anti-affinity-empty-dir", "yes")

	feature := features.New("Scale Up EmptyDir Volume").
		Assess("increase cluster size if pod requesting EmptyDir volume is pending", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			// Start with one anti-affinity pod on a single node
			err = client.Resources().Create(ctx, existingPod)
			if err != nil {
				t.Fatalf("failed to create pod %s: %v", existingPod.Name, err)
			}

			err = WaitForNodesReady(ctx, client, testCfg.NodeGroup, 1, testCfg.NodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 1 node: %v", err)
			}

			err = WaitForPodScheduled(ctx, client, existingPod, testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("pod %s was not scheduled: %v", existingPod.Name, err)
			}

			// Create the pod requesting an EmptyDir volume
			err = client.Resources().Create(ctx, emptyDirPod)
			if err != nil {
				t.Fatalf("failed to create pod %s: %v", emptyDirPod.Name, err)
			}

			err = WaitForNodesReady(ctx, client, testCfg.NodeGroup, 2, testCfg.NodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 2 nodes: %v", err)
			}

			err = WaitForPodScheduled(ctx, client, emptyDirPod, testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("pod %s was not scheduled: %v", emptyDirPod.Name, err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			TeardownPodAndNodeGroup(ctx, client, []*corev1.Pod{existingPod, emptyDirPod}, testCfg.NodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}
