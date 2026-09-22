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
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

func TestScaleDownReschedulingPodAllowedByPDB(t *testing.T) {
	ns := testEnv.EnvConf().Namespace()

	// Pods protected by PDB, sized as fractions of a single node's available resources (NodeCPU, NodeMemory)
	// so the test works across both KWOK (default 12 cores, 32Gi) and cloud providers with different node sizes:
	// - pdbPod1 (10% CPU) + fillerPod (80% CPU) = 90% CPU (fits on 1st node, leaving 10% free).
	// - pdbPod2 (20% CPU) cannot fit on the 1st node (90% + 20% = 110% > 100%), forcing a 2nd node.
	// - Once fillerPod is deleted, pdbPod1 (10%) + pdbPod2 (20%) = 30% CPU (both fit on 1 node).
	pdbPod1 := NewTestPodWithResourceFraction("pdb-pod-1", ns, 0.10, 0.02)
	pdbPod1.Labels["app"] = "pdb-test"
	pdbPod1.Annotations = map[string]string{
		"cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
	}

	pdbPod2 := NewTestPodWithResourceFraction("pdb-pod-2", ns, 0.20, 0.02)
	pdbPod2.Labels["app"] = "pdb-test"
	pdbPod2.Annotations = map[string]string{
		"cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
	}

	fillerPod := NewTestPodWithResourceFraction("filler-pod", ns, 0.80, 0.02)

	minAvailable := intstr.FromInt(1)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pdb-test-budget",
			Namespace: ns,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": "pdb-test",
				},
			},
			MinAvailable: &minAvailable,
		},
	}

	feature := features.New("Scale Down When Rescheduling A Pod Is Required And PDB Allows For It").
		Assess("scale down when rescheduling a pod is required and pdb allows for it", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			// Create PDB allowing at most 1 pod disrupted (minAvailable=1 with 2 replicas)
			err = client.Resources().Create(ctx, pdb)
			if err != nil {
				t.Fatalf("failed to create PDB: %v", err)
			}

			// Step 1: Create pdbPod1 and fillerPod to fill the first node
			for _, pod := range []*corev1.Pod{pdbPod1, fillerPod} {
				err = client.Resources().Create(ctx, pod)
				if err != nil {
					t.Fatalf("failed to create pod %s: %v", pod.Name, err)
				}
			}

			// Wait for the first node to become ready and pods scheduled
			err = WaitForNodesReady(ctx, client, testCfg.NodeGroup, 1, testCfg.NodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 1 node: %v", err)
			}
			err = WaitForPodsScheduled(ctx, client, []*corev1.Pod{pdbPod1, fillerPod}, testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("pods were not scheduled: %v", err)
			}

			// Step 2: Create pdbPod2 which cannot fit on the first node, forcing scale-up of a second node.
			err = client.Resources().Create(ctx, pdbPod2)
			if err != nil {
				t.Fatalf("failed to create pod %s: %v", pdbPod2.Name, err)
			}

			// Wait for both nodes to become ready and pdbPod2 scheduled on the second node
			err = WaitForNodesReady(ctx, client, testCfg.NodeGroup, 2, testCfg.NodeReadyTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale up to 2 nodes: %v", err)
			}
			err = WaitForPodScheduled(ctx, client, pdbPod2, testCfg.PodSchedulingTimeout)
			if err != nil {
				t.Fatalf("pod %s was not scheduled: %v", pdbPod2.Name, err)
			}

			// Step 3: Delete filler pod so the first node now has plenty of room
			err = client.Resources().Delete(ctx, fillerPod)
			if err != nil {
				t.Fatalf("failed to delete filler pod: %v", err)
			}
			_ = WaitForPodDeleted(ctx, client, fillerPod, testCfg.PodDeletionTimeout)

			// Step 4: Cluster Autoscaler should drain pdbPod2 (allowed by PDB) and scale down to 1 node
			err = WaitForNodeCount(ctx, client, testCfg.NodeGroup, 1, testCfg.ScaleDownTimeout)
			if err != nil {
				t.Fatalf("cluster did not scale down from 2 nodes to 1 node: %v", err)
			}

			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client, err := cfg.NewClient()
			if err != nil {
				t.Fatal(err)
			}
			_ = client.Resources().Delete(ctx, pdb)
			TeardownPodAndNodeGroup(ctx, client, []*corev1.Pod{pdbPod1, fillerPod, pdbPod2}, testCfg.NodeGroup)
			return ctx
		}).
		Feature()

	testEnv.Test(t, feature)
}
