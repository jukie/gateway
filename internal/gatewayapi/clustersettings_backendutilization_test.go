// Copyright Envoy Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package gatewayapi

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	egv1a1 "github.com/envoyproxy/gateway/api/v1alpha1"
	"github.com/envoyproxy/gateway/internal/ir"
)

func TestBuildLoadBalancer_BackendUtilization(t *testing.T) {
	backendUtilization := &egv1a1.BackendUtilization{
		BlackoutPeriod:                     new(gwapiv1.Duration("10s")),
		WeightExpirationPeriod:             new(gwapiv1.Duration("3m")),
		WeightUpdatePeriod:                 new(gwapiv1.Duration("1s")),
		ErrorUtilizationPenaltyPercent:     new(uint32(150)),
		MetricNamesForComputingUtilization: []string{"named_metrics.foo", "cpu_utilization"},
	}

	policy := &egv1a1.ClusterSettings{
		LoadBalancer: &egv1a1.LoadBalancer{
			Type:               egv1a1.BackendUtilizationLoadBalancerType,
			BackendUtilization: backendUtilization,
		},
	}

	lb, err := buildLoadBalancer(policy)
	require.NoError(t, err)
	require.NotNil(t, lb)
	require.NotNil(t, lb.BackendUtilization)

	got := lb.BackendUtilization
	require.Equal(t, ir.MetaV1DurationPtr(10*time.Second), got.BlackoutPeriod)
	require.Equal(t, ir.MetaV1DurationPtr(3*time.Minute), got.WeightExpirationPeriod)
	require.Equal(t, ir.MetaV1DurationPtr(1*time.Second), got.WeightUpdatePeriod)
	require.NotNil(t, got.ErrorUtilizationPenaltyPercent)
	require.EqualValues(t, 150, *got.ErrorUtilizationPenaltyPercent)
	require.Equal(t, []string{"named_metrics.foo", "cpu_utilization"}, got.MetricNamesForComputingUtilization)
}

func TestBuildLoadBalancer_BackendUtilizationOOB(t *testing.T) {
	policy := &egv1a1.ClusterSettings{
		LoadBalancer: &egv1a1.LoadBalancer{
			Type: egv1a1.BackendUtilizationLoadBalancerType,
			BackendUtilization: &egv1a1.BackendUtilization{
				OOB: &egv1a1.OOBReporting{
					ReportingPeriod: new(gwapiv1.Duration("5s")),
					Port:            new(uint32(9001)),
					Authority:       new("orca.local"),
				},
			},
		},
	}

	lb, err := buildLoadBalancer(policy)
	require.NoError(t, err)
	require.NotNil(t, lb.BackendUtilization)
	require.NotNil(t, lb.BackendUtilization.OOB)

	oob := lb.BackendUtilization.OOB
	require.Equal(t, ir.MetaV1DurationPtr(5*time.Second), oob.ReportingPeriod)
	require.NotNil(t, oob.Port)
	require.EqualValues(t, 9001, *oob.Port)
	require.NotNil(t, oob.Authority)
	require.Equal(t, "orca.local", *oob.Authority)
}

func TestBuildLoadBalancer_BackendUtilizationOOBEmpty(t *testing.T) {
	policy := &egv1a1.ClusterSettings{
		LoadBalancer: &egv1a1.LoadBalancer{
			Type: egv1a1.BackendUtilizationLoadBalancerType,
			BackendUtilization: &egv1a1.BackendUtilization{
				OOB: &egv1a1.OOBReporting{},
			},
		},
	}

	lb, err := buildLoadBalancer(policy)
	require.NoError(t, err)
	require.NotNil(t, lb.BackendUtilization.OOB)
	require.Nil(t, lb.BackendUtilization.OOB.ReportingPeriod)
	require.Nil(t, lb.BackendUtilization.OOB.Port)
	require.Nil(t, lb.BackendUtilization.OOB.Authority)
}
