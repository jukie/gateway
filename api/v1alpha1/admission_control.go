// Copyright Envoy Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package v1alpha1

import (
	"time"
)

/*
     typed_config:
       '@type': type.googleapis.com/envoy.extensions.filters.http.admission_control.v3.AdmissionControl
       enabled:
         default_value: true
         runtime_key: admission_control.enabled
       sampling_window: 60s
       sr_threshold:
         default_value:
           value: 95.0
         runtime_key: admission_control.sr_threshold
       aggression:
         default_value: 1.0
         runtime_key: admission_control.aggression
       rps_threshold:
         default_value: 1
         runtime_key: admission_control.rps_threshold
       max_rejection_probability:
         default_value:
           value: 95.0
         runtime_key: admission_control.max_rejection_probability
       success_criteria:
         http_criteria:
           http_success_status:
           - start: 100
             end: 400
           - start: 404
             end: 404
         grpc_criteria:
           grpc_success_status:
           - 0
           - 1
   - name: envoy.filters.http.router

*/

type AdmissionControl struct {
	SamplingWindow *time.Duration `json:"samplingWindow,omitempty"`
	// Dictates the success rate at which the rejection probability is non-zero.
	// As success rate drops below this threshold, rejection probability will increase.
	// Any success rate above the threshold results in a rejection probability of 0.
	// Defaults to 95%.
	//
	// +optional
	SRThreshold *float64 `json:"srThreshold,omitempty"`

	// Rejection probability is defined by the formula:
	//
	// max(0, (rq_count -  rq_success_count / sr_threshold) / (rq_count + 1)) ^ (1 / aggression)
	// The aggression dictates how heavily the admission controller will throttle requests
	// upon SR dropping at or below the threshold. A value of 1 will result in a linear
	// increase in rejection probability as SR drops. Any values less than 1.0, will be
	// set to 1.0. If the message is unspecified, the aggression is 1.0. See the
	// admission control documentation for a diagram illustrating this.
	//
	// +optional
	Aggression *float64 `json:"agression,omitempty"`

	// RPSThreshold
	//
	// +optional
	RPSThreshold *int32 `json:"rpsThreshold,omitempty"`

	// MaxRejectionProbability
	//
	// +optional
	MaxRejectionProbability *float64 `json:"maxRejectionProbability,omitempty"`
	// SuccessCriteria
	//
	// +optional
	SuccessCriteria *SuccessCriteria `json:"successCriteria,omitempty"`
}

type SuccessCriteria struct {
	// HTTPCriteria
	//
	// +optional
	HTTPCriteria *HTTPCriteria `json:"httpCriteria,omitempty"`
	// GRPCCriteria
	//
	// +optional
	GRPCCriteria *GRPCCriteria `json:"grpcCriteria,omitempty"`
}

type HTTPCriteria struct {
	// GRPCCriteria
	//
	// +optional
	SuccessStatus []HTTPSuccessStatus `json:"httpSuccessStatus,omitempty"`
}

type HTTPSuccessStatus struct {
	Start int32 `json:"start,omitempty"`
	End   int32 `json:"end,omitempty"`
}

type GRPCCriteria struct {
	SuccessCriteria []int32 `json:"grpcSuccessStatus,omitempty"`
}
