// Copyright Envoy Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package gatewayapi

import (
	egv1a1 "github.com/envoyproxy/gateway/api/v1alpha1"
	"github.com/envoyproxy/gateway/internal/gatewayapi/resource"
	"github.com/envoyproxy/gateway/internal/ir"
	"github.com/envoyproxy/gateway/internal/utils"
	corev1 "k8s.io/api/core/v1"
)

func (t *Translator) processEnvoyServiceDestinationSetting(
	name string,
	service *corev1.Service,
	resources *resource.Resources,
	envoyProxy *egv1a1.EnvoyProxy,
) *ir.DestinationSetting {
	var (
		endpoints []*ir.DestinationEndpoint
		addrType  *ir.DestinationAddressType
	)

	var servicePort corev1.ServicePort
	if len(service.Spec.Ports) > 0 {
		servicePort = service.Spec.Ports[0]
	}

	// Route to endpoints by default
	if !t.IsEnvoyServiceRouting(envoyProxy) {
		endpointSlices := resources.GetEndpointSlicesForBackend(service.Namespace, service.Name, resource.KindService)
		endpoints, addrType = getIREndpointsFromEndpointSlices(endpointSlices, servicePort.Name, servicePort.Protocol)
	} else {
		// Fall back to Service ClusterIP routing
		ep := ir.NewDestEndpoint(service.Spec.ClusterIP, uint32(servicePort.Port), false, nil)
		endpoints = append(endpoints, ep)
	}

	return &ir.DestinationSetting{
		Name:                    name,
		Protocol:                ir.HTTP,
		Endpoints:               endpoints,
		AddressType:             addrType,
		ZoneAwareRoutingEnabled: isZoneAwareRoutingEnabled(service),
	}
}

func (t *Translator) ProcessProxyCluster(gateways []*GatewayContext, resources *resource.Resources, xdsIR resource.XdsIRMap, infraIR map[string]*ir.Infra) {

	for _, g := range gateways {
		if g == nil || g.Gateway == nil {
			continue
		}

		irKey := t.getIRKey(g.Gateway)
		gwInfra := infraIR[irKey]
		svcLabels := gwInfra.GetProxyInfra().GetProxyMetadata().Labels
		svc := resources.GetServiceByLabel(t.Namespace, svcLabels)
		if svc == nil {
			continue
		}

		ds := t.processEnvoyServiceDestinationSetting(svc.Name, svc, resources, g.envoyProxy)
		ds.IPFamily = getServiceIPFamily(svc)
		if ds == nil {
			continue
		}

		clusterName := utils.GetHashedName(infraIR[irKey].Proxy.Name, 64)
		xdsIR[irKey].LocalServiceCluster = &ir.LocalServiceCluster{
			Name:        clusterName,
			Destination: ds,
			Traffic:     nil,
		}
	}
}
