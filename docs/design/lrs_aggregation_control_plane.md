# Design: LRS Aggregation and EDS Fraction Injection for Load-Aware Zone Routing

**Issue:** ga-181
**Status:** Design Proposal
**Author:** polecat/obsidian
**Date:** 2026-02-06

## Summary

This document designs the envoy-gateway control plane implementation for LRS-driven
zone-aware routing. The companion data-plane design (envoy rig's
`docs/design/load_aware_zone_routing_v3.md`) adds `LRS_REPORTED_RATE` mode and
`observed_traffic_fraction` to Envoy. This document covers how envoy-gateway:

1. Receives LRS (Load Reporting Service) reports from Envoy instances
2. Aggregates per-locality traffic data using EWMA smoothing
3. Injects `observed_traffic_fraction` into the local cluster's EDS response

envoy-gateway currently does **not** implement LRS. This design adds it.

## Problem Statement

Envoy's zone-aware routing computes per-locality traffic percentages from host counts
or weights. This assumes uniform traffic distribution ("30% of Envoy instances = 30%
of inbound traffic"), which breaks under BGP skew and heavy-client scenarios.

The v3 data-plane design solves this by introducing `LRS_REPORTED_RATE` mode where
Envoy reads control-plane-provided `observed_traffic_fraction` values from the local
cluster's EDS response. The control plane's job is to:

1. Collect LRS reports from all Envoy instances (actual request counts per locality)
2. Aggregate them into per-locality traffic fractions
3. Push fractions back via EDS

This document designs that control plane logic within envoy-gateway.

## Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                   envoy-gateway                      │
│                                                      │
│  ┌──────────────┐    ┌─────────────────────────┐    │
│  │  LRS Server   │───▶│  TrafficFractionStore   │    │
│  │  (new gRPC    │    │  (EWMA aggregation,     │    │
│  │   service)    │    │   per-irKey fractions)   │    │
│  └──────────────┘    └───────────┬─────────────┘    │
│                                   │                  │
│  ┌──────────────┐    ┌───────────▼─────────────┐    │
│  │  XdsIR        │───▶│  xDS Translator         │    │
│  │  Subscription │    │  (cluster.go)           │    │
│  └──────────────┘    │  reads fractions from    │    │
│                       │  store during EDS build  │    │
│                       └───────────┬─────────────┘    │
│                                   │                  │
│                       ┌───────────▼─────────────┐    │
│                       │  Snapshot Cache          │    │
│                       │  (EDS with fractions)    │    │
│                       └───────────┬─────────────┘    │
│                                   │                  │
└───────────────────────────────────┼──────────────────┘
                                    │ xDS (ADS/EDS)
                         ┌──────────▼──────────┐
                         │    Envoy Proxies     │
                         │  (send LRS reports,  │
                         │   receive EDS with   │
                         │   fractions)          │
                         └─────────────────────┘
```

## Detailed Design

### 1. LRS Server Implementation

envoy-gateway needs a new gRPC service: `envoy.service.load_stats.v3.LoadReportingService`.

#### 1.1 Service Registration

The LRS service runs on the **same gRPC server** as the existing xDS services (port 18000).
This is the simplest approach and reuses existing TLS, keepalive, and JWT auth
infrastructure.

**File:** `internal/xds/runner/runner.go`

The `registerServer` function gains one additional registration:

```go
import (
    loadstatsv3 "github.com/envoyproxy/go-control-plane/envoy/service/load_stats/v3"
)

func registerServer(srv serverv3.Server, g *grpc.Server, lrsSrv loadstatsv3.LoadReportingServiceServer) {
    // existing registrations...
    discoveryv3.RegisterAggregatedDiscoveryServiceServer(g, srv)
    // ...

    // New: LRS service
    loadstatsv3.RegisterLoadReportingServiceServer(g, lrsSrv)
}
```

**Why same server:** LRS is a lightweight bidirectional stream. A separate gRPC server
would require duplicating TLS config, keepalive params, JWT auth, and port management.
The existing server has capacity, and LRS traffic is modest (one stream per Envoy instance,
reports every 10-30s).

**Alternative considered: Separate runner.** Like `globalratelimit/runner/runner.go`,
LRS could have its own runner with a dedicated gRPC server on a different port. This
adds operational complexity (new port, new TLS config, new health check) without clear
benefit. Deferred unless load becomes an issue.

#### 1.2 LRS Service Handler

**New file:** `internal/xds/server/lrs/server.go`

The LRS protocol is a bidirectional gRPC stream:

1. Envoy opens stream and sends initial `LoadStatsRequest` (identifies node, clusters)
2. Control plane responds with `LoadStatsResponse` (tells Envoy what to report, interval)
3. Envoy periodically sends `LoadStatsRequest` with per-locality stats
4. Control plane responds with updated `LoadStatsResponse` after each request

```go
package lrs

import (
    "sync"
    "time"

    corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
    loadstatsv3 "github.com/envoyproxy/go-control-plane/envoy/service/load_stats/v3"
    "google.golang.org/protobuf/types/known/durationpb"

    "github.com/envoyproxy/gateway/internal/logging"
)

// Server implements the LRS gRPC service.
type Server struct {
    loadstatsv3.UnimplementedLoadReportingServiceServer

    store  *TrafficFractionStore
    logger logging.Logger

    // reportingInterval controls how often Envoy reports.
    // Default: 10s. Configurable via EnvoyGateway config.
    reportingInterval time.Duration
}

func NewServer(store *TrafficFractionStore, logger logging.Logger, interval time.Duration) *Server {
    return &Server{
        store:             store,
        logger:            logger,
        reportingInterval: interval,
    }
}

func (s *Server) StreamLoadStats(stream loadstatsv3.LoadReportingService_StreamLoadStatsServer) error {
    // Receive initial request (node identification)
    req, err := stream.Recv()
    if err != nil {
        return err
    }

    node := req.GetNode()
    s.logger.Info("LRS stream opened", "nodeID", node.GetId(), "cluster", node.GetCluster())

    // Send initial response telling Envoy what to report
    if err := stream.Send(&loadstatsv3.LoadStatsResponse{
        SendAllClusters:       true,
        LoadReportingInterval: durationpb.New(s.reportingInterval),
    }); err != nil {
        return err
    }

    // Main loop: receive reports, aggregate, respond
    for {
        req, err := stream.Recv()
        if err != nil {
            s.logger.Info("LRS stream closed", "nodeID", node.GetId(), "error", err)
            return err
        }

        // Process load stats from this Envoy instance
        s.store.ProcessLoadStatsRequest(node, req)

        // Respond with continued reporting config
        if err := stream.Send(&loadstatsv3.LoadStatsResponse{
            SendAllClusters:       true,
            LoadReportingInterval: durationpb.New(s.reportingInterval),
        }); err != nil {
            return err
        }
    }
}
```

#### 1.3 go-control-plane LRS Support

The `go-control-plane` dependency (v0.14.0) includes the LRS proto definitions at
`github.com/envoyproxy/go-control-plane/envoy/service/load_stats/v3`. The
`LoadReportingService` gRPC service interface and `LoadStatsRequest`/`LoadStatsResponse`
message types are available. No proto changes needed in envoy-gateway.

### 2. Traffic Fraction Store

**New file:** `internal/xds/server/lrs/store.go`

The store aggregates LRS reports from all Envoy instances and computes per-locality
traffic fractions.

#### 2.1 Data Model

```go
package lrs

// LocalityKey is a hashable representation of a Locality.
type LocalityKey struct {
    Region  string
    Zone    string
    SubZone string
}

// TrafficFractionStore aggregates LRS data and computes per-locality fractions.
type TrafficFractionStore struct {
    mu sync.RWMutex

    // Per irKey (cluster group) -> per locality -> EWMA rate
    // The irKey maps to the node.Cluster field from LRS streams,
    // which matches the snapshot cache's irKey concept.
    rates map[string]map[LocalityKey]*ewmaRate

    // Computed fractions per irKey -> per locality -> basis points (0-10000)
    fractions map[string]map[LocalityKey]uint32

    // EWMA smoothing factor (0-1). Higher = more responsive.
    ewmaAlpha float64

    // Last computation time per irKey
    lastComputed map[string]time.Time
}

type ewmaRate struct {
    value       float64
    lastUpdated time.Time
}
```

#### 2.2 EWMA Aggregation

When an LRS report arrives from an Envoy instance:

```go
func (s *TrafficFractionStore) ProcessLoadStatsRequest(
    node *corev3.Node,
    req *loadstatsv3.LoadStatsRequest,
) {
    s.mu.Lock()
    defer s.mu.Unlock()

    irKey := node.GetCluster()  // Maps to snapshot cache irKey

    for _, clusterStats := range req.GetClusterStats() {
        for _, localityStats := range clusterStats.GetUpstreamLocalityStats() {
            locality := localityStats.GetLocality()
            key := LocalityKey{
                Region:  locality.GetRegion(),
                Zone:    locality.GetZone(),
                SubZone: locality.GetSubZone(),
            }

            issued := localityStats.GetTotalIssuedRequests()

            // EWMA update
            rates, ok := s.rates[irKey]
            if !ok {
                rates = make(map[LocalityKey]*ewmaRate)
                s.rates[irKey] = rates
            }

            if existing, ok := rates[key]; ok {
                existing.value = s.ewmaAlpha*float64(issued) +
                    (1-s.ewmaAlpha)*existing.value
                existing.lastUpdated = time.Now()
            } else {
                rates[key] = &ewmaRate{
                    value:       float64(issued),
                    lastUpdated: time.Now(),
                }
            }
        }
    }

    // Recompute fractions after each update
    s.recomputeFractions(irKey)
}
```

#### 2.3 Fraction Computation

```go
func (s *TrafficFractionStore) recomputeFractions(irKey string) {
    // Must be called with s.mu held

    rates, ok := s.rates[irKey]
    if !ok {
        return
    }

    var total float64
    for _, rate := range rates {
        total += rate.value
    }

    if total == 0 {
        delete(s.fractions, irKey)
        return
    }

    fractions := make(map[LocalityKey]uint32, len(rates))
    for key, rate := range rates {
        // Basis points: 0-10000
        fractions[key] = uint32(10000.0 * rate.value / total)
    }

    if s.fractions == nil {
        s.fractions = make(map[string]map[LocalityKey]uint32)
    }
    s.fractions[irKey] = fractions
    s.lastComputed[irKey] = time.Now()
}
```

#### 2.4 Fraction Retrieval (for EDS injection)

```go
// GetFractions returns the current per-locality traffic fractions for a given irKey.
// Returns nil if no data is available.
func (s *TrafficFractionStore) GetFractions(irKey string) map[LocalityKey]uint32 {
    s.mu.RLock()
    defer s.mu.RUnlock()

    fractions, ok := s.fractions[irKey]
    if !ok {
        return nil
    }

    // Return a copy to avoid races
    result := make(map[LocalityKey]uint32, len(fractions))
    for k, v := range fractions {
        result[k] = v
    }
    return result
}

// HasFreshData returns true if fractions exist and were computed within the given threshold.
func (s *TrafficFractionStore) HasFreshData(irKey string, threshold time.Duration) bool {
    s.mu.RLock()
    defer s.mu.RUnlock()

    t, ok := s.lastComputed[irKey]
    if !ok {
        return false
    }
    return time.Since(t) < threshold
}
```

#### 2.5 EWMA Parameters

| Parameter | Default | Rationale |
|-----------|---------|-----------|
| `ewmaAlpha` | 0.3 | Balances responsiveness (higher) vs stability (lower). At 0.3, ~87% of a traffic shift is captured within 5 reporting intervals. |
| `reportingInterval` | 10s | Matches Envoy's default LRS interval. Provides ~10s update granularity. |
| `stalenessThreshold` | 60s | If no LRS report updates fractions for 60s, fractions are considered stale. Should be 5-6x reporting interval to tolerate transient gaps. |

### 3. EDS Fraction Injection

The fractions computed by the store must be injected into the local cluster's EDS response.
This is the critical integration point with envoy-gateway's existing xDS translation pipeline.

#### 3.1 Where the Local Cluster EDS Is Built

envoy-gateway already sets `cluster_manager.local_cluster_name` in the Envoy bootstrap
template (`internal/xds/bootstrap/bootstrap.yaml.tpl:13`):

```yaml
cluster_manager:
  local_cluster_name: {{ .ServiceClusterName }}
```

This means Envoy already knows its local cluster. When zone-aware routing is enabled,
Envoy uses the local cluster's EDS to populate `local_priority_set_`, from which it
reads `observed_traffic_fraction` values.

The local cluster is a **static cluster** defined in the bootstrap config — it points
to the Envoy fleet itself (same endpoints that appear in the Envoy proxy Service).
However, for the purposes of LRS-driven zone-aware routing, the fractions need to be
injected into the **dynamic** cluster's EDS (the xDS-managed cluster that represents
the Envoy fleet, separate from the static bootstrap cluster).

**Key insight:** The `observed_traffic_fraction` field goes on the `LocalityLbEndpoints`
of the local cluster's `ClusterLoadAssignment`. The local cluster's EDS is managed
separately from upstream service EDS. envoy-gateway currently creates the local cluster
in the bootstrap config as a static resource.

#### 3.2 Injection Approach: Translator-Level

The injection happens in `internal/xds/translator/cluster.go` within the
`buildZonalLocalities` function, which already creates `LocalityLbEndpoints` with
zone-based locality info. When zone-aware routing is enabled and LRS fractions are
available, the translator sets `observed_traffic_fraction` on each locality.

**Current flow:**
```
XdsIR → Translator.Translate() → buildXdsClusterLoadAssignment()
  → buildZonalLocalities() → LocalityLbEndpoints (no fractions)
  → Snapshot Cache → EDS to Envoy
```

**New flow:**
```
XdsIR → Translator.Translate() → buildXdsClusterLoadAssignment()
  → buildZonalLocalities() → LocalityLbEndpoints
  → injectTrafficFractions() → LocalityLbEndpoints (with fractions)
  → Snapshot Cache → EDS to Envoy
```

However, the translator currently operates purely on IR data and produces xDS resources
without external state. The `TrafficFractionStore` is external runtime state. We have
two options:

**Option A: Inject in translator (pass store to Translator)**

Add the `TrafficFractionStore` as an optional field on the `Translator` struct, similar
to how `GlobalRateLimit` settings are passed. The translator reads fractions during
`buildXdsClusterLoadAssignment` when building the local cluster.

```go
type Translator struct {
    // ...existing fields...
    TrafficFractionStore *lrs.TrafficFractionStore  // Optional, nil if LRS disabled
}
```

**Option B: Post-process in runner (inject after translation)**

After `t.Translate()` returns `result.XdsResources`, the runner post-processes the
EDS resources to inject fractions before passing to the snapshot cache.

```go
// In translateFromSubscription, after t.Translate():
if r.lrsStore != nil && result.XdsResources != nil {
    injectTrafficFractions(result.XdsResources, r.lrsStore, key)
}
```

**Recommendation: Option B (post-process in runner).**

Rationale:
1. Keeps the translator stateless and purely IR-driven
2. Separation of concerns: the translator handles IR→xDS, the runner handles runtime state injection
3. Easier to test: translator tests don't need LRS store mocking
4. Consistent with how the runner already handles snapshot caching and status publishing
5. The fractions are orthogonal to translation — they're runtime data overlaid on translated output

#### 3.3 Post-Processing Implementation

**File:** `internal/xds/runner/runner.go` (addition to `translateFromSubscription`)

```go
func injectTrafficFractions(
    resources types.XdsResources,
    store *lrs.TrafficFractionStore,
    irKey string,
) {
    fractions := store.GetFractions(irKey)
    if fractions == nil {
        return  // No LRS data yet; Envoy uses host-count fallback
    }

    // Find EDS resources (ClusterLoadAssignment) and inject fractions
    endpoints, ok := resources[resourcev3.EndpointType]
    if !ok {
        return
    }

    for _, resource := range endpoints {
        cla, ok := resource.(*endpointv3.ClusterLoadAssignment)
        if !ok {
            continue
        }

        for _, locality := range cla.GetEndpoints() {
            loc := locality.GetLocality()
            if loc == nil {
                continue
            }
            key := lrs.LocalityKey{
                Region:  loc.GetRegion(),
                Zone:    loc.GetZone(),
                SubZone: loc.GetSubZone(),
            }
            if fraction, ok := fractions[key]; ok {
                locality.ObservedTrafficFraction = wrapperspb.UInt32(fraction)
            }
        }
    }
}
```

**Important:** This injects fractions on **all** EDS resources for the irKey. The Envoy
data-plane only reads `observed_traffic_fraction` from the local cluster's EDS when
`locality_basis = LRS_REPORTED_RATE` is configured, so setting the field on other clusters
is harmless (ignored by Envoy). If more targeted injection is needed, the function can
filter by cluster name matching the local cluster pattern.

#### 3.4 Integration Point in Runner

In `translateFromSubscription`, after successful translation:

```go
// After: result, err := t.Translate(val.XdsIR)
// Before: r.cache.GenerateNewSnapshot(key, result.XdsResources, traceCtx)

// Inject LRS-based traffic fractions into EDS if store is available
if r.lrsStore != nil {
    injectTrafficFractions(result.XdsResources, r.lrsStore, key)
}
```

The `Runner.Config` struct gains a new field:

```go
type Config struct {
    // ...existing fields...
    LRSStore *lrs.TrafficFractionStore  // Optional, nil if LRS is disabled
}
```

#### 3.5 Triggering EDS Updates from LRS Data

The current design injects fractions during the normal IR→xDS translation cycle. This
means fractions update whenever the IR changes (gateway config change, endpoint change).
But fractions should also update independently when new LRS data arrives.

**Approach: Periodic snapshot refresh.**

Add a goroutine in the xDS runner that periodically checks for fraction changes and
triggers snapshot regeneration:

```go
func (r *Runner) periodicFractionRefresh(ctx context.Context, interval time.Duration) {
    ticker := time.NewTicker(interval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            if r.lrsStore == nil {
                continue
            }
            // For each irKey with active snapshots, regenerate if fractions changed
            for _, irKey := range r.cache.GetIrKeys() {
                if r.lrsStore.FractionsChanged(irKey) {
                    // Re-translate and update snapshot
                    // (or post-process existing snapshot with new fractions)
                    r.refreshSnapshotWithFractions(irKey)
                }
            }
        }
    }
}
```

**Refresh interval:** 15-30s. This should be 1-3x the LRS reporting interval. Shorter
intervals increase EDS churn without improving accuracy. Longer intervals delay fraction
propagation.

**Alternative considered: Event-driven refresh.** The LRS store could emit an event when
fractions change, triggering an immediate snapshot update. This is more responsive but
adds complexity (event channels, deduplication). The periodic approach is simpler and
sufficient — a 15s delay is acceptable for traffic redistribution.

### 4. Local Cluster Configuration

#### 4.1 How the Local Cluster Works in envoy-gateway

The Envoy bootstrap template (`internal/xds/bootstrap/bootstrap.yaml.tpl`) configures:

```yaml
cluster_manager:
  local_cluster_name: {{ .ServiceClusterName }}
```

And sets the node locality:

```yaml
node:
  locality:
    zone: $(ENVOY_SERVICE_ZONE)
```

When `local_cluster_name` is set, Envoy creates a `local_priority_set_` that mirrors
the local cluster's endpoints. Zone-aware routing uses this to determine the local
zone's share of traffic.

The `ServiceClusterName` is currently set to the gateway's service cluster name
(e.g., `envoy-gateway-system/eg`). This cluster must exist in the xDS config and
receive EDS updates for zone-aware routing to work.

#### 4.2 Ensuring the Local Cluster Gets Fractions

The local cluster's `ClusterLoadAssignment` must include the Envoy fleet's endpoints
organized by zone (already done by `buildZonalLocalities` when `PreferLocal` is set)
**plus** `observed_traffic_fraction` on each locality.

The `injectTrafficFractions` function in section 3.3 handles this. As long as:
1. The local cluster is an xDS-managed cluster (not just a static bootstrap cluster)
2. The local cluster's endpoints go through `buildZonalLocalities` (have zone info)
3. LRS reports include stats matching the locality zones

Then fractions will be injected automatically.

#### 4.3 Bootstrap Changes for LRS

Envoy needs to be configured to send LRS reports. This requires a `load_stats_config`
section on the static bootstrap cluster that points to the LRS server:

**New bootstrap template addition** (`internal/xds/bootstrap/bootstrap.yaml.tpl`):

```yaml
{{- if .LRSEnabled }}
  load_stats_config:
    api_config_source:
      api_type: GRPC
      transport_api_version: V3
      grpc_services:
      - envoy_grpc:
          cluster_name: xds_cluster
      set_node_on_first_message_only: true
{{- end }}
```

This reuses the existing `xds_cluster` (which already points to envoy-gateway's gRPC
server) for LRS. No additional cluster or connection is needed.

### 5. Update Frequency and Performance

#### 5.1 Timeline of a Fraction Update

```
T=0s:   Traffic pattern changes (BGP shift, etc.)
T=0-10s: Envoy instances observe new traffic pattern locally
T=10s:  First LRS reports with new data arrive at envoy-gateway
T=10-20s: EWMA begins incorporating new data (alpha=0.3)
T=30s:  EWMA is ~65% converged to new pattern
T=30-45s: Next periodic snapshot refresh picks up new fractions
T=45-60s: EDS update delivered to Envoy instances
T=45-60s: Envoy calls regenerateLocalityRoutingStructures()
T=60-90s: Full convergence (90%+ of shift reflected in routing)
```

**Total convergence time: 60-90 seconds.** This is appropriate for zone-aware routing,
where the goal is to correct persistent skew, not react to transient spikes.

#### 5.2 Resource Costs

| Resource | Cost | Notes |
|----------|------|-------|
| gRPC streams | 1 per Envoy instance | Lightweight, reuses existing connection to xDS server |
| Memory | ~100 bytes per locality per irKey | Tiny; typical deployments have <10 localities |
| CPU | Negligible | EWMA is O(1) per locality per report |
| EDS bandwidth | +4 bytes per locality per EDS push | One `UInt32Value` wrapper per locality |
| Snapshot rebuilds | +1 every 15-30s per irKey | Marginal cost on top of existing rebuild frequency |

### 6. Configuration

#### 6.1 EnvoyGateway Config

LRS and fraction injection should be configurable via the `EnvoyGateway` resource:

```go
// In api/v1alpha1/envoygateway_types.go (future)
type XDSServerSettings struct {
    // ...existing fields...

    // LoadReporting configures LRS (Load Reporting Service) for collecting
    // traffic statistics from Envoy instances.
    LoadReporting *LoadReportingSettings `json:"loadReporting,omitempty"`
}

type LoadReportingSettings struct {
    // Enabled controls whether the LRS server is started.
    // Default: false
    Enabled bool `json:"enabled"`

    // ReportingInterval is how often Envoy instances report load stats.
    // Default: 10s
    ReportingInterval *metav1.Duration `json:"reportingInterval,omitempty"`

    // EWMAAlpha is the EWMA smoothing factor (0-1).
    // Higher values make the system more responsive to changes.
    // Default: 0.3
    EWMAAlpha *float64 `json:"ewmaAlpha,omitempty"`

    // SnapshotRefreshInterval is how often EDS snapshots are refreshed
    // with updated traffic fractions.
    // Default: 15s
    SnapshotRefreshInterval *metav1.Duration `json:"snapshotRefreshInterval,omitempty"`
}
```

#### 6.2 Feature Gate

Since this is a significant new capability, it should be behind a feature gate during
initial development:

```go
// In api/v1alpha1/shared_types.go or similar
const (
    FeatureGateLRSAggregation = "LRSAggregation"
)
```

When the gate is disabled (default), the LRS server is not started and no fraction
injection occurs. This ensures zero impact on existing deployments.

### 7. Testing Strategy

#### 7.1 Unit Tests

| Test | File | Validates |
|------|------|-----------|
| `TestTrafficFractionStore_BasicAggregation` | `lrs/store_test.go` | EWMA computation, fraction normalization |
| `TestTrafficFractionStore_MultipleEnvoys` | `lrs/store_test.go` | Aggregation across multiple reporting instances |
| `TestTrafficFractionStore_Staleness` | `lrs/store_test.go` | Fractions expire after threshold |
| `TestTrafficFractionStore_EmptyData` | `lrs/store_test.go` | Returns nil when no data |
| `TestInjectTrafficFractions` | `runner/runner_test.go` | Fractions injected into correct EDS localities |
| `TestInjectTrafficFractions_NoStore` | `runner/runner_test.go` | No-op when store is nil |
| `TestInjectTrafficFractions_NoFractions` | `runner/runner_test.go` | No-op when no fractions computed |
| `TestLRSServer_StreamLifecycle` | `lrs/server_test.go` | Stream open, report, close |

#### 7.2 Integration Tests

| Test | Validates |
|------|-----------|
| `TestLRSEndToEnd` | LRS reports → aggregation → EDS fraction injection → Envoy receives fractions |
| `TestLRSWithZoneAwareRouting` | Full pipeline with zone-aware-enabled BackendTrafficPolicy |

#### 7.3 E2E Tests

A new e2e test suite that:
1. Deploys envoy-gateway with LRS enabled
2. Deploys a multi-zone upstream service
3. Generates traffic
4. Verifies Envoy instances report LRS data
5. Verifies EDS responses contain `observed_traffic_fraction`

### 8. File Changes Summary

#### New Files

| File | Purpose |
|------|---------|
| `internal/xds/server/lrs/server.go` | LRS gRPC service handler |
| `internal/xds/server/lrs/store.go` | TrafficFractionStore (EWMA aggregation) |
| `internal/xds/server/lrs/store_test.go` | Store unit tests |
| `internal/xds/server/lrs/server_test.go` | LRS server unit tests |

#### Modified Files

| File | Change |
|------|--------|
| `internal/xds/runner/runner.go` | Register LRS service, add store to Config, add `injectTrafficFractions`, add periodic refresh goroutine |
| `internal/xds/bootstrap/bootstrap.yaml.tpl` | Add optional `load_stats_config` section |
| `internal/xds/bootstrap/bootstrap.go` | Add `LRSEnabled` field to template data |
| `api/v1alpha1/envoygateway_types.go` | Add `LoadReportingSettings` config (future) |

### 9. Phased Implementation Plan

#### Phase 1: Core LRS + Store (MVP)

- Implement `TrafficFractionStore` with EWMA aggregation
- Implement LRS gRPC handler
- Register on existing xDS gRPC server
- Add `injectTrafficFractions` post-processing in runner
- Add periodic snapshot refresh
- Unit tests for store and injection
- Feature-gated, disabled by default

**Scope:** ~500 lines of implementation, ~400 lines of tests.

#### Phase 2: Bootstrap + Config

- Add `load_stats_config` to bootstrap template
- Add `LoadReportingSettings` to EnvoyGateway config
- Propagate config to runner and store
- Integration tests

#### Phase 3: E2E + Observability

- E2E test with multi-zone deployment
- Prometheus metrics for LRS (streams connected, reports received, fractions computed)
- Logging for fraction changes and staleness
- Documentation

#### Phase 4: Advanced Features (Future)

- Per-cluster fraction filtering (only inject on local cluster)
- LRS bidirectional feedback (send fractions back via `LoadStatsResponse`)
- Configurable EWMA parameters per cluster
- Stale locality eviction from the store

### 10. Interaction with Existing Zone-Aware Routing

envoy-gateway already supports zone-aware routing via `BackendTrafficPolicy.PreferLocal`
and Kubernetes Topology Aware Routing / Traffic Distribution. The existing flow:

1. `gatewayapi` translator detects `PreferLocal` config on BackendRef or BackendTrafficPolicy
2. Sets `ir.PreferLocalZone` on `DestinationSetting`
3. xDS translator calls `buildZonalLocalities()` (groups endpoints by zone)
4. xDS translator calls `buildZoneAwareLbConfig()` (sets `ZoneAwareLbConfig` on cluster)

LRS fraction injection is **additive** to this flow. It does not change any existing
behavior. When LRS is enabled:
- `buildZonalLocalities()` continues to produce zone-grouped localities (unchanged)
- `injectTrafficFractions()` overlays fractions on those localities (new)
- Envoy receives both zone structure and fractions

When LRS is disabled or no fractions are available, the `observed_traffic_fraction`
field is simply absent from EDS, and Envoy falls back to host-count-based percentages
(existing behavior, unchanged).

### 11. Open Questions

1. **Local cluster as xDS-managed vs static:** The current bootstrap uses a static
   cluster for `local_cluster_name`. For LRS fractions to be useful, the local cluster
   needs to receive dynamic EDS updates with fractions. This may require making the
   local cluster an xDS-managed cluster, or adding a mechanism to update the static
   cluster's load assignment. **This is the most significant design decision that needs
   further investigation.**

2. **Per-cluster vs global fractions:** The current design computes fractions per irKey
   (which maps to a gateway class). Should fractions be per-upstream-cluster instead?
   For the local cluster use case, per-irKey is correct (all Envoys in a gateway class
   see the same traffic distribution). Per-cluster fractions would be needed if different
   upstream services see different traffic patterns, but that's a future enhancement.

3. **Node-to-irKey mapping in LRS:** The LRS stream identifies the Envoy by `node.Cluster`,
   which maps to the irKey. This assumes all Envoys in a gateway class report to the
   same LRS server. In multi-replica envoy-gateway deployments, each replica receives
   a subset of LRS streams. **Leader election or shared state** may be needed for
   accurate aggregation across all Envoy instances.

4. **Multi-replica aggregation:** With multiple envoy-gateway replicas, each replica
   sees LRS reports from a subset of Envoy instances. Options:
   - **Shared store** (e.g., via etcd or a CRD): All replicas write to shared state
   - **Leader-based**: Only the leader runs LRS; others get fractions from shared state
   - **Local aggregation**: Each replica computes from its own subset (sufficient if
     Envoys are evenly distributed across replicas via connection load balancing)
   - **Recommendation:** Start with local aggregation. The random `MaxConnectionAge`
     already spreads Envoy connections across replicas. With enough Envoys per zone,
     each replica sees a representative sample.

5. **Interaction with `ForceLocalZone`:** When `ForceLocalZone` is configured, all
   traffic stays local regardless of fractions. LRS fractions are irrelevant in this
   mode. The `injectTrafficFractions` function should still inject them (harmless), and
   the Envoy data-plane ignores them when force-local is active.
