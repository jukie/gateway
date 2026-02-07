# Design v3: LRS-Driven LocalityBasis Mode for Zone-Aware Routing

**Issue:** en-sbx (follow-up to en-oh4, en-mz1)
**Status:** Design Proposal v3
**Author:** polecat/furiosa
**Date:** 2026-02-06

## Summary

This document designs a new `LRS_REPORTED_RATE` mode for the `LocalityBasis` enum that
uses control-plane-provided traffic distribution data to drive zone-aware routing decisions.
Unlike v2's Approach A (which switches to `locality_weighted_lb_config`), this approach
**stays within zone-aware routing** and preserves its inherent local-preference and
spillover semantics while gaining the global accuracy of control-plane aggregated data.

### The Gap Between v2 Approaches A and B

| Aspect | v2 Approach A (xDS-Driven) | v2 Approach B (Local Observation) | **v3: LRS-Driven** |
|--------|---------------------------|----------------------------------|---------------------|
| Routing mode | `locality_weighted_lb_config` | `zone_aware_lb_config` | `zone_aware_lb_config` |
| Data source | Control plane (global view) | Local counters (per-instance) | Control plane (global view) |
| Local preference | Must be engineered via weights | Inherent (zone-aware routing) | Inherent (zone-aware routing) |
| Spillover semantics | EDF scheduler (different model) | Native spillover preserved | Native spillover preserved |
| Accuracy | Excellent | Good for BGP, moderate for heavy-client | Excellent |

v2 Approach A provides global accuracy but abandons zone-aware routing. v2 Approach B
stays in zone-aware routing but only has a local view. **v3 combines the best of both:
global accuracy within zone-aware routing.**

## Problem Statement

Envoy's zone-aware routing computes per-locality percentages in
`calculateLocalityPercentages()` (load_balancer_impl.cc:653) using host counts or host
weights. These percentages drive the LocalityDirect / LocalityResidual decision that
determines how much traffic stays local vs spills to other zones.

The proportional assumption ("30% of Envoy instances = 30% of inbound traffic") breaks
under BGP skew and heavy-client scenarios. v2 Approach B (local observation) addresses
this with per-instance rate tracking but has no global view. The control plane, via LRS
aggregation, has the global view but v2 Approach A requires switching to a different
routing mode entirely.

**The v3 idea:** The control plane aggregates LRS data, computes actual per-zone traffic
fractions, and pushes them back to Envoy. Envoy's `calculateLocalityPercentages()` uses
these fractions as `local_percentage` instead of computing from host counts. Zone-aware
routing continues as normal with local preference, spillover logic, and all existing
semantics intact.

## Detailed Design

### New LocalityBasis Enum Value

```protobuf
enum LocalityBasis {
  HEALTHY_HOSTS_NUM = 0;
  HEALTHY_HOSTS_WEIGHT = 1;
  LRS_REPORTED_RATE = 2;    // New: use control-plane-provided traffic fractions
}
```

When `locality_basis_ == LRS_REPORTED_RATE`, the load balancer uses traffic distribution
data provided by the control plane (via one of the delivery mechanisms evaluated below)
instead of computing percentages from host counts.

### How It Plugs Into Existing Zone-Aware Routing

The integration point is `calculateLocalityPercentages()`. Currently the function computes
two parallel sets of weights:

```
local_weights:    per-locality weights from local_priority_set_ (the Envoy fleet)
upstream_weights: per-locality weights from priority_set_ (the upstream cluster)
```

These produce `local_percentage` and `upstream_percentage` per locality, which feed into
`regenerateLocalityRoutingStructures()` to determine:

1. **LocalityDirect**: `upstream_percentage >= local_percentage` for local locality
   -> 100% of traffic stays local
2. **LocalityResidual**: `local_percent_to_route_ = upstream_pct * 10000 / local_pct`
   -> partial local, rest spills via `residual_capacity_[]`

In `LRS_REPORTED_RATE` mode, only the **local_percentage** computation changes. The
`upstream_percentage` continues to use host counts/weights (representing capacity). This
asymmetry is correct:

- **local_percentage**: "What fraction of total inbound traffic does this zone handle?"
  -> Replace with control-plane-observed fraction (accurate)
- **upstream_percentage**: "What fraction of total upstream capacity is in this zone?"
  -> Keep as host-count/weight-based (represents real capacity)

```cpp
// In calculateLocalityPercentages(), new case for local weights:
case LocalityLbConfig::ZoneAwareLbConfig::LRS_REPORTED_RATE: {
  for (const auto& locality_hosts : local_hosts_per_locality.get()) {
    uint64_t locality_weight = 0;
    if (!locality_hosts.empty()) {
      const auto& locality = locality_hosts[0]->locality();
      auto it = lrs_reported_fractions_.find(locality);
      if (it != lrs_reported_fractions_.end() && !lrs_fractions_stale_) {
        // Use control-plane-provided fraction (stored as basis points, 0-10000)
        locality_weight = it->second;
      } else {
        // Fallback: use host count when no LRS data or data is stale
        locality_weight = locality_hosts.size();
      }
    }
    total_local_weight += locality_weight;
    if (!locality_hosts.empty()) {
      local_weights.emplace(locality_hosts[0]->locality(), locality_weight);
    }
  }
  break;
}

// Upstream weights ALWAYS use the existing logic (host counts or host weights):
// This is unchanged regardless of locality_basis_ setting.
// (See "Upstream Weight Independence" section below for rationale.)
```

**Upstream Weight Independence:** The upstream weight computation should remain
unchanged (host counts or weights) even in `LRS_REPORTED_RATE` mode. The upstream
percentages represent *capacity* — how many healthy hosts each upstream zone has available
to serve requests. The control plane's observed traffic fractions replace the *demand*
side (how much traffic is arriving at each zone), not the *supply* side. Keeping these
independent produces the correct spillover behavior: if zone A receives 60% of traffic
but only has 30% of upstream hosts, the zone-aware routing math correctly computes that
zone A should spill ~50% of its traffic to other zones.

### Periodic Regeneration via Timer

Currently, `regenerateLocalityRoutingStructures()` is only called reactively on topology
changes (host additions/removals via `PrioritySet` callbacks). For `LRS_REPORTED_RATE`
mode, we need periodic re-evaluation when the control plane pushes updated fractions.

The trigger depends on the delivery mechanism (see below), but in all cases the approach
is: when new fractions arrive, update the stored fractions map and call
`regenerateLocalityRoutingStructures()` to recompute routing state.

```cpp
class ZoneAwareLoadBalancerBase {
  // ...
  // Stored control-plane-provided traffic fractions per locality
  absl::flat_hash_map<envoy::config::core::v3::Locality, uint64_t,
                      LocalityHash, LocalityEqualTo> lrs_reported_fractions_;

  // Staleness tracking
  std::chrono::steady_clock::time_point lrs_fractions_last_updated_;
  bool lrs_fractions_stale_{true};
  std::chrono::milliseconds staleness_threshold_;

  // Timer for staleness checks
  Event::TimerPtr staleness_check_timer_;
};
```

---

## Delivery Mechanism Evaluation

The central question: how does the control plane push observed traffic fractions back
to Envoy? Five options are evaluated below.

### Option 1: New Field in ClusterLoadAssignment (EDS)

**Mechanism:** Add a new `observed_traffic_fraction` field to `LocalityLbEndpoints` in
the EDS response. The control plane sets this field based on aggregated LRS data.

```protobuf
// In api/envoy/config/endpoint/v3/endpoint_components.proto
message LocalityLbEndpoints {
  // Existing fields...
  core.v3.Locality locality = 1;
  repeated LbEndpoint lb_endpoints = 2;
  google.protobuf.UInt32Value load_balancing_weight = 3;
  uint32 priority = 5;
  google.protobuf.UInt32Value proximity = 6;

  // New: Control-plane-observed traffic fraction for this locality.
  // Represents the fraction of total fleet inbound traffic observed at this
  // locality, as reported by LRS and aggregated by the control plane.
  // Value is in basis points (0-10000, where 10000 = 100%).
  // Used by zone-aware routing when locality_basis = LRS_REPORTED_RATE.
  // If not set, Envoy falls back to host-count-based percentages.
  google.protobuf.UInt32Value observed_traffic_fraction = 10;
}
```

**Data flow:**
```
Envoy instances --> LRS reports --> Control plane aggregates
Control plane computes per-zone traffic fractions
Control plane sets observed_traffic_fraction in next EDS push
Envoy reads fractions during calculateLocalityPercentages()
```

**How Envoy receives the data:** The EDS update triggers the standard cluster update
path. The `ClusterLoadAssignment` is processed by `EdsClusterImpl`, which updates
`PrioritySet`. The `PrioritySet` update callback already triggers
`regenerateLocalityRoutingStructures()`. The load balancer reads the new
`observed_traffic_fraction` values from the host set's locality metadata during
`calculateLocalityPercentages()`.

**Note on EDS semantics:** The `observed_traffic_fraction` values describe the **local
cluster** (the Envoy fleet), not the upstream cluster. This is somewhat unusual for EDS,
which typically describes upstream endpoints. However, the zone-aware routing logic
already uses a separate `local_priority_set_` to represent the local Envoy fleet, and
that set is also populated via EDS (the "local cluster" config). The fractions would be
set on the local cluster's EDS response.

| Pro | Con |
|-----|-----|
| Uses existing EDS update path and callbacks | Couples traffic observation to endpoint discovery |
| Atomic: fractions update with topology | New field on `LocalityLbEndpoints` may confuse users of `locality_weighted_lb_config` |
| No new protocol or connection | Control plane must compute fractions at EDS push time |
| Natural staleness: stale if no EDS update | EDS push frequency may not match desired observation frequency |

### Option 2: New Field in Cluster (CDS)

**Mechanism:** Add a per-locality observed traffic map to the Cluster config.

```protobuf
// In api/envoy/config/cluster/v3/cluster.proto
message Cluster {
  // ...existing fields...

  // Observed per-locality traffic fractions from control plane LRS aggregation.
  // Keys are locality identifiers, values are basis points (0-10000).
  message ObservedLocalityTraffic {
    core.v3.Locality locality = 1;
    uint32 fraction_basis_points = 2;  // 0-10000
  }
  repeated ObservedLocalityTraffic observed_locality_traffic = 60;
}
```

| Pro | Con |
|-----|-----|
| Cluster is the natural config owner for LB behavior | CDS updates are heavier than EDS updates |
| Clearly a control-plane-driven setting | Per-locality data doesn't fit naturally in Cluster config |
| Separated from endpoint data | Would need frequent CDS pushes for timely updates |
| | CDS changes trigger full cluster rebuild (expensive) |

**Assessment: Not recommended.** CDS is designed for relatively static cluster
configuration. Pushing frequently-changing traffic fractions through CDS creates
unnecessary overhead (full cluster rebuilds) and is semantically out of place.

### Option 3: Overloading Existing Locality Metadata

**Mechanism:** Use `LocalityLbEndpoints.metadata` (field 9 in EDS) to carry observed
fractions as a well-known metadata key.

```
metadata:
  filter_metadata:
    envoy.lb:
      observed_traffic_fraction: 3500  # 35.0% in basis points
```

| Pro | Con |
|-----|-----|
| Zero proto changes | String-keyed, no type safety |
| Uses existing extensibility mechanism | Parsing cost on every metadata access |
| Control plane can start using immediately | Easy to misconfigure |
| | Not discoverable via proto documentation |
| | Metadata semantics are user-controlled, overloading them is fragile |

**Assessment: Viable but not recommended as primary mechanism.** Metadata-based delivery
is useful as a stopgap for control planes that cannot yet produce the new proto field,
but it should not be the canonical approach. Type safety and discoverability matter for
a feature that affects routing decisions.

### Option 4: Extend LoadStatsResponse (LRS Bidirectional)

**Mechanism:** The LRS protocol is already a bidirectional gRPC stream. Currently
`LoadStatsResponse` only tells Envoy what to report. Extend it to also carry observed
fractions back to Envoy.

```protobuf
// In api/envoy/service/load_stats/v3/lrs.proto
message LoadStatsResponse {
  // Existing fields...
  repeated string clusters = 1;
  google.protobuf.Duration load_reporting_interval = 2;
  bool report_endpoint_granularity = 3;
  bool send_all_clusters = 4;

  // New: Per-cluster observed traffic distribution from control plane.
  // The control plane aggregates LRS reports from all Envoy instances and
  // computes the actual per-locality traffic fractions, then pushes them
  // back to each Envoy instance.
  repeated ClusterObservedTraffic observed_traffic = 5;
}

message ClusterObservedTraffic {
  // Cluster name this observation applies to.
  string cluster_name = 1;

  // Per-locality observed traffic fractions.
  repeated LocalityTrafficFraction locality_fractions = 2;

  // When this observation was computed by the control plane.
  google.protobuf.Timestamp observation_time = 3;
}

message LocalityTrafficFraction {
  // The locality these fractions apply to.
  config.core.v3.Locality locality = 1;

  // Fraction of total fleet inbound traffic observed at this locality.
  // Basis points (0-10000, where 10000 = 100%).
  uint32 fraction_basis_points = 2;
}
```

**Data flow:**
```
Envoy --> LRS LoadStatsRequest (per-locality stats, already happening)
Control plane aggregates LRS from all instances
Control plane computes per-zone fractions
Control plane --> LRS LoadStatsResponse (includes observed_traffic)
Envoy load_stats_reporter receives response
Reporter updates LoadBalancer's lrs_reported_fractions_ map
LoadBalancer calls regenerateLocalityRoutingStructures()
```

**Wiring in Envoy:** The `LoadStatsReporter` (source/common/upstream/load_stats_reporter.cc)
already processes `LoadStatsResponse` in its `onReceiveMessage()` handler. The new field
would be read there and propagated to the relevant clusters' load balancers. This requires
a new interface from `LoadStatsReporter` to `ClusterManager` to update per-cluster
observed fractions.

| Pro | Con |
|-----|-----|
| Semantically perfect: LRS already carries traffic data in one direction | Extends LRS protocol (xDS API change) |
| Natural update frequency: tied to LRS reporting interval | Requires new wiring from LoadStatsReporter to ClusterManager to LoadBalancer |
| Bidirectional on existing stream (no new connections) | LRS is optional; clusters without LRS get no data |
| Control plane already has the data when it receives LRS | More complex implementation path than EDS approach |
| Clean separation: EDS for topology, LRS for traffic observations | |

### Option 5: New xDS Resource Type

**Mechanism:** Define a new xDS resource type (e.g., `LocalityTrafficDistribution`)
following the existing SOTW/Delta xDS patterns.

| Pro | Con |
|-----|-----|
| Clean separation of concerns | Entirely new resource type: proto, transport, subscription, etc. |
| Independent update frequency | Significant implementation effort in both Envoy and control planes |
| | Overkill for a single map of locality -> fraction |

**Assessment: Not recommended.** The implementation cost far exceeds the benefit. This
data naturally fits within existing EDS or LRS protocols.

### Recommendation: Option 1 (EDS) with Option 4 (LRS) as Future Enhancement

**Primary delivery: EDS (Option 1).**

Rationale:
1. **Lowest implementation cost.** The EDS update path already triggers
   `regenerateLocalityRoutingStructures()` via `PrioritySet` callbacks. Adding a new
   field to `LocalityLbEndpoints` and reading it during `calculateLocalityPercentages()`
   is a minimal change.
2. **Atomic with topology.** When the control plane pushes an EDS update that adds/removes
   hosts, the traffic fractions update atomically. There's no window where fractions and
   topology are out of sync.
3. **Works with existing infrastructure.** Every control plane that supports EDS can add
   this field. No new protocol support needed.
4. **Natural fallback.** If the field is unset, the load balancer falls back to host-count
   percentages. Partial rollout is safe.

**Future enhancement: LRS bidirectional (Option 4).**

The LRS approach is semantically cleaner and allows update frequency independent of EDS
pushes. However, it requires more extensive wiring (LoadStatsReporter -> ClusterManager ->
LoadBalancer) that adds implementation complexity. It should be pursued as a follow-up
once the core `LRS_REPORTED_RATE` mode is proven via EDS delivery.

**Metadata fallback (Option 3)** can be supported as a transitional mechanism for control
planes that cannot yet produce the new proto field, with a config flag to read fractions
from metadata instead.

---

## Proto Changes

### 1. LocalityBasis Enum Extension

**File:** `api/envoy/extensions/load_balancing_policies/common/v3/common.proto`

```protobuf
enum LocalityBasis {
  HEALTHY_HOSTS_NUM = 0;
  HEALTHY_HOSTS_WEIGHT = 1;

  // Use traffic distribution fractions provided by the control plane via EDS.
  // The control plane aggregates LRS data from all Envoy instances to compute
  // per-locality traffic fractions and pushes them back via the
  // observed_traffic_fraction field in LocalityLbEndpoints.
  //
  // When this mode is active:
  // - local_percentage in calculateLocalityPercentages() uses control-plane
  //   fractions instead of host counts
  // - upstream_percentage continues to use host counts/weights (capacity-based)
  // - Falls back to HEALTHY_HOSTS_NUM if fractions are not available or stale
  //
  // This preserves zone-aware routing semantics (local preference, spillover)
  // while using globally accurate traffic distribution data.
  LRS_REPORTED_RATE = 2;
}
```

### 2. LRS Rate Configuration

**File:** `api/envoy/extensions/load_balancing_policies/common/v3/common.proto`

```protobuf
message ZoneAwareLbConfig {
  // ... existing fields (1-6) ...

  // Configuration for LRS_REPORTED_RATE locality basis mode.
  // Only used when locality_basis = LRS_REPORTED_RATE.
  message LrsRateConfig {
    // Maximum age of control-plane-provided traffic fractions before they
    // are considered stale and the load balancer falls back to host counts.
    // Staleness is measured from the time the fractions were last received
    // via EDS update.
    //
    // Default: 60s. Should be set to 2-3x the expected EDS push interval
    // to tolerate occasional delayed updates without triggering fallback.
    google.protobuf.Duration staleness_threshold = 1
        [(validate.rules).duration = {gte {seconds: 5}, lte {seconds: 600}}];

    // Source of traffic fraction data.
    enum FractionSource {
      // Read from observed_traffic_fraction field in LocalityLbEndpoints (EDS).
      // This is the recommended source.
      EDS_FIELD = 0;

      // Read from locality metadata under the key
      // "envoy.lb.observed_traffic_fraction". This is a transitional mechanism
      // for control planes that cannot yet produce the new EDS proto field.
      LOCALITY_METADATA = 1;
    }

    // Where to read traffic fraction data from.
    // Default: EDS_FIELD.
    FractionSource fraction_source = 2;
  }

  LrsRateConfig lrs_rate_config = 7;
}
```

### 3. EDS Observed Traffic Fraction Field

**File:** `api/envoy/config/endpoint/v3/endpoint_components.proto`

```protobuf
message LocalityLbEndpoints {
  // ... existing fields (1-9) ...

  // Control-plane-observed traffic fraction for this locality.
  //
  // Represents the fraction of total fleet inbound traffic observed at this
  // locality, as computed by the control plane from aggregated LRS data.
  //
  // Value is in basis points: 0-10000, where 10000 = 100%.
  // For example, if the control plane observes that zone-a handles 35% of
  // total inbound traffic, this field would be set to 3500 for zone-a's
  // locality in the local cluster's EDS response.
  //
  // Used by zone-aware routing when locality_basis = LRS_REPORTED_RATE
  // in ZoneAwareLbConfig. Ignored otherwise.
  //
  // If not set (0), the load balancer falls back to host-count-based
  // percentage computation for this locality.
  //
  // This field is typically set on the **local cluster** (the cluster
  // representing the Envoy fleet itself), not the upstream service cluster.
  // Zone-aware routing uses local cluster fractions to determine how much
  // traffic stays local vs spills to other zones.
  google.protobuf.UInt32Value observed_traffic_fraction = 10;
}
```

---

## Integration With Existing Zone-Aware Routing Code

### calculateLocalityPercentages() Changes

The core change is a new case in the `switch (locality_basis_)` block within
`calculateLocalityPercentages()`:

```cpp
absl::FixedArray<ZoneAwareLoadBalancerBase::LocalityPercentages>
ZoneAwareLoadBalancerBase::calculateLocalityPercentages(
    const HostsPerLocality& local_hosts_per_locality,
    const HostsPerLocality& upstream_hosts_per_locality) {

  // --- LOCAL WEIGHT COMPUTATION ---
  absl::flat_hash_map<Locality, uint64_t, LocalityHash, LocalityEqualTo> local_weights;
  uint64_t total_local_weight = 0;

  for (const auto& locality_hosts : local_hosts_per_locality.get()) {
    uint64_t locality_weight = 0;
    switch (locality_basis_) {
    case LocalityLbConfig::ZoneAwareLbConfig::HEALTHY_HOSTS_WEIGHT:
      for (const auto& host : locality_hosts) {
        locality_weight += host->weight();
      }
      break;
    case LocalityLbConfig::ZoneAwareLbConfig::HEALTHY_HOSTS_NUM:
      locality_weight = locality_hosts.size();
      break;

    case LocalityLbConfig::ZoneAwareLbConfig::LRS_REPORTED_RATE: {
      // Use control-plane-provided traffic fractions for local percentages.
      // Read from LocalityLbEndpoints.observed_traffic_fraction in the local cluster.
      if (!locality_hosts.empty()) {
        auto fraction = getObservedTrafficFraction(locality_hosts[0]);
        if (fraction.has_value() && !isLrsFractionsStale()) {
          locality_weight = fraction.value();  // Already in basis points (0-10000)
        } else {
          // Fallback to host count if no fraction data or data is stale
          locality_weight = locality_hosts.size();
        }
      }
      break;
    }

    default:
      PANIC_DUE_TO_CORRUPT_ENUM;
    }
    total_local_weight += locality_weight;
    if (!locality_hosts.empty()) {
      local_weights.emplace(locality_hosts[0]->locality(), locality_weight);
    }
  }

  // --- UPSTREAM WEIGHT COMPUTATION (UNCHANGED) ---
  // Upstream weights always use the existing host-count/weight logic,
  // regardless of locality_basis_. This is correct: upstream weights
  // represent capacity, not traffic demand.
  //
  // NOTE: This means the upstream weight loop still uses the same
  // locality_basis_ switch for HEALTHY_HOSTS_NUM vs HEALTHY_HOSTS_WEIGHT,
  // but LRS_REPORTED_RATE falls through to HEALTHY_HOSTS_NUM for upstream.
  absl::flat_hash_map<Locality, uint64_t, LocalityHash, LocalityEqualTo> upstream_weights;
  uint64_t total_upstream_weight = 0;

  for (const auto& locality_hosts : upstream_hosts_per_locality.get()) {
    uint64_t locality_weight = 0;
    switch (locality_basis_) {
    case LocalityLbConfig::ZoneAwareLbConfig::HEALTHY_HOSTS_WEIGHT:
      for (const auto& host : locality_hosts) {
        locality_weight += host->weight();
      }
      break;
    // For both HEALTHY_HOSTS_NUM and LRS_REPORTED_RATE, upstream uses host counts
    case LocalityLbConfig::ZoneAwareLbConfig::HEALTHY_HOSTS_NUM:
    case LocalityLbConfig::ZoneAwareLbConfig::LRS_REPORTED_RATE:
      locality_weight = locality_hosts.size();
      break;
    default:
      PANIC_DUE_TO_CORRUPT_ENUM;
    }
    total_upstream_weight += locality_weight;
    if (!locality_hosts.empty()) {
      upstream_weights.emplace(locality_hosts[0]->locality(), locality_weight);
    }
  }

  // --- PERCENTAGE COMPUTATION (UNCHANGED) ---
  // The rest of the function is identical: compute local_percentage and
  // upstream_percentage from their respective weight maps.
  // ...
}
```

### Helper Methods

```cpp
// Read observed_traffic_fraction from a host's locality in the local cluster EDS.
absl::optional<uint64_t> ZoneAwareLoadBalancerBase::getObservedTrafficFraction(
    const HostSharedPtr& host) const {
  // Access the LocalityLbEndpoints proto for this host's locality.
  // The observed_traffic_fraction is set on the local cluster's EDS response.
  //
  // Implementation options:
  // 1. Store fractions in a map when EDS update is processed
  // 2. Read from host metadata (if using LOCALITY_METADATA source)
  //
  // For EDS_FIELD source: fractions are extracted during PrioritySet update
  // and stored in lrs_reported_fractions_ map.
  // For LOCALITY_METADATA source: read from host->metadata().
  if (fraction_source_ == LrsRateConfig::EDS_FIELD) {
    auto it = lrs_reported_fractions_.find(host->locality());
    if (it != lrs_reported_fractions_.end()) {
      return it->second;
    }
  } else {
    // Read from metadata key "envoy.lb.observed_traffic_fraction"
    auto metadata_fraction = readFractionFromMetadata(host);
    if (metadata_fraction.has_value()) {
      return metadata_fraction;
    }
  }
  return absl::nullopt;
}

bool ZoneAwareLoadBalancerBase::isLrsFractionsStale() const {
  if (lrs_fractions_last_updated_ == std::chrono::steady_clock::time_point{}) {
    return true;  // Never received fractions
  }
  auto age = std::chrono::steady_clock::now() - lrs_fractions_last_updated_;
  return age > staleness_threshold_;
}
```

### Fraction Storage and Update Path

When an EDS update arrives for the local cluster, the `PrioritySet` update callback
fires. In `LRS_REPORTED_RATE` mode, the load balancer extracts
`observed_traffic_fraction` values from the local cluster's `LocalityLbEndpoints` and
stores them:

```cpp
// In PrioritySet update callback (extends existing callback):
if (locality_basis_ == LocalityLbConfig::ZoneAwareLbConfig::LRS_REPORTED_RATE &&
    local_priority_set_ && priority == 0) {
  updateLrsReportedFractions();
  regenerateLocalityRoutingStructures();
}

void ZoneAwareLoadBalancerBase::updateLrsReportedFractions() {
  lrs_reported_fractions_.clear();
  const auto& local_hosts_per_locality =
      localHostSet().healthyHostsPerLocality().get();
  for (const auto& locality_hosts : local_hosts_per_locality) {
    if (!locality_hosts.empty()) {
      auto fraction = extractFractionFromEds(locality_hosts[0]);
      if (fraction.has_value()) {
        lrs_reported_fractions_[locality_hosts[0]->locality()] = fraction.value();
      }
    }
  }
  if (!lrs_reported_fractions_.empty()) {
    lrs_fractions_last_updated_ = std::chrono::steady_clock::now();
    lrs_fractions_stale_ = false;
  }
}
```

---

## Staleness and Fallback Behavior

### When Fractions Become Stale

If the control plane stops pushing updated fractions (e.g., control plane crash, LRS
aggregation failure, network partition), the load balancer must degrade gracefully.

**Staleness detection:** A timer checks `lrs_fractions_last_updated_` against
`staleness_threshold_`. When fractions exceed the threshold age:

1. Set `lrs_fractions_stale_ = true`
2. Call `regenerateLocalityRoutingStructures()` to recompute with host-count fallback
3. Log a warning: `"LRS traffic fractions stale (age: Xs > threshold: Ys), falling back to host counts"`

**Fallback behavior:** When `lrs_fractions_stale_` is true, the `LRS_REPORTED_RATE`
case in `calculateLocalityPercentages()` falls through to `HEALTHY_HOSTS_NUM` behavior.
This provides the same behavior as if `LRS_REPORTED_RATE` were never configured.

**Recovery:** When the next EDS update with valid fractions arrives, staleness is cleared
and LRS-based percentages are immediately used.

### Staleness Timer

```cpp
// In constructor, when LRS_REPORTED_RATE is configured:
staleness_check_timer_ = dispatcher.createTimer([this]() {
  if (isLrsFractionsStale() && !lrs_fractions_stale_) {
    lrs_fractions_stale_ = true;
    ENVOY_LOG(warn, "LRS traffic fractions stale, falling back to host counts");
    regenerateLocalityRoutingStructures();
  }
  staleness_check_timer_->enableTimer(staleness_threshold_ / 2);
});
staleness_check_timer_->enableTimer(staleness_threshold_);
```

### Edge Cases

| Scenario | Behavior |
|----------|----------|
| First startup (no fractions yet) | Fallback to host counts until first EDS with fractions |
| Control plane sends partial fractions | Localities with fractions use them; others fall back to host count |
| All fractions are 0 | Treated as "no data"; falls back to host counts |
| Fractions don't sum to 10000 | Normalized during percentage computation (already handled by the existing `total_local_weight` normalization) |
| Fractions sum to 0 | `total_local_weight == 0` case already handled (produces 0 percentages) |

---

## Control Plane Implementation Sketch

The control plane side is outside Envoy proper, but the design must be implementable.
Here is a sketch for a control plane that aggregates LRS and produces the
`observed_traffic_fraction` values for EDS.

### LRS Aggregation

```python
# Pseudocode for control plane LRS aggregation

class TrafficFractionComputer:
    def __init__(self, ewma_alpha=0.3):
        self.alpha = ewma_alpha
        self.locality_rates = {}  # locality -> ewma_rate

    def on_lrs_report(self, report: LoadStatsRequest):
        """Called when any Envoy instance sends an LRS report."""
        for cluster_stats in report.cluster_stats:
            for upstream_locality_stats in cluster_stats.upstream_locality_stats:
                locality = upstream_locality_stats.locality
                issued = upstream_locality_stats.total_issued_requests

                # EWMA update
                if locality in self.locality_rates:
                    self.locality_rates[locality] = (
                        self.alpha * issued +
                        (1 - self.alpha) * self.locality_rates[locality]
                    )
                else:
                    self.locality_rates[locality] = issued

    def compute_fractions(self) -> dict:
        """Compute per-locality traffic fractions for EDS."""
        total = sum(self.locality_rates.values())
        if total == 0:
            return {}

        fractions = {}
        for locality, rate in self.locality_rates.items():
            # Basis points: 0-10000
            fractions[locality] = int(10000 * rate / total)

        return fractions

    def apply_to_eds(self, cluster_load_assignment, fractions):
        """Set observed_traffic_fraction on local cluster EDS response."""
        for locality_lb_endpoints in cluster_load_assignment.endpoints:
            locality = locality_lb_endpoints.locality
            if locality in fractions:
                locality_lb_endpoints.observed_traffic_fraction.value = fractions[locality]
```

### Important Control Plane Considerations

1. **Aggregation window:** The control plane should aggregate LRS reports over a
   configurable window (e.g., 30-60s) before computing fractions. This smooths out
   transient spikes.

2. **Per-Envoy-zone computation:** Fractions can be the same for all Envoy instances
   (global traffic distribution) or customized per-zone. The simpler approach (same
   fractions for all) is recommended initially. The zone-aware routing math handles
   the per-zone implications correctly.

3. **LRS reporting interval:** The `load_reporting_interval` in `LoadStatsResponse`
   controls how often Envoys report. Setting this to 10-30s provides a good balance
   of freshness vs overhead. The EDS push of fractions should happen at a similar
   cadence.

4. **Bootstrap:** On initial startup (no LRS data yet), the control plane should either
   not set `observed_traffic_fraction` (letting Envoy use host-count fallback) or set
   it to the host-count-proportional estimate.

---

## Worked Example

### Setup

```
3 zones: zone-a (3 Envoys), zone-b (5 Envoys), zone-c (2 Envoys) = 10 total
Upstream service: zone-a (3 hosts), zone-b (5 hosts), zone-c (2 hosts) = 10 total
BGP skew: zone-a receives 50% of external traffic (expected: 30%)
```

### Without LRS_REPORTED_RATE (current behavior)

`calculateLocalityPercentages()` using `HEALTHY_HOSTS_NUM`:

```
local_percentage:    zone-a=3000  zone-b=5000  zone-c=2000  (30/50/20%)
upstream_percentage: zone-a=3000  zone-b=5000  zone-c=2000  (30/50/20%)
```

For a zone-a Envoy: `upstream_percentage (3000) >= local_percentage (3000)` ->
**LocalityDirect**. All traffic stays in zone-a.

**Problem:** Zone-a Envoys route 100% locally, but zone-a has 50% of traffic and only
30% of upstream capacity. Zone-a upstream hosts are overloaded (50/30 = 1.67x).

### With LRS_REPORTED_RATE

Control plane aggregates LRS and observes: zone-a=50%, zone-b=35%, zone-c=15%.

EDS for local cluster sets:
```
zone-a: observed_traffic_fraction = 5000
zone-b: observed_traffic_fraction = 3500
zone-c: observed_traffic_fraction = 1500
```

`calculateLocalityPercentages()` using `LRS_REPORTED_RATE`:

```
local_percentage:    zone-a=5000  zone-b=3500  zone-c=1500  (50/35/15%)
upstream_percentage: zone-a=3000  zone-b=5000  zone-c=2000  (30/50/20%, from host counts)
```

For a zone-a Envoy: `upstream_percentage (3000) < local_percentage (5000)` ->
**LocalityResidual**.

```
local_percent_to_route = upstream_pct * 10000 / local_pct = 3000 * 10000 / 5000 = 6000
```

60% of zone-a traffic stays local; 40% spills to other zones. The spillover is distributed
to zones with excess capacity:

```
zone-b residual: upstream_pct (5000) - local_pct (3500) = 1500
zone-c residual: upstream_pct (2000) - local_pct (1500) = 500
Total residual: 2000
zone-b gets 1500/2000 = 75% of spillover
zone-c gets 500/2000 = 25% of spillover
```

**Result:** Zone-a upstream hosts now receive ~60% of zone-a's traffic locally + spillover
from other zones. The global distribution approaches the balanced 30/50/20% split
matching upstream capacity.

---

## Implementation Plan

### Phase 1: Core EDS-Based Delivery (Minimal Viable)

**Files to modify:**

| File | Change | Description |
|------|--------|-------------|
| `api/envoy/extensions/load_balancing_policies/common/v3/common.proto` | Modify | Add `LRS_REPORTED_RATE = 2` to `LocalityBasis`, add `LrsRateConfig` message, add field to `ZoneAwareLbConfig` |
| `api/envoy/config/endpoint/v3/endpoint_components.proto` | Modify | Add `observed_traffic_fraction` field (10) to `LocalityLbEndpoints` |
| `source/extensions/load_balancing_policies/common/load_balancer_impl.h` | Modify | Add `lrs_reported_fractions_` map, staleness tracking members, `updateLrsReportedFractions()`, `getObservedTrafficFraction()`, `isLrsFractionsStale()` |
| `source/extensions/load_balancing_policies/common/load_balancer_impl.cc` | Modify | Add `LRS_REPORTED_RATE` case in `calculateLocalityPercentages()` (both local and upstream loops), fraction extraction in EDS update callback, staleness timer setup in constructor |
| `source/extensions/load_balancing_policies/common/BUILD` | Modify | Update deps if needed |
| `test/extensions/load_balancing_policies/common/load_balancer_impl_test.cc` | Modify | Add test cases (see below) |

**Estimated scope:** ~200-300 lines of implementation, ~300-400 lines of tests.

### Phase 2: LRS Bidirectional Delivery (Future Enhancement)

**Additional files:**

| File | Change | Description |
|------|--------|-------------|
| `api/envoy/service/load_stats/v3/lrs.proto` | Modify | Add `ClusterObservedTraffic` and `LocalityTrafficFraction` messages, add `observed_traffic` field to `LoadStatsResponse` |
| `source/common/upstream/load_stats_reporter.h` | Modify | Add interface to propagate received fractions |
| `source/common/upstream/load_stats_reporter.cc` | Modify | Parse `observed_traffic` from `LoadStatsResponse`, propagate to clusters |
| `include/envoy/upstream/cluster_manager.h` | Modify | Add method to update per-cluster observed fractions |
| `source/common/upstream/cluster_manager_impl.cc` | Modify | Implement fraction propagation to load balancers |

This phase is more invasive (touches the ClusterManager interface) and should be done
after Phase 1 is proven.

### Test Cases

**Phase 1 tests for `load_balancer_impl_test.cc`:**

1. **`LrsReportedRate_FallsBackToHostCountWhenNoFractions`**
   - Configure `LRS_REPORTED_RATE` but don't set any `observed_traffic_fraction`
   - Verify percentages match `HEALTHY_HOSTS_NUM` behavior

2. **`LrsReportedRate_UsesFractionsForLocalPercentage`**
   - Set `observed_traffic_fraction` on local cluster localities
   - Verify `local_percentage` uses fractions, `upstream_percentage` uses host counts

3. **`LrsReportedRate_SkewedTrafficCausesResidualRouting`**
   - Set fractions showing skew (e.g., zone-a at 50% instead of 30%)
   - Verify routing state transitions from LocalityDirect to LocalityResidual
   - Verify `local_percent_to_route_` is correctly computed

4. **`LrsReportedRate_StaleFractionsFallBack`**
   - Set fractions, advance time past staleness threshold
   - Verify fallback to host-count behavior

5. **`LrsReportedRate_PartialFractionsUseMixedSources`**
   - Set fractions for some localities but not all
   - Verify localities with fractions use them, others fall back to host count

6. **`LrsReportedRate_FractionsUpdateOnEds`**
   - Push initial fractions via EDS, verify routing
   - Push updated fractions, verify routing changes

7. **`LrsReportedRate_UpstreamUsesHostCountsRegardless`**
   - Verify upstream_percentage is always host-count-based in LRS_REPORTED_RATE mode

8. **`LrsReportedRate_SpilloverDistribution`**
   - Set fractions causing residual routing
   - Verify residual_capacity_ array correctly distributes spillover

### Ordering

1. Proto changes (API review prerequisite)
2. Core `calculateLocalityPercentages()` changes + fraction storage
3. Staleness timer + fallback logic
4. Tests
5. Documentation

---

## Comparison with v2 Approaches

| Criterion | v2-A: xDS-Driven Weights | v2-B: Local Observation | **v3: LRS-Driven LocalityBasis** |
|-----------|-------------------------|------------------------|----------------------------------|
| Routing mode | `locality_weighted_lb_config` (EDF) | `zone_aware_lb_config` (spillover) | `zone_aware_lb_config` (spillover) |
| Data source | Control plane | Local counters | Control plane |
| Envoy code changes | None | Medium (rate tracker, timer) | Small-Medium (new enum case, fraction storage) |
| Control plane changes | Yes (weight computation + per-zone EDS) | None | Yes (LRS aggregation + fraction injection in EDS) |
| Local preference | Must engineer via weight bias | Inherent | Inherent |
| Spillover semantics | EDF scheduler (proportional) | Native (LocalityDirect/Residual) | Native (LocalityDirect/Residual) |
| Accuracy | Excellent (global) | Good-BGP, moderate-heavy-client | Excellent (global) |
| Feedback latency | 10-30s (LRS + EDS) | ~seconds (configurable) | 10-30s (LRS + EDS) |
| Oscillation risk | Low (single source of truth) | Medium (independent decisions) | Low (single source of truth) |
| Fallback on failure | Stale weights remain valid | Falls back to host counts | Falls back to host counts |
| Migration from v2-B | Config change: swap to `locality_weighted_lb_config` | N/A | Config change: swap `locality_basis` enum value |

### When to Use Which

- **v2-A (xDS-Driven Weights):** Best when you want the control plane to fully control
  traffic distribution and are willing to switch to `locality_weighted_lb_config`. Gives
  most control but changes routing model.

- **v2-B (Local Observation):** Best when no control plane changes are possible. Fast
  local feedback, good for BGP skew.

- **v3 (LRS-Driven LocalityBasis):** Best when you want global accuracy AND zone-aware
  routing's local-preference/spillover semantics. Requires control plane work but keeps
  the familiar zone-aware routing model.

---

## Open Questions

1. **EDS field number allocation:** `observed_traffic_fraction = 10` in
   `LocalityLbEndpoints` needs to be verified as available. The current highest field
   number is 9 (metadata).

2. **API review process:** New proto fields in the Envoy API require an API review. The
   `LocalityBasis` enum extension is lower risk (existing extension point). The new EDS
   field is moderate risk (new semantics on a core message).

3. **Upstream weight basis in LRS_REPORTED_RATE mode:** This design proposes upstream
   weights always use host counts. An alternative is to allow a separate
   `upstream_locality_basis` config to independently control upstream weight computation.
   This adds complexity and is deferred unless a clear use case emerges.

4. **Control plane fraction computation frequency:** The design doesn't prescribe how
   often the control plane should recompute fractions. This is control-plane-specific.
   The staleness threshold provides the Envoy-side safety net.

5. **Interaction with overprovisioning factor:** The existing `overprovisioning_factor`
   (default 140) affects `effectiveLocalityWeight()` in the `LocalityWrr` path but does
   not affect zone-aware routing. This design does not change that. If users want
   overprovisioning-like behavior with `LRS_REPORTED_RATE`, the control plane can bake
   it into the fractions.

---

## Appendix: Key Code References

| Component | File | Line |
|-----------|------|------|
| `calculateLocalityPercentages()` | `load_balancer_impl.cc` | 653 |
| `regenerateLocalityRoutingStructures()` | `load_balancer_impl.cc` | 472 |
| `tryChooseLocalLocalityHosts()` | `load_balancer_impl.cc` | 741 |
| `hostSourceToUse()` | `load_balancer_impl.cc` | 796 |
| `earlyExitNonLocalityRouting()` | `load_balancer_impl.cc` | 580 |
| `ZoneAwareLoadBalancerBase` constructor | `load_balancer_impl.cc` | 387 |
| `LocalityBasis` enum | `common.proto` | 30 |
| `ZoneAwareLbConfig` proto | `common.proto` | 28 |
| `LocalityLbEndpoints` proto | `endpoint_components.proto` | 164 |
| `LoadStatsResponse` proto | `lrs.proto` | 76 |
| `UpstreamLocalityStats` proto | `load_report.proto` | 29 |
| `LoadStatsReporter` | `load_stats_reporter.cc` | 88 |
| `PrimitiveCounter::value()` | `primitive_stats.h` | 24 |
| `PrimitiveCounter::latch()` | `primitive_stats.h` | 32 |
