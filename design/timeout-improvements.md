# Design: Timeout Configuration Improvements for Envoy Gateway

**Issue:** ga-yzpk
**Status:** Design Proposal
**Author:** polecat/obsidian
**Date:** 2026-02-06
**Based on:** Research from ga-n6xi (timeout configuration landscape analysis)

## Summary

Envoy Gateway's timeout configuration has several gaps and user-facing confusions that
lead to recurring GitHub issues. This design proposes targeted improvements across three
areas: (1) exposing missing timeout controls, (2) adding API validation warnings for
common misconfiguration, and (3) comprehensive documentation of the timeout hierarchy.

## Problem Statement

Users consistently struggle with Envoy Gateway's timeout behavior, as evidenced by
multiple open GitHub issues:

| Issue | Problem | Root Cause |
|-------|---------|------------|
| GH#6726 | `perTryTimeout` appears to overwrite route `requestTimeout` | Expected Envoy behavior, undocumented |
| GH#8193 | `requestTimeout` ignored when `faultInjection` delay exceeds it | Expected Envoy filter-chain behavior, undocumented |
| GH#2611 | No per-route `idleTimeout` in BTP | Missing API field |
| GH#2509 | BTP timeout fields undocumented | Documentation gap |
| GH#7806 | Route idle timeout auto-config conflicting with CTP `streamIdleTimeout` | Fixed via PR#8058 |

The common thread: users' mental model of how timeouts interact doesn't match Envoy's
actual behavior, and Envoy Gateway doesn't provide enough controls or documentation to
bridge that gap.

## Current State

### Timeout Field Mapping: Envoy Gateway API -> Envoy Configuration

#### BackendTrafficPolicy (BTP) -- Route/Upstream Level

| EG API Field | Envoy Equivalent | Config Location | EG Default | Envoy Default |
|---|---|---|---|---|
| `timeout.tcp.connectTimeout` | `Cluster.connect_timeout` | Cluster | 10s | 5s |
| `timeout.http.requestTimeout` | `RouteAction.timeout` | Route | not set | 15s |
| `timeout.http.connectionIdleTimeout` | `CommonHttpProtocolOptions.IdleTimeout` | Cluster | not set | 1 hour |
| `timeout.http.maxConnectionDuration` | `CommonHttpProtocolOptions.MaxConnectionDuration` | Cluster | not set | unlimited |
| `timeout.http.maxStreamDuration` | `RouteAction.MaxStreamDuration.max_stream_duration` | Route | not set | not set |
| `retry.perRetry.timeout` | `RetryPolicy.per_try_timeout` | Route | not set | not set |
| -- | `RouteAction.idle_timeout` | Route | **auto-derived** | not set |

#### ClientTrafficPolicy (CTP) -- Listener/Downstream Level

| EG API Field | Envoy Equivalent | Config Location | EG Default | Envoy Default |
|---|---|---|---|---|
| `timeout.tcp.idleTimeout` | `TcpProxy.idle_timeout` | TCP filter | not set | 1 hour |
| `timeout.http.requestReceivedTimeout` | `HCM.request_timeout` | HCM | not set | disabled |
| `timeout.http.idleTimeout` | `HCM.CommonHttpProtocolOptions.idle_timeout` | HCM | not set | 1 hour |
| `timeout.http.streamIdleTimeout` | `HCM.stream_idle_timeout` | HCM | not set | 5 min |

#### Gateway API HTTPRoute Timeouts

| Gateway API Field | Maps To | Precedence |
|---|---|---|
| `HTTPRoute.spec.rules[].timeouts.request` | `RouteAction.timeout` | Takes precedence over BTP `requestTimeout` |
| `HTTPRoute.spec.rules[].timeouts.backendRequest` | `RouteAction.timeout` (no retries) or per-try budget (with retries) | See `processRouteTimeout` in `internal/gatewayapi/route.go:504-525` |

#### Auto-Derived: Route-Level idle_timeout

The route-level `idle_timeout` is auto-configured by `idleTimeout()` in
`internal/xds/translator/route.go:411-438` with this logic:

1. If CTP `streamIdleTimeout` is explicitly set -> **skip** (let listener value apply)
2. If a request timeout exists -> set idle timeout to `max(1 hour, requestTimeout)`
3. If request timeout is 0 (disabled) -> disable idle timeout (set to 0)
4. If no request timeout -> don't set route idle timeout

This auto-derivation was improved in PR#8058 (resolving GH#7806) but is still not
user-controllable, which is the gap GH#2611 identifies.

### Envoy Timeouts NOT Exposed by Gateway

| Envoy Timeout | Description | Gap Impact |
|---|---|---|
| `RouteAction.idle_timeout` (direct control) | Per-route override of `stream_idle_timeout` | **HIGH** -- requested in GH#2611, only auto-derived today |
| `per_try_idle_timeout` | Per-retry-attempt inactivity timeout | Medium -- useful for streaming retry |
| `request_headers_timeout` | Time to receive request headers | Low -- niche |
| `drain_timeout` | Grace period between HTTP/2 GOAWAYs | Low -- operational |
| `transport_socket_connect_timeout` | Downstream TLS handshake timeout | Low -- covered by other mechanisms |

## Proposed Changes

### Change 1: Add `idleTimeout` to BTP HTTPTimeout (GH#2611)

**Priority: HIGH**

Add a new `idleTimeout` field to the `HTTPTimeout` struct in BackendTrafficPolicy.

#### API Change

```go
// api/v1alpha1/timeout_types.go

type HTTPTimeout struct {
    ConnectionIdleTimeout *gwapiv1.Duration `json:"connectionIdleTimeout,omitempty"`
    MaxConnectionDuration *gwapiv1.Duration `json:"maxConnectionDuration,omitempty"`
    RequestTimeout        *gwapiv1.Duration `json:"requestTimeout,omitempty"`
    MaxStreamDuration     *gwapiv1.Duration `json:"maxStreamDuration,omitempty"`

    // NEW: IdleTimeout defines the period in which there must be active upstream/downstream
    // activity on a route. If not set, the route-level idle timeout is auto-derived from
    // the request timeout (for backwards compatibility). When set, it maps directly to
    // Envoy's RouteAction.idle_timeout, overriding the listener-level stream_idle_timeout
    // for this route. Set to "0s" to disable.
    //
    // +optional
    IdleTimeout *gwapiv1.Duration `json:"idleTimeout,omitempty"`
}
```

#### Translation Logic Change

In `internal/xds/translator/route.go`, modify the `idleTimeout()` function:

```go
func idleTimeout(httpRoute *ir.HTTPRoute, httpListener *ir.HTTPListener) *durationpb.Duration {
    // If user explicitly set route-level idle timeout in BTP, use it directly.
    if httpRoute.Traffic != nil &&
        httpRoute.Traffic.Timeout != nil &&
        httpRoute.Traffic.Timeout.HTTP != nil &&
        httpRoute.Traffic.Timeout.HTTP.IdleTimeout != nil {
        return durationpb.New(httpRoute.Traffic.Timeout.HTTP.IdleTimeout.Duration)
    }

    // ... existing auto-derivation logic unchanged ...
}
```

#### Rationale

This is the most impactful gap. Users with mixed streaming and non-streaming routes on
the same listener need per-route idle timeout control. The auto-derivation cannot handle
this case. A streaming route might need `idleTimeout: 0s` (disabled) while a REST route
on the same listener needs `idleTimeout: 5m`.

#### Backwards Compatibility

When `idleTimeout` is not set, behavior is identical to today (auto-derivation applies).
The auto-derivation code path is preserved as the default.

#### Issues Resolved

- GH#2611: Per-route idle timeout in BTP

---

### Change 2: Document the Timeout Hierarchy (GH#2509, GH#6726, GH#8193)

**Priority: HIGH**

Write comprehensive timeout documentation covering all BTP and CTP fields. The current
docs (`site/content/en/latest/tasks/traffic/http-timeouts.md`) only cover
`HTTPRoute.timeouts.request` with a basic example. They don't mention BTP or CTP
timeout fields at all.

#### Documentation Structure

Create or expand `site/content/en/latest/tasks/traffic/http-timeouts.md` with:

**Section 1: Timeout Hierarchy Diagram**

```
Client -> [Listener/HCM Timeouts] -> [Filter Chain] -> [Route Timeouts] -> Upstream

Downstream (CTP):                    Route (BTP / HTTPRoute):        Upstream (BTP):
  requestReceivedTimeout               requestTimeout                  connectTimeout
  streamIdleTimeout                     idleTimeout (auto/explicit)    connectionIdleTimeout
  idleTimeout                           maxStreamDuration              maxConnectionDuration
                                        perTryTimeout (retry)
```

**Section 2: Field Reference Table**

Full mapping of all EG fields to Envoy equivalents with defaults and descriptions
(based on the table in the Current State section above).

**Section 3: Common Interactions and Gotchas**

Document these specific interactions:

1. **`perTryTimeout` applies to ALL attempts, including the initial request.**
   When retries are configured, `perTryTimeout` caps each individual attempt. The
   overall `requestTimeout` acts as the total budget across all attempts. Users should
   set `perTryTimeout <= requestTimeout`. (Addresses GH#6726)

2. **Fault injection delay is NOT subject to route `requestTimeout`.**
   Fault injection runs in the HTTP filter chain BEFORE the request is forwarded
   upstream. The route timeout only starts counting after forwarding. If you need a
   total latency cap including fault injection, use CTP `requestReceivedTimeout` or
   `streamIdleTimeout`. (Addresses GH#8193)

3. **Route idle timeout auto-derivation.**
   When `requestTimeout` is set but no explicit CTP `streamIdleTimeout` or BTP
   `idleTimeout` is configured, Envoy Gateway auto-sets the route idle timeout to
   `max(1 hour, requestTimeout)` to prevent premature stream closure. This auto-config
   is skipped when CTP `streamIdleTimeout` is explicitly set.

4. **Gateway API `HTTPRoute.timeouts.request` takes precedence over BTP `requestTimeout`.**
   If both are set, the HTTPRoute value wins.

5. **When retries are configured, `HTTPRoute.timeouts.backendRequest` maps to per-try
   timeout, not overall timeout.**
   See `internal/gatewayapi/route.go:504-525`.

**Section 4: Decision Tree for Common Scenarios**

| Scenario | Configuration |
|----------|--------------|
| Cap total request latency | BTP `timeout.http.requestTimeout` or HTTPRoute `timeouts.request` |
| Cap each retry attempt | BTP `retry.perRetry.timeout` |
| Detect stalled connections | CTP `timeout.http.streamIdleTimeout` |
| Different idle timeout per route | BTP `timeout.http.idleTimeout` (proposed Change 1) |
| Cap time waiting for request headers | CTP `timeout.http.requestReceivedTimeout` |
| Long-running streaming requests | BTP `timeout.http.requestTimeout: "0s"` with appropriate `idleTimeout` |

#### Issues Resolved

- GH#2509: Documentation for BTP timeout fields
- GH#6726: Clarifies `perTryTimeout` semantics (documentation fix, not code change)
- GH#8193: Clarifies fault injection / timeout interaction (documentation fix)

---

### Change 3: API Validation Warnings for Common Misconfiguration

**Priority: MEDIUM**

Add informational status conditions when potentially confusing timeout configurations
are detected.

#### Warning 1: `perTryTimeout` exceeds `requestTimeout`

When `retry.perRetry.timeout > timeout.http.requestTimeout`, emit a status condition:

```
Type: Warning
Reason: TimeoutMisconfiguration
Message: "perTryTimeout (30s) exceeds requestTimeout (10s); each retry attempt
          will be capped by requestTimeout before perTryTimeout takes effect"
```

This warns users about a configuration that is almost certainly a mistake -- if the
per-try budget exceeds the total budget, the per-try timeout is effectively unused.

#### Warning 2: `faultInjection.delay` exceeds `requestTimeout`

When `faultInjection.delay.fixedDelay > timeout.http.requestTimeout`, emit:

```
Type: Warning
Reason: TimeoutMisconfiguration
Message: "faultInjection delay (5s) exceeds requestTimeout (2s); fault injection
          delay is applied before route timeout starts, so requests will always
          exceed the route timeout. Consider using CTP streamIdleTimeout for a
          total latency cap."
```

#### Implementation

Add validation in `internal/gatewayapi/backendtrafficpolicy.go` where BTP is
translated to IR. These are informational warnings set as status conditions on the
BTP resource, not admission rejections.

#### Issues Resolved

- Proactive mitigation for GH#6726 and GH#8193 patterns

---

### Change 4: Consider Exposing `per_try_idle_timeout`

**Priority: LOW**

Add `perRetry.idleTimeout` to the retry configuration, mapping to Envoy's
`RetryPolicy.per_try_idle_timeout`. This is useful for streaming retry scenarios where
detecting a stalled attempt is important.

#### API Change

```go
// api/v1alpha1/retry_types.go

type PerRetryPolicy struct {
    Timeout *gwapiv1.Duration `json:"timeout,omitempty"`
    BackOff *BackOffPolicy    `json:"backOff,omitempty"`

    // NEW: IdleTimeout specifies the idle timeout per retry attempt. If not set,
    // there is no per-try idle timeout. Set this when retrying streaming RPCs
    // where a stalled attempt should trigger a retry.
    //
    // +optional
    IdleTimeout *gwapiv1.Duration `json:"idleTimeout,omitempty"`
}
```

This is lower priority because the use case (streaming retries) is niche. Include it
if the other changes are accepted, or defer to a follow-up.

## Default Values Summary

| Timeout | EG Default | Envoy Default | Notes |
|---|---|---|---|
| Connect timeout | 10s | 5s | EG is more generous |
| Route timeout | not set (Envoy applies 15s) | 15s | Potential surprise for users |
| Route idle timeout | auto: max(1hr, requestTimeout) | not set | Auto-derived, proposed to be user-configurable |
| Stream idle timeout | not set (Envoy applies 5min) | 5 min | CTP field |
| Connection idle timeout (downstream) | not set (Envoy applies 1hr) | 1 hour | CTP field |
| Connection idle timeout (upstream) | not set (Envoy applies 1hr) | 1 hour | BTP field |
| Request received timeout | not set (disabled) | disabled | CTP field |
| Per-try timeout | not set | not set | BTP retry field |

### Notable Default Gaps

1. **Route timeout:** When EG doesn't set `RouteAction.timeout`, Envoy applies its 15s
   default. Users expecting "no timeout" get 15s. This matches Gateway API semantics
   (implementation-specific default) but should be documented.

2. **Connect timeout:** EG defaults to 10s vs Envoy's 5s. Minor, but worth noting.

## Migration / Backwards Compatibility

All proposed changes are backwards-compatible:

| Change | Existing Behavior | New Behavior When Not Using New Fields |
|--------|-------------------|----------------------------------------|
| BTP `idleTimeout` | Auto-derived from request timeout | Identical (auto-derivation preserved) |
| Documentation | Undocumented | No API change |
| Validation warnings | No warnings | Opt-in via existing status conditions |
| `perRetry.idleTimeout` | Not available | Identical (field optional) |

No migration is required. No existing configurations are affected.

## Issues Resolution Map

| GitHub Issue | Classification | Resolved By |
|---|---|---|
| GH#2509 | Documentation gap | Change 2 (documentation) |
| GH#2611 | Feature request | Change 1 (`idleTimeout` in BTP) |
| GH#6726 | Expected behavior + doc gap | Change 2 (documentation) + Change 3 (validation warning) |
| GH#7806 | Already resolved (PR#8058) | No action needed |
| GH#8193 | Expected behavior + doc gap | Change 2 (documentation) + Change 3 (validation warning) |

## Implementation Plan

1. **Phase 1 (Documentation):** Write the timeout hierarchy docs (Change 2). This has
   the highest leverage -- it preempts support issues and can be merged independently.

2. **Phase 2 (API):** Add `idleTimeout` to BTP HTTPTimeout (Change 1). Requires API
   type change, IR update, translation logic change, and tests.

3. **Phase 3 (Validation):** Add status condition warnings (Change 3). Can be done
   alongside or after Phase 2.

4. **Phase 4 (Optional):** Add `perRetry.idleTimeout` (Change 4). Defer unless
   there's explicit demand.
