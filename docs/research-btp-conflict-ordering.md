# Research: BTP Conflict Ordering in GetBTPRoutingTypeForRoute

**Issue:** ga-7lu6 (Gap 3 from review ga-0zv9)
**Branch:** `feat-routing-type-btp`
**Status:** Read-only research — no code changes

---

## Executive Summary

`GetBTPRoutingTypeForRoute()` bypasses the established BTP conflict resolution system by operating on the raw, unfiltered `resources.BackendTrafficPolicies` list. This is a **real bug** — not just theoretical — because it can select a routing type from a BTP that the conflict resolution system would reject as `Conflicted`. The first/last-writer inconsistency within the function is a secondary concern; the primary issue is that it operates outside the conflict resolution framework entirely.

---

## Question 1: How does ProcessBackendTrafficPolicies() conflict resolution work?

### The Mechanism

`ProcessBackendTrafficPolicies()` (backendtrafficpolicy.go:51-194) uses a **first-writer-wins** model with stateful attachment tracking:

1. **Sorting**: BTPs are pre-sorted by creation timestamp, then namespace/name (resource.go:408-417). This ensures deterministic ordering.

2. **Iteration order**: BTPs are processed in 4 priority phases:
   - Phase 1: Policies targeting RouteRules (most specific)
   - Phase 2: Policies targeting xRoutes
   - Phase 3: Policies targeting Listeners
   - Phase 4: Policies targeting Gateways (least specific)

3. **Conflict detection**: Two resolve functions track what's already attached:

   - `resolveBackendTrafficPolicyRouteTargetRef()` (line 582-639):
     - Tracks `route.attached` (boolean) — has any BTP attached to this route?
     - Tracks `route.attachedToRouteRules` (set) — which route rules have BTPs?
     - If already attached, returns `PolicyReasonConflicted` error

   - `resolveBackendTrafficPolicyGatewayTargetRef()` (line 518-580):
     - Tracks `gateway.attached` (boolean) — has any BTP attached to this gateway?
     - Tracks `gateway.attachedToListeners` (set) — which listeners have BTPs?
     - If already attached, returns `PolicyReasonConflicted` error

4. **What gets marked Conflicted**: The second (and subsequent) BTP targeting the same resource gets a `PolicyReasonConflicted` status condition on its status. The first BTP (by creation timestamp) wins.

5. **What gets filtered**: Conflicted BTPs are not prevented from appearing in the return list `res`, but their policy status is set to Conflicted, and critically, the `translateBackendTrafficPolicy*` function is NOT called for them — the conflict resolution error causes the processing to skip translation. So the conflicting BTP's settings are never applied to the IR.

### Key insight

The conflict resolution operates as a **side effect of processing** — it doesn't produce a separate "filtered" list. Instead, it prevents conflicting BTPs from being translated into IR configuration, while still recording their status.

---

## Question 2: Does GetBTPRoutingTypeForRoute() receive raw or post-conflict BTPs?

**Raw, unfiltered BTPs.**

In `processDestination()` (route.go, feat branch), the call is:

```go
btpRoutingType = GetBTPRoutingTypeForRoute(
    resources.BackendTrafficPolicies,   // <-- raw list from resources
    routeNN,
    routeType,
    gatewayNN,
    parentRef.SectionName,
)
```

`resources.BackendTrafficPolicies` is the original sorted list from the provider layer. It contains **all** BTPs, including ones that `ProcessBackendTrafficPolicies()` would mark as Conflicted.

### Why this matters

`processDestination()` is called **during route processing**, which happens in the translator's `Translate()` method. The call flow is:

1. `Translate()` processes routes (which calls `processDestination()` → `GetBTPRoutingTypeForRoute()`)
2. `Translate()` later calls `ProcessBackendTrafficPolicies()` (translator.go:316)

So `GetBTPRoutingTypeForRoute()` runs **before** conflict resolution has even happened. Even if it received a "post-conflict" list, that list doesn't exist yet at the time it's called.

**Update**: Looking more carefully at the translator flow, `ProcessBackendTrafficPolicies` is called at line 316 of translator.go, while route processing happens earlier. The BTP routing type resolution in `processDestination()` occurs during route translation, which is indeed before `ProcessBackendTrafficPolicies()` runs. This confirms the function operates on pre-conflict-resolution data.

---

## Question 3: Can two BTPs target the same route with different routingType values?

**Yes.** There is no validation preventing this. A user can create:

```yaml
apiVersion: gateway.envoyproxy.io/v1alpha1
kind: BackendTrafficPolicy
metadata:
  name: btp-service-routing
  namespace: default
  creationTimestamp: "2024-01-01T00:00:00Z"
spec:
  targetRef:
    group: gateway.networking.k8s.io
    kind: HTTPRoute
    name: my-route
  routingType: Service
---
apiVersion: gateway.envoyproxy.io/v1alpha1
kind: BackendTrafficPolicy
metadata:
  name: btp-endpoint-routing
  namespace: default
  creationTimestamp: "2024-01-02T00:00:00Z"
spec:
  targetRef:
    group: gateway.networking.k8s.io
    kind: HTTPRoute
    name: my-route
  routingType: Endpoint
```

In `ProcessBackendTrafficPolicies()`, `btp-service-routing` would win (older timestamp), and `btp-endpoint-routing` would get `PolicyReasonConflicted`. The route would use Service routing for all BTP-translated features.

But in `GetBTPRoutingTypeForRoute()`, which BTP wins depends on iteration order. Since BTPs are sorted by creation timestamp, `btp-service-routing` would also win here (it's first in the list, and the function returns immediately on route-level match). **In this specific case, the results happen to agree.**

### The gateway/listener case is where it diverges

```yaml
# BTP 1: targets Gateway, sets Service routing (created first)
spec:
  targetRef:
    kind: Gateway
    name: my-gateway
  routingType: Service

# BTP 2: targets Gateway, sets Endpoint routing (created second)
spec:
  targetRef:
    kind: Gateway
    name: my-gateway
  routingType: Endpoint
```

- `ProcessBackendTrafficPolicies()`: BTP 1 wins (first-writer, older timestamp). BTP 2 is Conflicted. Gateway uses Service routing.
- `GetBTPRoutingTypeForRoute()`: Iterates all BTPs. BTP 1 sets `gatewayBTPRoutingType = Service`. BTP 2 **overwrites** it: `gatewayBTPRoutingType = Endpoint`. Returns **Endpoint**.

**Result**: The BTP status says Service routing is active (BTP 1 accepted, BTP 2 conflicted), but the actual destination routing uses Endpoint routing (from BTP 2 overwriting in the loop).

---

## Question 4: Concrete example of inconsistent behavior

### Scenario: Two BTPs target the same gateway with different routingType

**Setup:**
- Gateway `default/my-gw` with listener `http`
- HTTPRoute `default/my-route` attached to `my-gw/http`
- EnvoyProxy has no routingType set (default Endpoint routing)

**BTPs (sorted by creation timestamp):**

| BTP | Created | Target | routingType |
|-----|---------|--------|-------------|
| btp-a | 2024-01-01 | Gateway/my-gw | Service |
| btp-b | 2024-01-02 | Gateway/my-gw | Endpoint |

**What ProcessBackendTrafficPolicies() does:**
1. Iterates BTPs in order (btp-a first, btp-b second)
2. btp-a targets Gateway/my-gw → `gateway.attached = true` → btp-a is accepted
3. btp-b targets Gateway/my-gw → `gateway.attached` is already true → btp-b gets `PolicyReasonConflicted`
4. btp-a's settings (including any traffic features) are translated to IR
5. btp-b's settings are NOT translated (conflicted)

**User sees in status:**
- btp-a: Accepted (routingType: Service)
- btp-b: Conflicted ("another BackendTrafficPolicy has already attached")

**What GetBTPRoutingTypeForRoute() does:**
1. Iterates ALL BTPs (both btp-a and btp-b, since it uses raw `resources.BackendTrafficPolicies`)
2. btp-a: routingType=Service, targets Gateway/my-gw → sets `gatewayBTPRoutingType = Service`
3. btp-b: routingType=Endpoint, targets Gateway/my-gw → **overwrites** `gatewayBTPRoutingType = Endpoint`
4. Returns `Endpoint`

**Actual routing behavior:**
- `IsServiceRouting(envoyProxy, btpRoutingType)` receives `Endpoint` → returns `false`
- Destination uses endpoint routing (EndpointSlice IPs)

**The inconsistency:**
- BTP status says: btp-a (Service) is the accepted policy
- Actual routing: Endpoint routing (from btp-b, which is marked Conflicted)

### Route-level variant

For route-level BTPs, the inconsistency is different:

| BTP | Created | Target | routingType |
|-----|---------|--------|-------------|
| btp-a | 2024-01-01 | HTTPRoute/my-route | Service |
| btp-b | 2024-01-02 | HTTPRoute/my-route | Endpoint |

- `ProcessBackendTrafficPolicies()`: btp-a wins (Service)
- `GetBTPRoutingTypeForRoute()`: btp-a wins (returns immediately on first route match)
- **No inconsistency at route level** because both use first-writer semantics

The bug only manifests at **gateway and listener levels** where the loop overwrites instead of returning early.

---

## Question 5: Real bug or theoretical concern?

**This is a real bug, not merely theoretical.** Here's why:

### Why it's real

1. **The code path is exercised**: `processDestination()` is called for every backend reference in every route. If any BTP sets `routingType`, `GetBTPRoutingTypeForRoute()` runs.

2. **The conflict scenario is natural**: Users commonly create BTPs targeting gateways for broad policy application. If a user creates a new BTP targeting the same gateway (perhaps to change routing type), the old one remains until deleted. During this window, both BTPs exist.

3. **The bug produces silent wrong behavior**: The routing type applied to destinations silently differs from what the BTP status indicates. There's no error, warning, or event. The user sees btp-a accepted with Service routing, but destinations actually use Endpoint routing from the conflicted btp-b.

4. **Kubernetes ordering matters**: BTPs are sorted by creation timestamp. Newer BTPs come later in the list. For gateway-level targets, the newer (conflicted) BTP overwrites the older (accepted) one's routing type. This is the exact opposite of what the conflict resolution system intends.

### Severity: Medium

- **Impact**: Silent routing type mismatch when multiple BTPs target the same gateway/listener with different routingType values
- **Likelihood**: Moderate — requires multiple BTPs targeting the same gateway with different routingType, which is an error state but a natural one during policy transitions
- **Blast radius**: Affects all routes under the targeted gateway/listener — could silently change routing for many services

### Recommended fix

The cleanest fix would be to have `GetBTPRoutingTypeForRoute()` use the post-conflict-resolution BTP list rather than the raw `resources.BackendTrafficPolicies`. This could be done by:

1. Running `ProcessBackendTrafficPolicies()` earlier (before route processing), or
2. Passing the accepted-only BTPs from `ProcessBackendTrafficPolicies()` to `processDestination()`, or
3. Having `GetBTPRoutingTypeForRoute()` replicate the conflict resolution logic (not recommended — duplicates logic)

Option 2 is likely the cleanest approach since `ProcessBackendTrafficPolicies()` already returns the list of processed (non-conflicted) BTPs.

---

## Appendix: Code References

| Function | File | Line (main) | Notes |
|----------|------|-------------|-------|
| `GetBTPRoutingTypeForRoute()` | backendtrafficpolicy.go | 46-96 (feat branch) | New function on feat branch |
| `ProcessBackendTrafficPolicies()` | backendtrafficpolicy.go | 51-194 | Conflict resolution entry point |
| `resolveBackendTrafficPolicyGatewayTargetRef()` | backendtrafficpolicy.go | 518-580 | Gateway/listener conflict detection |
| `resolveBackendTrafficPolicyRouteTargetRef()` | backendtrafficpolicy.go | 582-639 | Route/routerule conflict detection |
| `processDestination()` | route.go | ~1849 (feat branch) | Where GetBTPRoutingTypeForRoute is called |
| `IsServiceRouting()` | translator.go | 528-548 (feat branch) | BTP routing type priority over EnvoyProxy |
| BTP sort | resource.go | 408-417 | Sorted by creation timestamp, then ns/name |
| `GetTargetRefs()` | policy_helpers.go | 65-70 | Returns targetRef/targetRefs (NOT selectors) |
| `getPolicyTargetRefs()` | helpers.go | 657-711 | Returns targetRef/targetRefs AND resolved selectors |
