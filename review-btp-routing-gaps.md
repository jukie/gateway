# Review: feat-routing-type-btp — Unhandled BTP Config Gaps

**Branch:** `feat-routing-type-btp` (in `gateway/crew/jukie`)
**Diff against:** `main`
**Reviewer:** obsidian (polecat)
**Issue:** ga-0zv9

## Summary

The branch adds BTP-level `routingType` override support, allowing per-route/gateway/listener
override of the EnvoyProxy-level `routingType` setting. The core implementation is solid:
`GetBTPRoutingTypeForRoute()` resolves BTP routing type with correct priority (route > listener > gateway),
and `IsServiceRouting()` was updated to incorporate BTP routing type in the priority hierarchy.

## Gaps Found

### Gap 1: `targetSelectors` not handled in `GetBTPRoutingTypeForRoute`
**File:** `internal/gatewayapi/backendtrafficpolicy.go:61`
**Severity:** Medium

`GetBTPRoutingTypeForRoute()` calls `btp.Spec.GetTargetRefs()` to resolve which resources
a BTP targets. However, `GetTargetRefs()` only returns `targetRef` or `targetRefs` — it
does **not** return targets matched by `targetSelectors` (label-based targeting). The
`BackendTrafficPolicySpec` validation allows `targetSelectors` as a valid alternative:

```
+kubebuilder:validation:XValidation:rule="(has(self.targetRef) && !has(self.targetRefs)) ||
  (!has(self.targetRef) && has(self.targetRefs)) ||
  (has(self.targetSelectors) && self.targetSelectors.size() > 0)"
```

If a BTP uses `targetSelectors` with `routingType` set, the routing type override will
be silently ignored during destination processing because `GetBTPRoutingTypeForRoute`
won't find any matching targetRefs.

Note: The existing BTP processing in `ProcessBackendTrafficPolicies()` *does* handle
`targetSelectors` via `getPolicyTargetRefs()` which resolves selectors to concrete refs.
But the new `GetBTPRoutingTypeForRoute()` operates on raw BTP objects and doesn't have
access to the resolved selector targets.

### Gap 2: `Namespace` field on `targetRef` is ignored
**File:** `internal/gatewayapi/backendtrafficpolicy.go:63`
**Severity:** Low (BTP enforces same-namespace, but worth noting)

`GetBTPRoutingTypeForRoute()` uses `btp.Namespace` as the reference namespace for all
targetRefs. This is correct for `targetRef` (which is `Local` and enforces same-namespace).
However, `targetRefs` entries are also `LocalPolicyTargetReferenceWithSectionName` which
by definition don't have a `Namespace` field — so this is actually fine. No gap here after
closer inspection; the code is correct.

**Status: NOT A GAP** (keeping for completeness of analysis)

### Gap 3: Multiple BTPs targeting the same route — last-writer-wins without deterministic ordering
**File:** `internal/gatewayapi/backendtrafficpolicy.go:56-72`
**Severity:** Medium

When multiple BTPs target the same route and both set `routingType`, the function iterates
`btps` and returns the **first** match (for route-level) or keeps the **last** match (for
gateway/listener-level, since variables are overwritten in the loop). This creates
inconsistent priority behavior:

- **Route-level:** First BTP encountered wins (returns immediately on line 72)
- **Gateway/listener-level:** Last BTP encountered wins (variable overwrite on lines 83/86)

The existing `ProcessBackendTrafficPolicies()` flow handles conflicts by marking the second
BTP as "Conflicted" and rejecting it. But `GetBTPRoutingTypeForRoute()` operates on the raw
unfiltered list of all BTPs (`resources.BackendTrafficPolicies`), meaning it may see BTPs
that would be rejected by the normal conflict resolution process. This could lead to a
different routing type being applied at the destination level than what the BTP policy
status indicates.

### Gap 4: `ext_service.go` always passes `nil` for BTP routing type
**File:** `internal/gatewayapi/ext_service.go:113,118,139`
**Severity:** Medium

The `processExtServiceDestination()` function passes `nil` as the `btpRoutingType` parameter
to `processServiceDestinationSetting()`, `processServiceImportDestinationSetting()`, and
`IsServiceRouting()`. This means ext-auth, rate-limit, and other extension service backends
will **never** respect BTP routing type overrides — they'll always fall back to the
EnvoyProxy-level setting.

This may be intentional (extension services are infrastructure, not application backends),
but it's not documented and could surprise users who set `routingType: Service` on a BTP
expecting all backends in scope to use ClusterIP routing.

### Gap 5: `listener.go` and `globalresources.go` always pass `nil` for BTP routing type
**Files:**
- `internal/gatewayapi/listener.go:951`
- `internal/gatewayapi/globalresources.go:88`
**Severity:** Low

Both `processBackendRefs()` in listener.go and `processServiceClusterForGateway()` in
globalresources.go pass `nil` as BTP routing type. These are for infrastructure-level
backends (listener backend refs, global service clusters) rather than user-facing route
backends, so this is likely intentional. But the behavior gap should be documented.

### Gap 6: No validation that `routingType` is only set with supported route types
**File:** `api/v1alpha1/backendtrafficpolicy_types.go:126-132`
**Severity:** Low

The BTP spec allows `routingType` to be set on BTPs targeting any route kind including
`UDPRoute`, `TCPRoute`, and `TLSRoute`. For these non-HTTP route types, routing type
only affects endpoint selection (Service ClusterIP vs EndpointSlice IPs). This is valid
and works correctly, but there's no CEL validation or documentation clarifying that
`routingType` has the same effect across all route types. This is consistent with how
the EnvoyProxy-level `routingType` works, so it's not really a bug — just a doc gap.

### Gap 7: `RoutingType` not reflected in IR `TrafficFeatures` — no status/observability
**File:** `internal/gatewayapi/backendtrafficpolicy.go:913-1017`
**Severity:** Low

The `buildTrafficFeatures()` function constructs `ir.TrafficFeatures` from a BTP, but
`RoutingType` is not included in `TrafficFeatures` (it's resolved separately in
`processDestination()`). This means:

1. There's no status condition or event when a BTP's `routingType` takes effect or
   is overridden
2. Users can't easily observe which routing type was actually applied to a given route
3. The `RoutingType` field is not visible in the IR, making debugging harder

This is an observability gap rather than a functional bug.

### Gap 8: `IsServiceRouting` EnvoyProxy fallback changed behavior for unrecognized values
**File:** `internal/gatewayapi/translator.go:543-544`
**Severity:** Low

The old `IsEnvoyServiceRouting` had an explicit `default: return false` case for
unrecognized `RoutingType` values. The new `IsServiceRouting` checks:
```go
if envoyProxy != nil && envoyProxy.Spec.RoutingType != nil &&
    *envoyProxy.Spec.RoutingType != egv1a1.EndpointRoutingType {
    return true
}
```

This means any **unrecognized** routing type string (not "Endpoint" and not "Service")
would now result in service routing being enabled, whereas before it would have defaulted
to endpoint routing. This is a minor behavioral change but could be surprising if new
routing type values are added in the future.

## Test Coverage Assessment

The branch adds:
- `TestGetBTPRoutingTypeForRoute` — Unit tests for the routing type resolution function
- 7 new testdata scenarios covering: endpoint, gateway, listener, override, service, mixed-btp, and endpoint-btp-service

**Missing test coverage:**
- No test for `targetSelectors` with `routingType` (Gap 1)
- No test for conflicting BTPs with `routingType` (Gap 3)
- No integration test verifying ext-service behavior with BTP routing type (Gap 4)
- No test for BTP merge (`mergeType`) interaction with `routingType`

## Recommendations

1. **Gap 1 (targetSelectors):** Either add selector-based resolution to `GetBTPRoutingTypeForRoute()` or add validation to prevent `routingType` + `targetSelectors` combination.
2. **Gap 3 (conflict ordering):** Pass pre-filtered/conflict-resolved BTPs to `GetBTPRoutingTypeForRoute()` instead of the raw list, or add deterministic ordering.
3. **Gap 4 (ext_service):** Document the intentional exclusion of extension services from BTP routing type, or implement routing type resolution for them.
4. **Gap 8 (fallback behavior):** Add explicit handling for unrecognized routing type values to maintain backward compatibility.
