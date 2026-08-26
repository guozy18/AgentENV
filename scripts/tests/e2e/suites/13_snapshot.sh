#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "13_snapshot"

log "Suite: Snapshot Lifecycle"

snapshot_alias="e2e-snapshot-$(date +%s%N)"
rejected_local_snapshot_alias="e2e-local-snapshot-$(date +%s%N)"

# -- Create source sandbox --
source_sandbox_id=$(create_sandbox "$AENV_TEMPLATE_ID" 60); _sync_http
assert_status "$HTTP_STATUS" "201" "create source sandbox"
assert_not_empty "$source_sandbox_id" "source sandbox ID is present"
track_sandbox "$source_sandbox_id"

if wait_for_sandbox_state "$source_sandbox_id" "running" 30; then
  _pass "source sandbox reaches running state"
else
  _fail "source sandbox reaches running state" "running" "timeout"
fi

# -- Local snapshots are ID-only; aliases are rejected before capture --
api_post "/sandboxes/${source_sandbox_id}/snapshots" "$(jq -nc \
  --arg name "$rejected_local_snapshot_alias" \
  '{name: $name, snapshotType: "local"}')"
assert_status "$HTTP_STATUS" "400" "create Local snapshot with name returns 400"

api_post "/sandboxes/${source_sandbox_id}/snapshots" '{"snapshotType":"local"}'
assert_status "$HTTP_STATUS" "201" "create Local snapshot returns 201"

local_snapshot_id=$(echo "$HTTP_BODY" | jq -r '.snapshotID // empty')
local_snapshot_type=$(echo "$HTTP_BODY" | jq -r '.snapshotType // empty')
assert_not_empty "$local_snapshot_id" "Local snapshotID is present"
assert_eq "$(echo "$HTTP_BODY" | jq -c '.names // []')" "[]" "Local snapshot has no aliases"
assert_eq "$local_snapshot_type" "local" "Local snapshot response reports local availability"

api_get "/snapshots/${local_snapshot_id}"
assert_status "$HTTP_STATUS" "200" "GET Local snapshot by ID returns 200"
assert_eq "$(echo "$HTTP_BODY" | jq -r '.snapshotID // empty')" "$local_snapshot_id" \
  "GET Local snapshot preserves its ID"
assert_eq "$(echo "$HTTP_BODY" | jq -r '.snapshotType // empty')" "local" \
  "GET Local snapshot reports local availability"

api_get "/snapshots?sandboxID=${source_sandbox_id}"
assert_status "$HTTP_STATUS" "200" "list snapshots includes Local snapshot"
listed_local_type=$(echo "$HTTP_BODY" | jq -r --arg id "$local_snapshot_id" \
  '[.[] | select(.snapshotID == $id)][0].snapshotType // empty')
assert_eq "$listed_local_type" "local" "listed Local snapshot reports local availability"

local_relaunch_id=$(create_sandbox "$local_snapshot_id" 60); _sync_http
assert_status "$HTTP_STATUS" "201" "create sandbox from Local snapshot ID"
assert_not_empty "$local_relaunch_id" "Local snapshot relaunch ID is present"
track_sandbox "$local_relaunch_id"
if wait_for_sandbox_state "$local_relaunch_id" "running" 30; then
  _pass "Local snapshot ID launches a running sandbox"
else
  _fail "Local snapshot ID launches a running sandbox" "running" "timeout"
fi
delete_sandbox "$local_relaunch_id"
assert_status "$HTTP_STATUS" "204" "delete sandbox launched from Local snapshot"

# -- Promote in place and launch through the preserved ID --
api_post "/snapshots/${local_snapshot_id}/promote"
assert_status "$HTTP_STATUS" "200" "promote Local snapshot returns 200"
assert_eq "$(echo "$HTTP_BODY" | jq -r '.snapshotID // empty')" "$local_snapshot_id" \
  "promotion preserves snapshot ID"
assert_eq "$(echo "$HTTP_BODY" | jq -c '.names // []')" "[]" "promotion keeps Local snapshot aliasless"
assert_eq "$(echo "$HTTP_BODY" | jq -r '.snapshotType // empty')" "distributed" \
  "promotion reports distributed availability"

api_get "/snapshots/${local_snapshot_id}"
assert_status "$HTTP_STATUS" "200" "GET promoted snapshot by ID returns 200"
assert_eq "$(echo "$HTTP_BODY" | jq -r '.snapshotID // empty')" "$local_snapshot_id" \
  "promoted ID resolves to the original snapshot"
assert_eq "$(echo "$HTTP_BODY" | jq -r '.snapshotType // empty')" "distributed" \
  "promoted ID reports distributed availability"

api_post "/snapshots/${local_snapshot_id}/promote"
assert_status "$HTTP_STATUS" "200" "repeated promotion is idempotent"
assert_eq "$(echo "$HTTP_BODY" | jq -r '.snapshotID // empty')" "$local_snapshot_id" \
  "repeated promotion preserves snapshot ID"
assert_eq "$(echo "$HTTP_BODY" | jq -r '.snapshotType // empty')" "distributed" \
  "repeated promotion remains distributed"

promoted_id_relaunch_id=$(create_sandbox "$local_snapshot_id" 60); _sync_http
assert_status "$HTTP_STATUS" "201" "create sandbox from promoted snapshot ID"
assert_not_empty "$promoted_id_relaunch_id" "promoted ID relaunch ID is present"
track_sandbox "$promoted_id_relaunch_id"
if wait_for_sandbox_state "$promoted_id_relaunch_id" "running" 30; then
  _pass "promoted snapshot ID launches a running sandbox"
else
  _fail "promoted snapshot ID launches a running sandbox" "running" "timeout"
fi
delete_sandbox "$promoted_id_relaunch_id"
assert_status "$HTTP_STATUS" "204" "delete sandbox launched from promoted snapshot ID"

# -- Capture snapshot from source sandbox --
api_post "/sandboxes/${source_sandbox_id}/snapshots" "$(jq -nc \
  --arg name "$snapshot_alias" \
  '{name: $name}')"
assert_status "$HTTP_STATUS" "201" "POST /sandboxes/{id}/snapshots returns 201"

snapshot_id=$(echo "$HTTP_BODY" | jq -r '.snapshotID // empty')
snapshot_name=$(echo "$HTTP_BODY" | jq -r '.names[0] // empty')
assert_not_empty "$snapshot_id" "snapshotID is present"
assert_not_empty "$snapshot_name" "snapshot name is present"
assert_contains "$snapshot_name" "$snapshot_alias" "snapshot name contains requested alias"
assert_eq "$(echo "$HTTP_BODY" | jq -r '.snapshotType // empty')" "distributed" \
  "omitted snapshotType defaults to distributed availability"

# -- List snapshots filtered by source sandbox --
api_get "/snapshots?sandboxID=${source_sandbox_id}"
assert_status "$HTTP_STATUS" "200" "GET /snapshots filtered by sandboxID returns 200"

listed_snapshot_count=$(echo "$HTTP_BODY" | jq -r --arg id "$snapshot_id" \
  '[.[] | select(.snapshotID == $id)] | length')
assert_eq "$listed_snapshot_count" "1" "snapshot appears in filtered snapshot list"

# -- Relaunch from snapshot name --
relaunched_sandbox_id=$(create_sandbox "$snapshot_name" 60); _sync_http
assert_status "$HTTP_STATUS" "201" "create sandbox from snapshot name"
assert_not_empty "$relaunched_sandbox_id" "relaunched sandbox ID is present"
track_sandbox "$relaunched_sandbox_id"

if wait_for_sandbox_state "$relaunched_sandbox_id" "running" 30; then
  _pass "sandbox created from snapshot reaches running state"
else
  _fail "sandbox created from snapshot reaches running state" "running" "timeout"
fi

# -- Delete source sandbox; snapshot should remain reusable --
delete_sandbox "$source_sandbox_id"
assert_status "$HTTP_STATUS" "204" "delete source sandbox"

api_get "/sandboxes/${source_sandbox_id}"
assert_status "$HTTP_STATUS" "404" "deleted source sandbox returns 404"

api_get "/snapshots?sandboxID=${source_sandbox_id}"
assert_status "$HTTP_STATUS" "200" "GET /snapshots still works after deleting source sandbox"
listed_after_delete=$(echo "$HTTP_BODY" | jq -r --arg id "$snapshot_id" \
  '[.[] | select(.snapshotID == $id)] | length')
assert_eq "$listed_after_delete" "1" "snapshot remains listed after deleting source sandbox"

# -- Snapshot remains launchable after source sandbox deletion --
reused_sandbox_id=$(create_sandbox "$snapshot_name" 60); _sync_http
assert_status "$HTTP_STATUS" "201" "create second sandbox from snapshot after source deletion"
assert_not_empty "$reused_sandbox_id" "reused sandbox ID is present"
track_sandbox "$reused_sandbox_id"

if wait_for_sandbox_state "$reused_sandbox_id" "running" 30; then
  _pass "snapshot remains launchable after source sandbox deletion"
else
  _fail "snapshot remains launchable after source sandbox deletion" "running" "timeout"
fi

# -- Cleanup relaunched sandboxes --
delete_sandbox "$relaunched_sandbox_id"
assert_status "$HTTP_STATUS" "204" "delete first sandbox created from snapshot"

delete_sandbox "$reused_sandbox_id"
assert_status "$HTTP_STATUS" "204" "delete second sandbox created from snapshot"

# Public reusable-snapshot deletion is intentionally not exposed. Both snapshots
# remain inside the suite's isolated AENV_HOME and are removed with that test root.

suite_summary "13_snapshot"
