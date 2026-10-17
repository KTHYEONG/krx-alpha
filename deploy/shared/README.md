# Host broker contract v1

`host_broker_contract.py` is the canonical distribution artifact for host-shared
broker tokens and REST admission. Its initial bytes come from the verified
K-Closing Alpha reference extraction.

Initial SHA-256:
`21dacdbbcb55c87c2698d9c32a4a9d43935bc2759157d8e93ac40fb6c9eaf31b`

## Public ABI

- `TokenStore` and `AsyncTokenStore` bind a token file `path`. Supply keyword
  `vendor`, `lock_timeout_seconds`, `expiry_margin_seconds`, `clock`, and
  `issue_throttle_policy` (`"wait"` or `"raise"`) at construction.
- `get_or_issue(issue)` and `replace_rejected(rejected_token, issue)` return
  `TokenRecord`; policy is not a per-call argument.
- `token_record_path(directory, vendor, app_key)` names token records.
- `resolve_host_broker_dir` accepts `require_shared` and optional bool
  `in_container`; the directory is always caller-injected (no host-path default,
  so vendored copies pass source-hygiene gates) and `HOST_ADMISSION_MARKER`
  names the marker.
- `DEFAULT_RATES` owns vendor rates, including case-preserving Toss scopes and
  Kiwoom per-TR wildcard lookup.

## Distribution and verification

KCA consumes a byte-identical copy at `src/api/host_broker_contract.py` and pins
the same hash in its contract tests. Make future changes here first, re-vendor
the complete file, and update the hash pins together. Run each consumer's
contract suite and strict static checks before publishing an update.

KRX's publication checks are `tests/unit/core/test_host_broker_canonical.py`;
`uv run python tools/verify.py` checks the artifact and selects these guards when
it changes. KRX runtime adoption is a separate wiring change described in
`docs/specs/host_broker_contract_krx_spec.md`; it must preserve the existing
project-local rotation cooldown before switching collectors to the shared API.
