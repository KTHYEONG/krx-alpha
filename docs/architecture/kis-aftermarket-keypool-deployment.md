# KIS Aftermarket Shared Keypool Deployment

Implements contract change `kis_aftermarket_keypool_deployment_runbook`.

Shared KIS data-key env and VPS activation runbook.

## 1. Secret source (local)

- Local `/home/kth/.quant.env` is the symlinked authoring source `/home/kth/.quant_env.sh`,
  not a runtime input. Its resolved target stays `0600`.
- No secret reaches git, docs, logs, argv, or tests. All validation below is
  fingerprint-only (`sha256(app_key)[:12]`); never print `APP_SECRET` values.

## 2. Verified pool assignment

- Verified pool is slots `1,2,3,4,5`. Active collection assignment is ordered `1,2,3,4`;
  slot `5` is reserve.
- Shared fragment sets:

  ```sh
  KIS_DATA_SLOTS=1,2,3,4,5
  KIS_HOST_DATA_SLOTS=1,2,3,4
  ```

- Failure mode guard: local edits are NOT auto-synced to the VPS. Every change must be
  explicitly copied to the VPS fragment and re-validated (no assumption of automatic
  reflection).

## 3. Shared fragment (VPS)

- Create `/home/ubuntu/quant-secrets/kis-data.env` with mode `0600` owner `ubuntu:ubuntu`,
  from only `KIS_DATA_SLOTS`, `KIS_HOST_DATA_SLOTS` and
  `KIS_DATA_<1..5>_{APP_KEY,APP_SECRET,HTS_ID}`.
- Excluded from the fragment: account fields, `KIS_TRADE_*`, `KIS_APP_*`, and all
  unrelated credentials.
- Failure mode guard: copying a trade key (`KIS_TRADE_*`) or primary key (`KIS_APP_*`)
  into the fragment is rejected during fingerprint validation.
- Provision the fragment only from a trusted workstation with the audited CLI
  (it never runs in GitHub Actions):

  ```sh
  uv run python -m src.cli.provision_kis_keypool --host or-vps
  ```

  The CLI reads the workstation source (`~/.quant.env` by default, override with
  `--source`), builds the canonical 17-line fragment, and installs it at the
  canonical path `/home/ubuntu/quant-secrets/kis-data.env` with mode `0600`
  owner `ubuntu:ubuntu` over SSH. Preview without installing via `--dry-run`.
- The CLI writes the VPS-specific selectors itself:
  `KIS_DATA_SLOTS=1,2,3,4,5` and `KIS_HOST_DATA_SLOTS=1,2,3,4`. Any selector
  copies in the workstation source are ignored, so a local host-only
  assignment cannot affect the VPS.
- CI has a validation-only role: the deploy workflow runs the remote
  `validate_shared_keypool()` check before KCA/Compose wiring and fails closed
  unless the shared file exists with mode `0600` owner `ubuntu:ubuntu`,
  contains only the 17 canonical keys, and holds nonempty values for all 15
  data credentials. The validator prints only fixed status text and key names,
  never credential values.
- Rotation procedure: update the workstation source, then re-run the same CLI
  command above. Verify with `--dry-run` first when only validation is wanted,
  then deploy normally so CI validation confirms the rotated fragment.

## 4. Compose wiring (KRX)

- Compose loads `.env` first then the required shared env:

  ```yaml
  env_file:
    - .env
    - ${KIS_DATA_ENV_FILE:?KIS_DATA_ENV_FILE is required}
  ```

- `/home/ubuntu/krx-alpha/.env` contains only:

  ```sh
  KIS_DATA_ENV_FILE=/home/ubuntu/quant-secrets/kis-data.env
  KIS_SHARED_TOKEN_CACHE_DIR=/home/ubuntu/.cache/kis
  ```

  It contains no copied data key secret.
- Token cache is mounted read-only:
  `${KIS_SHARED_TOKEN_CACHE_DIR}:/run/kis-token-cache:ro`.
- Failure mode guard: `Compose 변수 누락` is blocked by the `:?` required-variable
  syntax; `docker compose config` must resolve both env files before restart.

## 5. KCA wiring (independent)

- KCA code reads project `.env` then OS environment; it is independent of Compose.
- Install a user-systemd drop-in for `kca-kis-token-warmup.service` with:

  ```ini
  [Service]
  EnvironmentFile=/home/ubuntu/quant-secrets/kis-data.env
  ```

  then `daemon-reload` and restart the timer/service.
- Do not copy data keys to the KCA `.env`.
- Failure mode guard: verify KCA warms up all four active tokens (slots 1..4);
  a missing warm-up blocks activation.

## 6. Pre-restart validation (fingerprint-only)

- Prove slots 1..4 exist, differ (distinct fingerprints), and do not collide with
  `KIS_APP_KEY` / `KIS_TRADE_APP_KEY`.
- Prove the KCA cache dir is writable by `ubuntu` and Compose mounts it read-only.
- `docker compose config` must resolve both env files.

## 7. Activation and rollback

- Restarting `krx-collector` is explicit post-deployment work.
- `KRX_ALPHA_AFTERMARKET_ENABLED` remains `false` until a separate G0 approval.
- Rollback removes only the KCA drop-in and KRX path variables
  (`KIS_DATA_ENV_FILE`, `KIS_SHARED_TOKEN_CACHE_DIR`); token cache, L0, L1, and
  manifest files are never deleted.

## 8. Performance budget

- Expected scale: 40 symbols x 2 venues x 2 streams = 160 pairs; active keys 1..4,
  reserve 5.
- Storage format: existing L0 JSONL.zst / L1 Parquet-zstd (no change).
- Numerical transformation: none (`dtype_precision`: no numerical transformation).
- Activation is configuration-only (`chunking_strategy`: configuration-only activation);
  no acceleration candidate.
