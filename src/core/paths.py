"""Persistent collector path derivations from one data root."""

from __future__ import annotations

import datetime as dt
import pathlib
from dataclasses import dataclass

DEFAULT_DATA_ROOT: pathlib.Path = pathlib.Path("data")
"""Single hardcoded data root; settings defaults derive from this constant."""

DEFAULT_HOST_BACKUP_STATUS_PATH: pathlib.Path = pathlib.Path("/run/host-backup/host_backup_status.json")
"""Read-only container mount of the status file the host backup script writes."""

DEFAULT_KIS_TOKEN_CACHE_DIR: pathlib.Path = DEFAULT_DATA_ROOT / "execution" / "kis_tokens"
"""Default KIS token cache directory derived from the single data root."""


@dataclass(frozen=True)
class DataPaths:
    """Derive every persistent collector path from one data root."""

    root: pathlib.Path

    @property
    def bars_daily_dir(self) -> pathlib.Path:
        return self.root / "bars" / "daily"

    @property
    def legacy_bars_file(self) -> pathlib.Path:
        return self.root / "bars" / "daily.parquet"

    @property
    def market_map(self) -> pathlib.Path:
        return self.root / "market_map.json"

    @property
    def program_trades_dir(self) -> pathlib.Path:
        return self.root / "bars" / "program_trades"

    @property
    def legacy_program_trades_file(self) -> pathlib.Path:
        return self.root / "bars" / "program_trades.parquet"

    @property
    def candidates(self) -> pathlib.Path:
        return self.root / "candidates.json"

    @property
    def universe_dir(self) -> pathlib.Path:
        return self.root / "universe"

    @property
    def manifest_dir(self) -> pathlib.Path:
        return self.root / "manifest"

    @property
    def journal_root(self) -> pathlib.Path:
        return self.root / "l0"

    @property
    def archive_root(self) -> pathlib.Path:
        return self.root / "l1"

    @property
    def quarantine_root(self) -> pathlib.Path:
        return self.root / "quarantine"

    @property
    def work_root(self) -> pathlib.Path:
        return self.root / "work"

    @property
    def kis_ws_lease_dir(self) -> pathlib.Path:
        return self.work_root / "kis_ws_leases"

    @property
    def logs_dir(self) -> pathlib.Path:
        return self.root / "logs"

    @property
    def calendar_cache(self) -> pathlib.Path:
        return self.root / "calendar_cache.json"

    @property
    def session_calendar_dir(self) -> pathlib.Path:
        return self.root / "calendar"

    @property
    def daemon_lifecycle(self) -> pathlib.Path:
        return self.work_root / "daemon_lifecycle.json"

    def universe_out(self, day: dt.date) -> pathlib.Path:
        return self.universe_dir / f"{day.isoformat()}.parquet"

    def snapshot_partition(self, dataset: str, day: dt.date) -> pathlib.Path:
        """Return the L1 day partition for a REST snapshot dataset.

        Snapshot partitions live under the L1 archive root so the existing remote
        offload and verified local pruning apply unchanged; the file name must keep
        the ``dt=YYYY-MM-DD`` form that retention parses.
        """
        return self.archive_root / "snapshot" / dataset / f"dt={day.isoformat()}.parquet"

    def aftermarket_candidates(self, day: dt.date) -> pathlib.Path:
        return self.universe_dir / "aftermarket" / f"{day.isoformat()}.json"

    def manifest_path(self, day: dt.date) -> pathlib.Path:
        return self.manifest_dir / f"{day.isoformat()}.json"

    def aftermarket_manifest_path(self, day: dt.date, venue: object, shard_index: int) -> pathlib.Path:
        venue_s = str(getattr(venue, "value", venue))
        return self.manifest_dir / "aftermarket" / f"{day.isoformat()}.{venue_s}.shard-{shard_index:02d}.json"

    @property
    def execution_dir(self) -> pathlib.Path:
        return self.root / "execution"

    @property
    def order_journal_dir(self) -> pathlib.Path:
        return self.execution_dir / "journal"

    @property
    def kis_token_cache(self) -> pathlib.Path:
        return self.execution_dir / "kis_token.json"

    @property
    def kill_switch_file(self) -> pathlib.Path:
        return self.execution_dir / "KILL_SWITCH"
