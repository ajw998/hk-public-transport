from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

JournalMode = Literal["DELETE", "TRUNCATE", "PERSIST", "MEMORY", "WAL", "OFF"]
SyncMode = Literal["OFF", "NORMAL", "FULL", "EXTRA"]


@dataclass(frozen=True, slots=True)
class CommitConfig:
    cache_size_kb: int = 200_000
    batch_rows: int = 50_000
    import_journal_mode: JournalMode = "WAL"
    import_synchronous: SyncMode = "NORMAL"
    final_journal_mode: JournalMode = "DELETE"
    final_synchronous: SyncMode = "FULL"
    run_analyze: bool = True
    run_optimize: bool = True
    run_vacuum: bool = True
    enforce_single_source_per_table: bool = True
    create_headway_debug_tables: bool = True
