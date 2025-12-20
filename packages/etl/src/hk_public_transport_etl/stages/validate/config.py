from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ValidateConfig:
    fail_on_warn: bool = True
    sample_size: int = 100

    # Hard-stop mode
    hard_stop_on_missing_core: bool = True

    # Sequences
    seq_base: int = 1
    require_contiguous_seq: bool = True

    # Pattern shape
    min_pattern_stops_warn: int = 2
    max_pattern_stops_warn: int = 200

    # Unresolved gating
    allow_unresolved_fares: bool = True

    def to_dict(self) -> dict[str, object]:
        return {
            "fail_on_warn": self.fail_on_warn,
            "sample_size": self.sample_size,
            "hard_stop_on_missing_core": self.hard_stop_on_missing_core,
            "seq_base": self.seq_base,
            "require_contiguous_seq": self.require_contiguous_seq,
            "min_pattern_stops_warn": self.min_pattern_stops_warn,
            "max_pattern_stops_warn": self.max_pattern_stops_warn,
            "allow_unresolved_fares": self.allow_unresolved_fares,
        }
