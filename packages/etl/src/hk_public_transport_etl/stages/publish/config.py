from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PublishConfig:
    bundle_id: str = "hk_public_transport"
    schema_version: int = 1
    min_app_version: str = "0.1.0"

    refuse_on_failed_validation: bool = True
    fail_on_warn: bool = False

    # Allow re-publishing the same version by replacing the existing dir.
    overwrite: bool = True

    signing_private_key_path: str | None = None

    etl_version: str = "1.0.0"
    git_commit: str = "unknown"
    deterministic: bool = True

    def to_dict(self) -> dict[str, object]:
        return {
            "bundle_id": self.bundle_id,
            "schema_version": self.schema_version,
            "min_app_version": self.min_app_version,
            "refuse_on_failed_validation": self.refuse_on_failed_validation,
            "fail_on_warn": self.fail_on_warn,
            "overwrite": self.overwrite,
            "signing_private_key_path": self.signing_private_key_path,
            "etl_version": self.etl_version,
            "git_commit": self.git_commit,
            "deterministic": self.deterministic,
        }
