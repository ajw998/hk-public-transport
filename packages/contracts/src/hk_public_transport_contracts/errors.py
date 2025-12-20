from __future__ import annotations


class ContractsError(RuntimeError):
    """Base error for contracts package failures"""


class ContractsResourceError(ContractsError):
    """
    Raised when a required contract resource file cannot be located or read

    Deliberately raise a RuntimeError instead of FileNotFoundError so downstream code
    can treat it as a '`contract` package is broken or mispackaged' rather than
    'user path not found'.
    """


class ManifestValidationError(ContractsError):
    """Manifest instance did not validate against the shipped JSON schema"""
