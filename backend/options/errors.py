from enum import Enum


class ContextViolation(ValueError):
    """Raised when a point-in-time read would expose unavailable data."""


class DuplicateFactConflict(ValueError):
    """Raised when an idempotency key resolves to different immutable content."""


class InvalidBatchTransition(ValueError):
    """Raised when a raw ingestion batch cannot make the requested state transition."""


class ProviderErrorCategory(str, Enum):
    AUTHORIZATION = "AUTHORIZATION"
    RATE_LIMIT = "RATE_LIMIT"
    TRANSIENT = "TRANSIENT"
    REQUEST = "REQUEST"
    SCHEMA = "SCHEMA"
    PAGINATION = "PAGINATION"
    RESPONSE_LIMIT = "RESPONSE_LIMIT"
    MISSING_SPOT = "MISSING_SPOT"


class OptionProviderError(RuntimeError):
    def __init__(
        self,
        category: ProviderErrorCategory,
        message: str,
        *,
        status_code: int | None = None,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.category = category
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds

    @property
    def retryable(self) -> bool:
        return self.category in (
            ProviderErrorCategory.RATE_LIMIT,
            ProviderErrorCategory.TRANSIENT,
        )