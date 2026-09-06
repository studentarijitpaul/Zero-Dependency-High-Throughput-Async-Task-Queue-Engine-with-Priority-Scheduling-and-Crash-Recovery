from __future__ import annotations

import random
from dataclasses import dataclass
from typing import TypeAlias

ExceptionTypes: TypeAlias = tuple[type[Exception], ...]


@dataclass(slots=True, frozen=True)
class RetryPolicy:
    """Defines how task failures are retried."""

    base_delay: float = 1.0
    max_delay: float = 30.0
    retryable_exceptions: ExceptionTypes = (Exception,)
    jitter: bool = False

    def __post_init__(self) -> None:
        """Validate retry policy configuration."""
        if self.base_delay < 0:
            raise ValueError("base_delay must be >= 0")

        if self.max_delay < 0:
            raise ValueError("max_delay must be >= 0")

        if self.base_delay > self.max_delay:
            raise ValueError("base_delay must be <= max_delay")

    def should_retry(
        self,
        exception: Exception,
    ) -> bool:
        """Return whether an exception is retryable."""
        return isinstance(
            exception,
            self.retryable_exceptions,
        )

    def get_delay(self, retry_number: int) -> float:
        """Calculate the delay before a retry."""
        if retry_number < 1:
            raise ValueError("retry_number must be >= 1")

        calculated_delay: float = min(
            self.max_delay,
            self.base_delay * (2 ** (retry_number - 1)),
        )

        if self.jitter:
            jitter: float = random.random()
            return calculated_delay * jitter

        return calculated_delay
