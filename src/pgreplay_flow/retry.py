from __future__ import annotations

import random
from dataclasses import dataclass, field


@dataclass(frozen=True)
class RetryPolicy:
    """Step retry policy. Backoff is durable: the run suspends between attempts
    and any worker may pick up the next one."""

    max_attempts: int = 3
    initial_delay: float = 1.0
    backoff: float = 2.0
    max_delay: float = 300.0
    jitter: float = 0.1  # +/- fraction of the delay
    non_retryable: tuple[type[BaseException], ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

    def should_retry(self, attempts: int, exc: BaseException) -> bool:
        if isinstance(exc, self.non_retryable):
            return False
        return attempts < self.max_attempts

    def delay(self, attempts: int) -> float:
        """Delay before attempt number `attempts + 1`."""
        d = min(self.initial_delay * self.backoff ** (attempts - 1), self.max_delay)
        if self.jitter:
            d *= 1 + random.uniform(-self.jitter, self.jitter)
        return max(d, 0.0)


NO_RETRY = RetryPolicy(max_attempts=1)
