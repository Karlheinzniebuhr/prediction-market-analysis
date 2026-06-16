"""Adaptive block-span sizing for Polymarket trades backfill."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class AdaptiveChunkSizer:
    """Pick block spans that keep peak contract logs near a target budget.

    Uses an exponential moving average of observed logs/block density so sparse
    historical eras get large chunks (fewer RPC calls) while dense chain-head
    periods shrink automatically. Updates happen only on in-order commits.
    """

    target_logs: int = 8500
    min_blocks: int = 10
    max_blocks: int = 400
    initial_blocks: int = 100
    ema_alpha: float = 0.25

    def __post_init__(self) -> None:
        self._blocks = max(self.min_blocks, min(self.max_blocks, self.initial_blocks))
        self._density: float | None = None

    @property
    def current_blocks(self) -> int:
        return self._blocks

    def plan(self, start: int, to_block: int) -> tuple[int, int]:
        """Return the next chunk range starting at ``start``."""
        span = max(1, min(self._blocks, to_block - start + 1))
        return start, start + span - 1

    def observe(self, blocks: int, peak_logs: int) -> None:
        """Record a committed chunk and retune the next span."""
        if blocks <= 0:
            return

        sample_density = peak_logs / blocks
        if self._density is None:
            self._density = sample_density
        else:
            self._density = self.ema_alpha * sample_density + (1.0 - self.ema_alpha) * self._density

        if self._density <= 0:
            self._blocks = min(self.max_blocks, self._blocks * 2)
            return

        ideal = int(self.target_logs / self._density)
        self._blocks = max(self.min_blocks, min(self.max_blocks, ideal))
