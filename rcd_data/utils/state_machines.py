"""State machine implementations for lifecycle management in RCD Corp."""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np


@dataclass
class StateMachine:
    """Generic state machine with probabilistic transitions."""

    transitions: dict[str, dict[str, float]]
    terminal_states: set[str]

    def run(
        self,
        start_state: str,
        rng: np.random.Generator,
        max_steps: int = 20,
        crisis_mode: bool = False,
    ) -> str:
        state = start_state
        overrides = self._crisis_overrides() if crisis_mode else {}
        for _ in range(max_steps):
            if state in self.terminal_states:
                return state
            if state not in self.transitions:
                return state
            row = overrides.get(state, self.transitions[state])
            states = list(row.keys())
            probs = np.array(list(row.values()), dtype=float)
            probs /= probs.sum()
            state = str(rng.choice(states, p=probs))
        return state

    def _crisis_overrides(self) -> dict[str, dict[str, float]]:
        return {}


class OrderLifecycle(StateMachine):
    """Order: pending → paid → picked → shipped → delivered | cancelled | returned | refunded."""

    def __init__(self) -> None:
        super().__init__(
            transitions={
                "pending": {"paid": 0.82, "cancelled": 0.18},
                "paid": {"picked": 0.93, "cancelled": 0.07},
                "picked": {"shipped": 0.97, "cancelled": 0.03},
                "shipped": {"delivered": 0.88, "returned": 0.09, "lost": 0.03},
                "returned": {"refunded": 0.90, "returned": 0.10},
            },
            terminal_states={"cancelled", "delivered", "refunded", "lost"},
        )

    def _crisis_overrides(self) -> dict[str, dict[str, float]]:
        return {
            "pending": {"paid": 0.50, "cancelled": 0.50},
            "shipped": {"delivered": 0.60, "returned": 0.30, "lost": 0.10},
        }


class TicketLifecycle(StateMachine):
    """Support ticket: open → in_progress → waiting_customer → resolved → closed | escalated."""

    def __init__(self) -> None:
        super().__init__(
            transitions={
                "open": {"in_progress": 0.75, "closed": 0.10, "escalated": 0.15},
                "in_progress": {"waiting_customer": 0.40, "resolved": 0.50, "escalated": 0.10},
                "waiting_customer": {"in_progress": 0.60, "closed": 0.30, "resolved": 0.10},
                "escalated": {"in_progress": 0.80, "resolved": 0.20},
                "resolved": {"closed": 0.85, "open": 0.15},
            },
            terminal_states={"closed"},
        )

    def _crisis_overrides(self) -> dict[str, dict[str, float]]:
        return {
            "open": {"in_progress": 0.50, "closed": 0.05, "escalated": 0.45},
            "in_progress": {"waiting_customer": 0.20, "resolved": 0.30, "escalated": 0.50},
        }


class LeadPipeline(StateMachine):
    """Sales lead: new → contacted → qualified → proposal → won | lost | nurturing."""

    def __init__(self) -> None:
        super().__init__(
            transitions={
                "new": {"contacted": 0.70, "lost": 0.30},
                "contacted": {"qualified": 0.55, "nurturing": 0.30, "lost": 0.15},
                "qualified": {"proposal": 0.65, "nurturing": 0.20, "lost": 0.15},
                "nurturing": {"contacted": 0.40, "lost": 0.60},
                "proposal": {"won": 0.50, "lost": 0.50},
            },
            terminal_states={"won", "lost"},
        )


class ShipmentTracking(StateMachine):
    """Shipment: created → picked_up → in_transit → out_for_delivery → delivered."""

    def __init__(self) -> None:
        super().__init__(
            transitions={
                "created": {"picked_up": 0.95, "cancelled": 0.05},
                "picked_up": {"in_transit": 0.97, "returned_to_sender": 0.03},
                "in_transit": {"out_for_delivery": 0.90, "delayed": 0.10},
                "delayed": {"in_transit": 0.70, "returned_to_sender": 0.30},
                "out_for_delivery": {"delivered": 0.92, "failed_delivery": 0.08},
                "failed_delivery": {"out_for_delivery": 0.60, "returned_to_sender": 0.40},
            },
            terminal_states={"delivered", "returned_to_sender", "cancelled"},
        )


class RecruitmentFunnel(StateMachine):
    """Recruitment: applied → screening → interview → technical → offer → hired."""

    def __init__(self) -> None:
        super().__init__(
            transitions={
                "applied": {"screening": 0.60, "rejected": 0.40},
                "screening": {"interview": 0.55, "rejected": 0.45},
                "interview": {"technical": 0.60, "rejected": 0.40},
                "technical": {"offer": 0.50, "rejected": 0.50},
                "offer": {"hired": 0.80, "declined": 0.20},
            },
            terminal_states={"hired", "rejected", "declined"},
        )


class ProductionRunStatus(StateMachine):
    """Production run: scheduled → in_progress → quality_check → completed | rework | scrapped."""

    def __init__(self) -> None:
        super().__init__(
            transitions={
                "scheduled": {"in_progress": 0.90, "cancelled": 0.10},
                "in_progress": {"quality_check": 0.85, "halted": 0.15},
                "halted": {"in_progress": 0.70, "cancelled": 0.30},
                "quality_check": {"completed": 0.88, "rework": 0.12},
                "rework": {"quality_check": 0.75, "scrapped": 0.25},
            },
            terminal_states={"completed", "cancelled", "scrapped"},
        )
