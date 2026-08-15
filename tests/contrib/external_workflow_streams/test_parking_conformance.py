"""P2b — the parking conformance suite, and proof that it fails a broken stub."""

from __future__ import annotations

import uuid
from datetime import timedelta

import pytest

from temporalio.contrib.external_workflow_streams._backend import (
    ParkIntent,
    StreamBackend,
    StreamKey,
)
from temporalio.contrib.external_workflow_streams._record import BEGINNING
from tests.contrib.external_workflow_streams import conformance
from tests.contrib.external_workflow_streams.conformance import (
    PARKING_CONFORMANCE_CHECKS,
    Check,
)
from tests.contrib.external_workflow_streams.memory_backend import MemoryStreamBackend


@pytest.fixture
def stream_key() -> StreamKey:
    return StreamKey("ns", "wf", uuid.uuid4().hex, "tokens")


@pytest.fixture
def backend() -> MemoryStreamBackend:
    return MemoryStreamBackend()


# --- the reference backend conforms -----------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("check", PARKING_CONFORMANCE_CHECKS, ids=lambda c: c.__name__)
async def test_reference_backend_conforms(
    check: Check, backend: MemoryStreamBackend, stream_key: StreamKey
) -> None:
    await check(backend, stream_key)


def test_the_parking_suite_is_not_silently_empty() -> None:
    assert len(PARKING_CONFORMANCE_CHECKS) >= 10
    assert len({c.__name__ for c in PARKING_CONFORMANCE_CHECKS}) == len(
        PARKING_CONFORMANCE_CHECKS
    )


class ObserveOnlyBackend(MemoryStreamBackend):
    """Cannot lease, and says so. Every producer signals idempotently."""

    supports_leased_claims = False

    async def claim_park_generation(
        self,
        key: StreamKey,
        wait_id: int,
        park_generation: int,
        *,
        claimant: str,
        lease: timedelta,
    ) -> bool:
        return True


@pytest.mark.asyncio
@pytest.mark.parametrize("check", PARKING_CONFORMANCE_CHECKS, ids=lambda c: c.__name__)
async def test_an_observe_only_backend_conforms(
    check: Check, stream_key: StreamKey
) -> None:
    """The suite must accept the fallback it explicitly permits.

    A provider that cannot lease is not broken -- it is a different, weaker
    contract the design allows for, and a suite that rejected it would push
    implementers toward an unleased claim, which *is* broken.
    """
    await check(ObserveOnlyBackend(), stream_key)


# --- deliberately broken backends -------------------------------------------


class StreamKeyedIntentBackend(MemoryStreamBackend):
    """Keys park intents by stream alone, ignoring the wait id.

    The realistic mistake: thinking of a park as a property of the stream rather
    than of the subscription.
    """

    async def install_park_intent(self, key: StreamKey, intent: ParkIntent) -> None:
        self._intents[(key, 0)] = intent

    async def park_intent(self, key: StreamKey, wait_id: int) -> ParkIntent | None:
        return self._intents.get((key, 0))


class NonExpiringClaimBackend(MemoryStreamBackend):
    """Claims are exclusive forever. Nothing recovers a crashed producer's."""

    async def claim_park_generation(
        self,
        key: StreamKey,
        wait_id: int,
        park_generation: int,
        *,
        claimant: str,
        lease: timedelta,
    ) -> bool:
        held = self._claims.get((key, wait_id))
        if held is not None and held[0] != claimant and held[1] == park_generation:
            return False
        self._claims[(key, wait_id)] = (claimant, park_generation, float("inf"))
        return True


class BlindRecheckBackend(MemoryStreamBackend):
    """Rechecks against the stream's state when the intent was installed."""

    async def recheck(self, key: StreamKey, wait_id: int) -> bool:
        return False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("broken", "check", "reason"),
    [
        pytest.param(
            StreamKeyedIntentBackend,
            conformance.check_intents_are_keyed_by_stream_and_wait_id,
            r"keyed \(stream key, wait_id\)",
            id="intents-keyed-by-stream-alone",
        ),
        pytest.param(
            NonExpiringClaimBackend,
            conformance.check_an_expired_claim_is_taken_over,
            "expired claim was not taken over",
            id="claim-never-expires",
        ),
        pytest.param(
            BlindRecheckBackend,
            conformance.check_recheck_sees_an_append_past_the_cursor,
            "append/park race is not closed",
            id="recheck-cannot-see-a-late-append",
        ),
    ],
)
async def test_a_broken_backend_fails_for_the_right_reason(
    broken: type[StreamBackend], check: Check, reason: str, stream_key: StreamKey
) -> None:
    with pytest.raises(AssertionError, match=reason):
        await check(broken(), stream_key)


# --- the intent value type --------------------------------------------------


def test_a_park_generation_of_zero_is_refused() -> None:
    """0 is the reserved unparked-wake sentinel and can never be a real park."""
    with pytest.raises(ValueError, match="starts at 1"):
        ParkIntent(1, BEGINNING, park_generation=0, run_id="run-a")


@pytest.mark.asyncio
async def test_two_same_stream_subscriptions_are_visible_separately(
    backend: MemoryStreamBackend, stream_key: StreamKey
) -> None:
    """The end state P21 asserts, checked here at the provider level.

    Inspecting both in the backend is what distinguishes "two subscriptions
    parked independently" from "one overwrote the other and happens to look
    parked".
    """
    for wait_id in (1, 2):
        await backend.install_park_intent(
            stream_key,
            ParkIntent(wait_id, BEGINNING, park_generation=5, run_id="run-a"),
        )

    assert await backend.current_park_generation(stream_key, 1) == 5
    assert await backend.current_park_generation(stream_key, 2) == 5
    assert (await backend.park_intent(stream_key, 1)) != (
        await backend.park_intent(stream_key, 2)
    )
