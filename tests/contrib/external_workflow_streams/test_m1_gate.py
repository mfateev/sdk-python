"""P16a/P16b — the milestone gates.

The required-test lists are what "the milestone is met" means. This module makes
them enforceable rather than aspirational: each case in the plan is mapped to the
test that covers it, and the mapping is checked against both the plan and the
test suite.

A case that no test covers yet maps to :data:`BLOCKED` together with the
deliverable it waits on. That is deliberately louder than leaving it out --
an unmapped case would pass by omission, while a blocked one is counted and
reported every time the gate runs.
"""

from __future__ import annotations

import pytest

from tests.contrib.external_workflow_streams.m1_gate import (
    declared_count,
    required_cases,
    unresolved,
)

#: Case number -> the test(s) that cover it, or ``BLOCKED``. A case may need
#: more than one test: several of them name two independent properties, and
#: mapping such a case to whichever half happened to be written first would
#: claim coverage the suite does not have.
#: Numbers follow `tests-m1.md` in document order.
M1_COVERAGE: dict[int, str | tuple[str, ...]] = {
    1: "external_streams.rs::a_normal_completion_commits_its_marker",
    2: (
        "external_streams.rs::an_activity_command_writes_its_marker_ordered_before_it",
        "external_streams.rs::replaying_a_marker_before_an_activity_delivers_its_record_exactly_once",
    ),
    3: (
        "external_streams.rs::a_failed_workflow_writes_its_marker_ordered_before_the_failure",
        "external_streams.rs::a_continue_as_new_writes_its_marker_ordered_before_it",
    ),
    4: "external_streams.rs::several_progress_reports_collapse_into_one_marker",
    5: "external_streams.rs::every_completion_path_writes_exactly_one_marker_ending_in_a_terminal",
    6: "test_runtime.py::test_a_first_subscription_to_an_empty_stream_still_emits",
    7: "test_runtime.py::test_an_activation_that_drained_nothing_emits_an_empty_segment",
    8: (
        "external_streams.rs::a_budget_driven_split_writes_two_markers_rather_than_one_oversized_one",
        "test_replay.py::test_two_markers_reassemble_in_workflow_task_order",
    ),
    9: "test_annotation.py::test_a_large_single_stream_batch_encodes_as_one_run",
    10: (
        "test_annotation.py::test_encoded_size_stays_flat_with_sparse_control_records",
        "test_annotation.py::test_encoded_size_grows_only_with_the_number_of_control_records",
    ),
    11: "external_streams.rs::readiness_before_the_idle_timer_expires_cancels_it",
    12: "external_streams.rs::a_confirmed_idle_park_writes_one_marker_and_completes_the_task",
    13: "test_runtime_only_jobs.py::test_a_recheck_that_finds_records_abandons_the_whole_park",
    14: (
        "test_wake.py::test_a_producer_that_finds_a_wakeable_generation_sends_exactly_one_signal",
        "test_runtime.py::test_readiness_after_a_re_block_names_the_current_generation",
    ),
    15: "external_streams.rs::a_stale_generation_produces_no_activation",
    16: "external_streams.rs::an_all_fenced_snapshot_parks_without_waiting_out_the_idle_timeout",
    17: "external_streams.rs::a_core_decided_boundary_asks_for_a_terminal_before_writing_anything",
    18: "external_streams.rs::a_core_decided_boundary_asks_for_a_terminal_before_writing_anything",
    19: "test_rollover_integration.py::test_a_continuously_fed_stream_survives_a_rollover",
    20: "external_streams.rs::the_rollover_deadline_fires_on_a_workflow_only_worker",
    21: "test_rollover_integration.py::test_a_signal_into_a_retained_task_lands_by_the_rollover_deadline",
    22: (
        "test_worker_integration.py::test_an_append_with_no_open_task_wakes_the_workflow",
        "test_api.py::test_a_record_buffered_while_not_iterating_is_still_delivered",
    ),
    23: "test_rollover_integration.py::test_an_append_after_a_rollover_completion_wakes_the_subscription",
    24: "test_manager.py::test_undeliverable_readiness_owes_a_wake_and_keeps_the_right_watchers",
    25: "external_streams.rs::an_unknown_envelope_version_is_ignored_harmlessly",
    26: (
        "test_wake.py::test_two_producers_retrying_one_wake_derive_the_identical_request_id",
        "test_wake.py::test_two_producers_retrying_one_wake_are_deduplicated_by_the_server",
    ),
    27: (
        "test_wake.py::test_two_unparked_wakes_from_different_senders_differ",
        "test_wake.py::test_two_workers_unparked_wakes_are_both_delivered",
        "test_wake.py::test_one_senders_retry_stays_a_single_wake",
    ),
    28: "external_streams.rs::a_second_worker_reconstructs_the_subscription_from_the_shutdown_marker",
    29: "test_worker_handoff.py::test_shutdown_with_no_open_task_hands_the_run_to_another_worker",
    30: "test_worker_handoff.py::test_a_finalization_that_cannot_be_answered_writes_no_marker",
    31: "external_streams.rs::an_unwritten_annotation_exists_only_while_a_workflow_task_is_open",
    32: (
        "test_shutdown_sweep.py::test_a_momentarily_failing_wake_is_retried_and_succeeds",
        "test_shutdown_sweep.py::test_the_retry_is_bounded_and_then_reported",
        "test_shutdown_sweep.py::test_the_retry_is_the_same_wake_not_a_second_one",
        "test_shutdown_sweep.py::test_an_unacknowledged_wake_surfaces_on_the_metric",
    ),
    33: "test_runtime_only_jobs.py::test_finalization_touches_no_provider_at_all",
    34: "test_replay.py::test_replay_performs_no_live_waiting",
    35: "test_replay_end_to_end.py::test_replaying_a_stream_history_reproduces_the_same_observations",
    36: (
        "test_replay_end_to_end.py::test_an_empty_stream_parked_and_evicted_replays_from_the_recorded_cursor",
        "test_replay_end_to_end.py::test_an_empty_stream_replays_from_its_recorded_boundary",
    ),
    37: "test_replay.py::test_an_unreachable_backend_fails_as_transient_storage",
    38: "test_replay.py::test_each_deletion_position_fails_a_different_check",
    39: "test_replay.py::test_an_intact_but_undecodable_record_is_a_decode_error",
    40: "test_replay.py::test_a_marker_naming_an_unknown_wait_is_nondeterminism_not_integrity_loss",
    41: "test_worker_crash.py::test_a_crash_before_the_marker_makes_the_next_worker_re_read",
    42: "test_continuation.py::test_a_first_execution_starts_at_the_beginning",
    43: "external_streams.rs::a_pending_timer_suppresses_retention_but_its_subscriptions_survive_it",
    44: "test_backend_conformance.py::test_a_broken_backend_fails_for_the_right_reason",
    45: "test_backend_conformance.py::test_a_backend_needing_a_nameable_cursor_fails_the_tail_check",
    46: "test_redis_backend.py::test_offsets_compare_numerically_not_lexically",
    47: "test_producer.py::test_republishing_different_content_under_one_key_is_an_error",
    48: "test_manager.py::test_a_backend_slower_than_the_deadlock_timeout_delays_readiness",
    49: "test_runtime_only_jobs.py::test_a_park_slower_than_the_deadlock_timeout_is_still_answered",
    50: "test_manager.py::test_the_provider_is_never_called_from_the_workflow_thread",
    51: "test_manager.py::test_a_full_buffer_stops_prefetch_without_dropping_or_blocking",
    52: "test_manager.py::test_an_evicted_run_re_delivers_the_records_it_had_already_seen",
    53: "test_shutdown_sweep.py::test_teardown_removes_every_run",
    54: "test_registry.py::test_a_backend_without_the_guarantee_fails_worker_construction",
    55: (
        "test_redis_backend.py::test_a_trimmed_range_is_integrity_loss_not_a_storage_failure",
        "test_redis_backend.py::test_a_deleted_write_fence_is_integrity_loss",
    ),
}

#: Case number -> what is still missing, for cases the suite does not fully
#: cover. A case is in exactly one of the two maps: claiming a partially covered
#: case as covered is how a gate stops meaning anything.
M1_GAPS: dict[int, str] = {}

#: The same, for `tests-m2.md`.
M2_COVERAGE: dict[int, str | tuple[str, ...]] = {
    1: "test_m2_required.py::test_readiness_on_one_of_several_streams_resets_global_quiescence",
    2: "test_multi_stream.py::test_one_idle_stream_cannot_park_while_another_is_active",
    3: "test_multi_stream.py::test_a_fence_on_one_stream_alone_does_not_make_the_set_parkable",
    4: "test_multi_stream.py::test_all_fenced_streams_make_the_whole_set_parkable",
    5: (
        "test_multi_stream.py::test_an_alternating_two_stream_batch_encodes_one_run_per_delivery",
        "test_m2_required.py::test_an_alternating_two_stream_batch_rolls_over_within_budget",
    ),
    6: "test_m2_required.py::test_simultaneously_ready_streams_are_drained_in_one_pass",
    7: "test_runtime.py::test_alternating_streams_produce_one_run_per_delivery",
    8: (
        "test_multi_stream.py::test_each_same_stream_subscription_receives_every_record",
        "test_continuation.py::test_two_same_stream_subscriptions_restore_independently",
    ),
    9: (
        "test_multi_stream.py::test_two_same_stream_subscriptions_install_distinct_park_intents",
        "test_m2_required.py::test_two_same_stream_subscriptions_never_overwrite_each_other",
    ),
    10: (
        "test_multi_stream.py::test_differing_idle_timeouts_reduce_to_the_minimum",
        "test_m2_required.py::test_the_idle_timeout_reduction_reproduces_exactly",
    ),
    11: (
        "test_m2_required.py::test_a_wake_for_one_stream_resolves_every_blocked_wait",
        "test_m2_required.py::test_resolving_twice_is_harmless",
    ),
    12: "test_continuation.py::test_a_chain_resumes_where_its_predecessor_stopped",
}

#: The same, for `tests-m2.md`, which has none.
M2_GAPS: dict[int, str] = {}


def _coverage(list_name: str) -> dict[int, str | tuple[str, ...]]:
    return M1_COVERAGE if list_name == "tests-m1.md" else M2_COVERAGE


def _gaps(list_name: str) -> dict[int, str]:
    return M1_GAPS if list_name == "tests-m1.md" else M2_GAPS


def _node_ids(coverage: dict[int, str | tuple[str, ...]]) -> list[str]:
    """Flattens the map, since a case may name more than one test."""
    flat: list[str] = []
    for value in coverage.values():
        flat.extend([value] if isinstance(value, str) else list(value))
    return flat


@pytest.mark.parametrize("list_name", ["tests-m1.md", "tests-m2.md"])
def test_the_parsed_case_count_matches_the_plans_own_heading(list_name: str) -> None:
    """Catches a case added to the plan without the heading being updated.

    The two numbers are maintained by hand in the same document, so they are
    exactly the kind of pair that drifts. If they disagree, every count below is
    measuring the wrong thing.
    """
    cases = required_cases(list_name)

    assert len(cases) == declared_count(list_name), (
        f"{list_name} declares {declared_count(list_name)} cases but contains "
        f"{len(cases)}"
    )


@pytest.mark.parametrize("list_name", ["tests-m1.md", "tests-m2.md"])
def test_every_required_case_is_accounted_for(list_name: str) -> None:
    """A new case must fail the gate rather than pass by not being mentioned."""
    cases = required_cases(list_name)
    coverage = _coverage(list_name)

    gaps = _gaps(list_name)
    unmapped = [c for c in cases if c.number not in coverage and c.number not in gaps]

    assert not unmapped, (
        "these required cases appear in neither map -- add the test that covers "
        "each, or record what is still missing in the gaps map:\n"
        + "\n".join(f"  {c.number}. {c.text}" for c in unmapped)
    )


@pytest.mark.parametrize("list_name", ["tests-m1.md", "tests-m2.md"])
def test_every_mapped_test_exists(list_name: str) -> None:
    """Catches a rename or deletion that would leave the gate pointing at nothing.

    This is the failure the whole mechanism exists for: without it the map keeps
    claiming coverage that was deleted, and the milestone stays "met" on paper.
    """
    coverage = _coverage(list_name)

    missing = unresolved(_node_ids(coverage))

    assert not missing, "the coverage map names tests that do not exist:\n" + "\n".join(
        f"  {node_id}" for node_id in missing
    )


@pytest.mark.parametrize("list_name", ["tests-m1.md", "tests-m2.md"])
def test_no_case_is_both_covered_and_open(list_name: str) -> None:
    """A case in both maps would be counted twice and read as covered.

    Claiming a partially covered case as covered is how a gate stops meaning
    anything, so the two maps must partition the list rather than overlap.
    """
    overlap = sorted(set(_coverage(list_name)) & set(_gaps(list_name)))

    assert not overlap, f"cases claimed as both covered and open: {overlap}"


@pytest.mark.parametrize("list_name", ["tests-m1.md", "tests-m2.md"])
def test_every_open_case_says_what_is_missing(list_name: str) -> None:
    """An empty reason is indistinguishable from "nobody looked"."""
    empty = sorted(n for n, reason in _gaps(list_name).items() if not reason.strip())

    assert not empty, f"open cases with no stated gap: {empty}"


def test_the_gate_reports_how_far_each_milestone_is_from_met() -> None:
    """The gate's actual output, and the thing a human reads.

    Not an assertion that a milestone *is* met -- Milestone 1 is not, and a test
    that failed for that reason would be a permanent red mark saying nothing new
    each run. What must not happen is losing track of which cases remain, which
    the two maps above make impossible to do quietly.
    """
    for list_name in ("tests-m1.md", "tests-m2.md"):
        cases = {c.number: c.text for c in required_cases(list_name)}
        gaps = _gaps(list_name)
        covered = len(cases) - len(gaps)
        blocked = {n for n, why in gaps.items() if why.startswith("BLOCKED")}
        print(
            f"\n{list_name}: {covered}/{len(cases)} covered, {len(gaps)} open "
            f"({len(blocked)} of them blocked on a deliverable)"
        )
        for number in sorted(gaps):
            print(f"  {number}. {cases[number][:70]}")
            print(f"     {gaps[number]}")
