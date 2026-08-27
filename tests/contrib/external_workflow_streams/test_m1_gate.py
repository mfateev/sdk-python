"""P16a/P16b — the milestone gates.

The required-test lists are what "the milestone is met" means. This module makes
them enforceable rather than aspirational: each case in the plan is mapped to the
test that covers it, and the mapping is checked against both the plan and the
test suite.

A case that no test fully covers yet maps to its concrete missing boundary in
the corresponding gaps table. That is deliberately louder than leaving it out:
an unmapped case would pass by omission, while an open one is counted and
reported every time the gate runs.
"""

from __future__ import annotations

import pytest

from tests.contrib.external_workflow_streams.m1_gate import (
    declared_count,
    required_cases,
    unresolved,
)

#: Case number -> the test(s) that fully cover it. A case may need
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
    # Cases 56-62 -- "Boundaries an implementation review found unprotected".
    # These are mapped here before the vendored copy of the list carries them:
    # the gate parses the plan out of `temporalio/bridge/sdk-core`, so it counts
    # 55 cases until the submodule pointer moves to the commit that adds them.
    # Mapping ahead of that is deliberate -- an unmapped case is what the gate is
    # designed to fail on, so the map has to arrive no later than the list does.
    56: "test_replay.py::test_the_first_live_drain_after_a_replay_needs_no_loop_turn",
    57: "test_replay.py::test_a_marker_drains_once_per_segment_including_the_activations_own",
    58: "test_runtime.py::test_the_segment_that_crosses_the_mark_asks_for_rollover_itself",
    59: (
        "test_runtime.py::test_a_frame_larger_than_the_slack_rolls_over_instead_of_raising",
        "test_runtime.py::test_a_subscription_set_too_large_to_record_is_refused_at_subscribe",
    ),
    60: (
        "test_wake.py::test_a_coordination_failure_after_the_append_is_still_unacknowledged",
        "test_wake.py::test_a_coordination_failure_after_a_fence_is_still_unacknowledged",
    ),
    61: (
        "test_shutdown_sweep.py::test_a_grace_period_expiry_counts_every_wake_it_abandons",
        "test_shutdown_sweep.py::test_a_probe_that_cannot_answer_is_not_reported_as_nothing_owed",
        "test_shutdown_sweep.py::test_a_hanging_probe_counts_the_runs_it_never_answered_for",
        "test_shutdown_sweep.py::test_a_run_with_nothing_owed_is_not_counted_as_a_failure",
    ),
    62: "test_replay.py::test_an_unreachable_payload_store_is_a_storage_failure_not_a_decode_one",
    # 63 and 64 came out of reviewing the fixes for 56-62 rather than out of the
    # review itself: the first fix for the byte budget stopped delivery without
    # obliging the same completion to ask for the rollover, and the first fix for
    # the sweep counted subscriptions on a manager that had no sweep to perform.
    63: "test_runtime.py::test_an_activation_the_annotation_budget_stopped_is_not_wedged_by_it",
    64: "test_shutdown_sweep.py::test_a_manager_with_no_probe_owes_nothing_and_reports_nothing",
    65: "test_replay.py::test_a_marker_with_no_segments_closes_before_the_activations_own_drain",
    # 66-69 came out of an independent review of the fixes for 56-62, which found
    # four more defects in this round's own code. The shape is the one worth
    # remembering: each was correct for the case it was aimed at.
    66: "test_runtime.py::test_the_first_record_of_an_activation_is_priced_from_a_measurement",
    67: "test_runtime.py::test_a_run_too_large_for_any_annotation_says_so_and_fails_the_workflow",
    68: "test_runtime.py::test_the_capacity_floor_covers_everything_an_empty_annotation_carries",
    69: (
        "test_replay.py::test_a_failing_activation_reports_its_own_error_not_the_replays",
        "test_shutdown_sweep.py::test_a_wake_the_live_path_delivered_is_not_counted_as_abandoned",
    ),
    # 70-71 pin both halves of replay-to-live repositioning: a read already in
    # flight cannot refill a retracted buffer, and replay delivers no record the
    # marker omitted.
    70: "test_replay.py::test_a_read_in_flight_across_a_reposition_cannot_be_appended",
    71: "test_replay.py::test_a_replay_activation_never_delivers_a_record_the_marker_omits",
    # 72-76 are the fourth review's five findings. Four of them are the same shape
    # as the third round's: a guarantee written for the path it was aimed at and
    # not for the adjacent one -- a budget spent where it was not reserved, a key
    # drawn after an await that reorders it, a handler for `Exception` on a path
    # cancellation also takes, and a retry whose answer was discarded.
    72: (
        "test_delivery_budget.py::test_two_independent_consumers_share_one_budget",
        "test_delivery_budget.py::test_a_carried_over_ready_list_is_charged_to_the_next_activation",
    ),
    73: (
        "test_producer.py::test_concurrent_publishes_take_their_sequence_in_invocation_order",
        "test_producer.py::test_reordered_encodes_cannot_duplicate_across_two_topics",
    ),
    74: (
        "test_wake.py::test_cancellation_after_the_append_is_an_unacknowledged_wake",
        "test_wake.py::test_cancellation_after_a_fence_is_an_unacknowledged_wake",
        "test_wake.py::test_cancellation_before_the_append_stays_a_cancellation",
    ),
    75: (
        "test_api.py::test_a_second_coroutine_waiting_on_one_subscription_is_refused",
        "test_api.py::test_iterating_again_after_the_first_consumer_stopped_is_allowed",
        "test_api.py::test_a_merge_cannot_take_a_wait_another_consumer_is_blocked_on",
    ),
    76: "test_manager.py::test_a_stale_retry_that_finds_the_run_gone_tears_the_watcher_down",
    # 77 came out of reviewing case 74's fix, and is the same shape once more: a
    # recovery written for the boundary it was aimed at -- cancellation after
    # `append()` returned -- and not for the one just before it, where a remote
    # backend has already committed and the answer is what was lost.
    77: (
        "test_wake.py::test_cancellation_after_backend_commit_before_append_ack_is_recoverable",
        "test_wake.py::test_a_lost_append_response_is_recoverable_the_same_way",
        "test_wake.py::test_an_unresolved_append_refuses_the_publish_that_would_duplicate_it",
        "test_wake.py::test_settling_an_unacknowledged_append_wakes_exactly_once",
        "test_wake.py::test_an_unacknowledged_fence_settles_to_exactly_one_fence",
        "test_wake.py::test_settling_an_append_that_never_landed_appends_it_once",
        "test_wake.py::test_a_conflicting_append_is_a_refusal_not_an_unknown_outcome",
    ),
    # 78 is case 77's own review. The outcome was right and its recovery was
    # under-bound: it carried the record but not the operation, and validated
    # neither the stream the record belongs to nor the producer instance that
    # still holds the session's counters.
    78: (
        "test_wake.py::test_a_refused_append_preserves_the_unresolved_operations_recovery",
        "test_wake.py::test_an_unknown_append_can_only_be_resolved_on_its_originating_topic",
        "test_wake.py::test_recovery_is_bound_to_the_producer_instance_that_made_the_append",
    ),
    # 79 follows the same operation through a second unknown outcome. The
    # recovery attempt's effective wake and lease supersede the original
    # publish's, while cancellation delivered on either attempt remains owed.
    79: "test_wake.py::test_a_reinterrupted_append_recovery_preserves_its_latest_wake",
    # 80-101 promote Workflow-originated output. Only cases with every named
    # boundary asserted belong here; partial unit coverage stays in M1_GAPS.
    80: "test_output_worker_integration.py::test_reader_waits_at_pending_barrier_until_marker_commit",
    81: "test_output_worker_integration.py::test_rejected_post_stage_completion_never_exposes_phantom_output",
    82: "external_streams.rs::speculative_output_redelivery_accepts_a_fresh_token_once",
    83: "test_output_client.py::test_first_task_boundary_without_token_aborts_pending_stage",
    84: "external_streams.rs::exact_output_floor_excludes_the_previous_workflow_task_close",
    85: (
        "test_worker_crash.py::test_disconnected_update_leaves_live_staged_output_undecided_until_timeout",
        "test_output_client.py::test_disconnected_speculative_update_stays_pending_until_history_loss",
    ),
    86: "test_worker_crash.py::test_real_crash_after_output_stage_aborts_old_token_and_exposes_one_retry",
    87: "test_output_worker_integration.py::test_cold_client_repairs_post_report_commit_failure",
    88: (
        "test_redis_output.py::test_exact_stage_retry_returns_actual_status_after_terminal",
        "test_output_client.py::test_repeating_the_same_history_reconciliation_is_idempotent",
    ),
    89: "test_redis_output.py::test_reusing_stage_identity_with_another_manifest_conflicts_atomically",
    90: "test_output_runtime.py::test_output_replay_performs_no_io_token_mint_or_live_policy_split",
    91: "test_output_runtime.py::test_replay_keeps_recorded_segments_when_codec_wire_size_changes",
    92: "test_output_worker_integration.py::test_output_latency_flushes_a_retained_workflow_task",
    93: "test_output_worker_integration.py::test_three_retained_output_windows_write_three_markers_and_wfts",
    94: "test_redis_output.py::test_pending_stage_blocks_later_direct_output_until_commit",
    95: "test_output_producer.py::test_ambiguous_append_retries_exact_record_without_duplicate",
    96: "test_output_runtime.py::test_changed_codec_bytes_keep_logical_retry_identity_and_first_bytes",
    97: "test_output_codec.py::test_metadata_insertion_order_does_not_change_frame_or_fingerprint",
    98: "test_output_worker_integration.py::test_concurrent_updates_preserve_their_own_turn_ids",
    99: "test_output_worker_integration.py::test_stage_outage_blocks_wft_and_reports_external_storage_cause",
    100: (
        "test_redis_output.py::test_missing_staged_record_is_an_integrity_failure",
        "test_output_client.py::test_history_loss_is_integrity_but_an_outage_is_storage",
    ),
    101: (
        "test_output_runtime.py::test_oversized_marker_manifest_is_rejected_before_external_io",
        "test_output_runtime.py::test_one_oversized_logical_record_is_refused_without_poisoning_batch",
    ),
    # 102 restores the public default to the required contract after the design
    # documents were split into guide, specification, and rationale sets.
    102: "test_api.py::test_the_default_idle_timeout_is_one_second",
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
    13: "test_replay_end_to_end.py::test_live_and_replay_share_one_input_output_drain_schedule",
    14: "test_output_worker_integration.py::test_cursor_resume_crosses_rollover_and_continue_as_new",
    15: "test_redis_output.py::test_input_and_output_with_same_user_identity_are_physically_isolated",
    16: (
        "test_output_worker_integration.py::test_output_deadline_wins_park_race_and_removes_every_intent",
        "external_streams.rs::confirmed_park_flushes_output_in_its_marker_without_forcing_a_task",
        "external_streams.rs::output_deadline_invalidates_an_issued_park_and_flushes_once",
    ),
    17: "test_output_worker_integration.py::test_finished_output_survives_continue_as_new_without_backend_reads",
}

#: The same, for `tests-m2.md`.
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

    Coverage and open cases are reported independently of the consistency
    assertions above. If a future requirement is added before its test, its gap
    remains visible without turning this reporting test into the failure itself.
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
