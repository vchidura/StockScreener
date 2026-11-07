"""G102 design 5.2 (G91): a list change prepares only what changed.

On Nov 6 each list change re-ran all five Equity preparation stages for every new member (about 450 provider requests
and 1.5 hours). A member an earlier revision of the same source cohort and effective session finished on identical
targets is READY at once with no work; a member it only started resumes from the earlier progress."""
from datetime import date, datetime, timezone
from unittest.mock import MagicMock
from uuid import NAMESPACE_URL, UUID, uuid5

from realtime import advanced_realtime_desired_state_repository as repository
from realtime.advanced_realtime_desired_state import (
    DesiredMemberIntent,
    DesiredMemberKind,
    DesiredStateChange,
    build_desired_state_revision,
)

UTC = timezone.utc
SOURCE_COHORT = UUID(int=41)
EARLIER = str(UUID(int=42))
OBSERVED = datetime(2026, 11, 6, 15, 0, tzinfo=UTC)
TICKERS = ("AMD", "CRWD", "PLTR", "SPY")
SECURITIES = {ticker: uuid5(NAMESPACE_URL, f"g91:{ticker}") for ticker in TICKERS}


def revision():
    change = DesiredStateChange(
        source_cohort_id=SOURCE_COHORT,
        members=tuple(DesiredMemberIntent(ticker, DesiredMemberKind.EQUITY) for ticker in TICKERS),
    )
    return build_desired_state_revision(
        change, effective_session_date=date(2026, 11, 9), recorded_at=OBSERVED, security_ids=SECURITIES,
        selection_policy_reference="stored-equity-policy", selection_policy_sha256="c" * 64,
    )


def earlier_work(current, statuses):
    """rt.realtime_equity_preparation_work rows of the earlier revision: ticker -> per-stage status (or a changed
    target, marked 'OTHER_TARGET')."""
    targets = {stage: target for stage, _order, target in repository._equity_preparation_targets(current)}
    rows = []
    for ticker, by_stage in statuses.items():
        for stage, target in targets.items():
            status = by_stage.get(stage, "COMPLETE")
            rows.append((EARLIER, str(SECURITIES[ticker]), "EQUITY", stage,
                         "COMPLETE" if status == "OTHER_TARGET" else status,
                         dict(target, effective_session_date="2026-11-06") if status == "OTHER_TARGET" else target))
    return rows


def test_only_a_member_finished_on_identical_targets_is_carried():
    current = revision()
    cursor = MagicMock()
    cursor.fetchall.return_value = earlier_work(current, {
        "AMD": {},  # all five COMPLETE on identical targets
        "CRWD": {"TECHNICAL_WARMUP": "RETRY"},  # started, not finished
        "PLTR": {"REFERENCE": "OTHER_TARGET"},  # finished for another target
    })
    carried = repository._carried_equity_preparations_cursor(
        cursor, current, security_ids=SECURITIES, source_base={SECURITIES["SPY"]},
    )
    assert carried == {(SECURITIES["AMD"], "EQUITY"): EARLIER}
    sql, params = cursor.execute.call_args.args
    assert "earlier.effective_session_date=%s" in sql and "source_cohort_id" not in sql  # G102 A4: any cohort
    assert params[1:] == (str(current.revision_id), date(2026, 11, 9))
    assert params[0] == sorted(str(SECURITIES[ticker]) for ticker in ("AMD", "CRWD", "PLTR"))


def test_nothing_is_looked_up_when_every_member_is_in_the_source_cohort():
    cursor = MagicMock()
    assert repository._carried_equity_preparations_cursor(
        cursor, revision(), security_ids=SECURITIES, source_base=set(SECURITIES.values()),
    ) == {}
    cursor.execute.assert_not_called()


def test_a_carried_member_is_ready_without_work_and_the_others_resume(monkeypatch):
    current = revision()
    batches = []
    monkeypatch.setattr(repository, "execute_values",
                        lambda _cursor, statement, values, page_size: batches.append((statement, tuple(values))))
    cursor = MagicMock()
    cursor.fetchall.side_effect = [
        [(str(SECURITIES[ticker]), str(uuid5(NAMESPACE_URL, f"g91-revision:{ticker}"))) for ticker in TICKERS],
        earlier_work(current, {"AMD": {}, "CRWD": {"TECHNICAL_WARMUP": "RETRY"}}),
    ]
    repository._persist_desired_members_cursor(
        cursor, current, security_ids=SECURITIES,
        instrument_ids={ticker: index + 1 for index, ticker in enumerate(TICKERS)},
        source_base={SECURITIES["SPY"]}, source_underlyings=set(), ready_option_securities=set(),
        checked_at=OBSERVED,
    )
    by_table = {statement.split("(")[0].split("INTO ")[1].strip(): values for statement, values in batches}
    intents = {row[2]: row[0] for row in by_table["rt.realtime_desired_member_intents"]}
    readiness = {row[0]: row[1] for row in by_table["rt.realtime_desired_intent_readiness"]}
    assert {ticker: readiness[intents[ticker]] for ticker in TICKERS} == {
        "AMD": "READY", "CRWD": "WARMING", "PLTR": "WARMING", "SPY": "READY"}
    assert all(row[2] == [] for row in by_table["rt.realtime_desired_intent_readiness"])
    lineage = {row[2]: row[6].adapted for row in by_table["rt.realtime_desired_member_bindings"]}
    assert lineage[str(SECURITIES["AMD"])] == {"source": "STORED_REFERENCE", "carried_from": EARLIER}
    assert lineage[str(SECURITIES["CRWD"])] == {"source": "STORED_REFERENCE"}
    work = by_table["rt.realtime_equity_preparation_work"]
    assert sorted({row[5] for row in work}) == ["CRWD", "PLTR"] and len(work) == 10
    resume_sql, resume_params = cursor.execute.call_args.args
    assert resume_sql.startswith("UPDATE rt.realtime_equity_preparation_work AS target")
    assert "target.status='PENDING'" in resume_sql and "target.target=source.target" in resume_sql
    assert "terminal_reason=source.terminal_reason" in resume_sql
    assert resume_params[2] == sorted(str(SECURITIES[ticker]) for ticker in ("CRWD", "PLTR"))
    assert "source_cohort_id" not in resume_sql
