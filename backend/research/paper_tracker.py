"""Bounded prospective research observations; no orders or production writes."""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path

import exchange_calendars
import numpy as np
import pandas as pd


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def utc(value):
    parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("paper clocks must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def plan_cohorts(enrolled_at, count=6):
    enrolled_at = utc(enrolled_at)
    calendar = exchange_calendars.get_calendar("XNYS")
    decision = calendar.date_to_session(pd.Timestamp(enrolled_at.date()), direction="next")
    if calendar.session_close(decision).to_pydatetime() <= enrolled_at:
        decision = calendar.next_session(decision)
    rows = []
    for ordinal in range(count):
        session = calendar.session_offset(decision, ordinal * 21)
        entry = calendar.next_session(session)
        exit_session = calendar.session_offset(session, 21)
        rows.append(dict(cohort=ordinal + 1, session=session.date().isoformat(),
                         decision_close=calendar.session_close(session).isoformat(),
                         entry_session=entry.date().isoformat(), entry_open=calendar.session_open(entry).isoformat(),
                         signal_deadline=(calendar.session_open(entry) - pd.Timedelta(minutes=5)).isoformat(),
                         exit_session=exit_session.date().isoformat(), exit_close=calendar.session_close(exit_session).isoformat()))
    return rows


def capture_state(cohort, now):
    now = utc(now)
    if now < utc(cohort["decision_close"]) + timedelta(minutes=15):
        return "WAITING_FOR_DECISION"
    if now >= utc(cohort["signal_deadline"]):
        return "MISSED_SIGNAL_WINDOW"
    return "CAPTURE_ALLOWED"


class PaperLedger:
    def __init__(self, directory):
        self.directory = Path(directory)

    @contextmanager
    def locked(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        lock = self.directory / ".lock"
        try:
            handle = lock.open("x", encoding="ascii")
        except FileExistsError as error:
            raise RuntimeError("paper tracker is busy; stale locks require operator review") from error
        try:
            with handle:
                handle.write(str(os.getpid()))
            yield self
        finally:
            lock.unlink()

    def read(self, name):
        path = self.directory / f"{name}.json"
        if not path.exists():
            return None
        envelope = json.loads(path.read_text(encoding="utf-8"))
        if digest(envelope["payload"]) != envelope["sha256"]:
            raise ValueError(f"paper artifact checksum mismatch: {name}")
        return envelope["payload"]

    def append(self, name, payload):
        previous = self.read(name)
        if previous is not None:
            if previous != payload:
                raise ValueError(f"paper observations are immutable: {name}")
            return False
        text = json.dumps(dict(sha256=digest(payload), payload=payload), indent=2, allow_nan=False) + "\n"
        temporary = self.directory / f".{name}.{os.getpid()}.tmp"
        destination = self.directory / f"{name}.json"
        try:
            with temporary.open("x", encoding="utf-8") as handle:
                handle.write(text)
                handle.flush()
                os.fsync(handle.fileno())
            os.link(temporary, destination)
        finally:
            if temporary.exists():
                temporary.unlink()
        return True


def freeze_manifest(report_path, now):
    report_path = Path(report_path)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if report["config_sha256"] != digest(report["config"]) or report["config_sha256"] != "be1b44081289cca693b274b29d51258c947e0f826ffd4fb5a44af283513ce191":
        raise ValueError("paper study requires the frozen reviewed ridge experiment")
    model = max(report["models"], key=lambda row: row["session"])
    if model["model_sha256"] != digest({key: value for key, value in model.items() if key != "model_sha256"}):
        raise ValueError("frozen model checksum mismatch")
    if utc(now).date().isoformat() <= model["session"]:
        raise ValueError("paper study must start after the fitted model")
    samples, sources = {}, []
    for prior in report["provenance"]:
        path = report_path.parent / prior["source_report"]
        if hashlib.sha256(path.read_bytes()).hexdigest() != prior["source_report_sha256"]:
            raise ValueError("frozen sample source changed")
        source = json.loads(path.read_text(encoding="utf-8"))
        samples[str(prior["sample"])] = source["sampled_tickers"]
        sources.append(dict(file=path.name, sha256=prior["source_report_sha256"]))
    if set(samples) != {"1", "2"} or any(len(set(names)) != 300 for names in samples.values()) \
            or set(samples["1"]).intersection(samples["2"]):
        raise ValueError("paper study must preserve both disjoint fixed samples")
    return dict(study_id="ridge_forward_paper_v1", enrolled_at=utc(now).isoformat(), research_drawdown_limit_pct=20,
                checkpoint_completed_cohorts=6, cohorts=plan_cohorts(now), model=model, samples=samples,
                source_report_sha256=hashlib.sha256(report_path.read_bytes()).hexdigest(), sample_sources=sources,
                model_policy="FIXED_LAST_SAVED_FIT_NO_REFITTING", equity_exposure=.75, cost_bps=10,
                universe_policy="Current complete decision-visible live universe, shared by ridge and momentum; historical samples are provenance only",
                policy_version="polygon_reference_v1", minimum_eligible=50, minimum_feature_coverage=.90,
                execution_policy="Prospectively reserved basket; later final daily open is a simulated fill proxy, not evidence of execution",
                missing_policy="Missing input, mark, identity or corporate-action coverage stays unresolved; no invented fills or liquidations",
                scope="OBSERVATION_ONLY_NO_ORDERS_NO_LIVE_RANK_PUBLICATION", historical_criterion_unchanged=True)


def serializable(value):
    if isinstance(value, dict):
        return {str(key): serializable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [serializable(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


class ReadOnlyInputs:
    def __init__(self, cursor):
        self.cursor = cursor

    def universe(self, manifest, cohort, now):
        self.cursor.execute("""
            SELECT universe_run_id::text, admitted_members, effective_from, observed_at, created_at, policy_sha256
            FROM equity_universe_runs WHERE availability_mode='LIVE_OBSERVED' AND status='COMPLETE'
              AND policy_version=%s AND effective_from<=%s AND effective_from>=%s
              AND observed_at<=%s AND created_at<=%s AND completed_at<=%s
            ORDER BY effective_from DESC,observed_at DESC,created_at DESC LIMIT 1
        """, (manifest["policy_version"], utc(cohort["decision_close"]), utc(cohort["decision_close"]) - timedelta(days=7), now, now, now))
        run = self.cursor.fetchone()
        if not run:
            raise ValueError("no recent complete visible live universe")
        self.cursor.execute("""
            SELECT ticker,security_id::text,first_observed_at FROM equity_universe_members
            WHERE universe_run_id=%s::uuid AND first_observed_at<=%s ORDER BY ticker
        """, (run["universe_run_id"], now))
        members = [dict(row) for row in self.cursor.fetchall()]
        if len(members) != run["admitted_members"] or len({row["ticker"] for row in members}) != len(members) \
                or len({row["security_id"] for row in members}) != len(members):
            raise ValueError("live universe membership is incomplete or ambiguous")
        return serializable(dict(run)), serializable(members)

    def bars(self, tickers, start, end, now):
        self.cursor.execute("""
            SELECT DISTINCT ON(ticker,session_date) ticker,security_id::text,session_date,bar_revision_id::text,
                open_price::float8 AS open,high_price::float8 AS high,low_price::float8 AS low,
                close_price::float8 AS close,volume::float8,bar_end,system_observed_at,created_at,
                availability_mode,payload_sha256
            FROM equity_bar_revisions WHERE ticker=ANY(%s::text[]) AND session_date BETWEEN %s::date AND %s::date
              AND interval='1d' AND session_scope='RTH' AND adjusted=FALSE AND is_final
              AND bar_end<=%s AND system_observed_at<=%s AND created_at<=%s
            ORDER BY ticker,session_date,system_observed_at DESC,created_at DESC,bar_revision_id
        """, (sorted(set(tickers)), start, end, now, now, now))
        return serializable([dict(row) for row in self.cursor.fetchall()])

    def actions(self, tickers, start, end, now):
        self.cursor.execute("""
            SELECT DISTINCT ON(ticker,action_type,window_start,window_end) coverage_id::text,ticker,action_type,security_id::text,
                window_start,window_end,first_observed_at,created_at,response_action_count,response_sha256
                        FROM equity_corporate_action_coverage WHERE ticker=ANY(%s::text[]) AND window_end>=%s::date
                            AND window_start<=%s::date AND first_observed_at<=%s AND created_at<=%s
              AND response_sha256 IS NOT NULL AND response_action_count IS NOT NULL
            ORDER BY ticker,action_type,window_start,window_end,first_observed_at DESC,created_at DESC,coverage_id
        """, (sorted(set(tickers)), start, end, now, now))
        coverage = serializable([dict(row) for row in self.cursor.fetchall()])
        self.cursor.execute("""
            SELECT link.coverage_id::text,action.corporate_action_id::text,action.ticker,action.security_id::text,
                action.action_type,action.effective_date,action.split_from::float8,action.split_to::float8,
                action.cash_amount::float8,action.payload_sha256,action.first_observed_at,
                action.revised_observed_at,action.created_at
            FROM equity_corporate_action_coverage_members AS link
            JOIN equity_corporate_actions AS action USING(corporate_action_id)
            WHERE link.coverage_id=ANY(%s::uuid[])
        """, ([row["coverage_id"] for row in coverage],))
        members = serializable([dict(row) for row in self.cursor.fetchall()])
        self.cursor.execute("""
            SELECT corporate_action_id::text,ticker,security_id::text,action_type,effective_date
            FROM equity_corporate_actions WHERE ticker=ANY(%s::text[]) AND effective_date BETWEEN %s::date AND %s::date
              AND action_type NOT IN ('SPLIT','DIVIDEND') AND first_observed_at<=%s AND created_at<=%s
        """, (sorted(set(tickers)), start, end, now, now))
        bundle = dict(coverage=coverage, members=members, other=serializable([dict(row) for row in self.cursor.fetchall()]))
        from equity.polygon import PolygonEquityClient
        from scripts.prepare_historical_signal_research import ResponseCache

        cache = ResponseCache(Path(__file__).resolve().parents[1] / ".cache" / "equity-paper")
        def fetch():
            rows = PolygonEquityClient().fetch_splits(pd.Timestamp(start).date(), pd.Timestamp(end).date())
            return [dict(observed_at=datetime.now(timezone.utc).isoformat(), start=start, end=end, rows=list(rows))]
        bundle["native_split_evidence"] = cache.get_or_fetch("paper-splits", f"{start}_{end}_{datetime.now(timezone.utc).date()}", fetch)[0]
        return bundle


def attach_native_split_coverage(bundle, identities, start, end, now):
    evidence = bundle.get("native_split_evidence")
    if evidence is None:
        return bundle
    if evidence["start"] != start or evidence["end"] != end or utc(evidence["observed_at"]) > utc(now):
        raise ValueError("native split evidence scope or availability mismatch")
    result = dict(bundle, coverage=list(bundle["coverage"]), members=list(bundle["members"]))
    for ticker, identity in identities.items():
        rows = [row for row in evidence["rows"] if row.get("ticker") == ticker]
        coverage_id = digest(dict(ticker=ticker, identity=identity, response=evidence))
        result["coverage"].append(dict(ticker=ticker, security_id=identity, action_type="SPLIT", coverage_id=coverage_id,
                                      window_start=start, window_end=end, first_observed_at=evidence["observed_at"],
                                      created_at=evidence["observed_at"], response_action_count=len(rows), response_sha256=digest(rows),
                                      source="PAPER_NATIVE_SPLIT_RESPONSE"))
        for row in rows:
            result["members"].append(dict(coverage_id=coverage_id, corporate_action_id=digest(row), ticker=ticker, security_id=identity,
                                          action_type="SPLIT", effective_date=row["execution_date"], split_from=row.get("split_from"),
                                          split_to=row.get("split_to"), first_observed_at=evidence["observed_at"], created_at=evidence["observed_at"]))
    return result


def covered_splits(bundle, ticker, security_id, start, end, now):
    candidates = [row for row in bundle["coverage"] if row["ticker"] == ticker and row["action_type"] == "SPLIT"
                  and row["window_start"] <= end and row["window_end"] >= start and row["security_id"] == security_id
                  and max(utc(row[field]) for field in ("first_observed_at", "created_at")) <= utc(now)]
    selected = {}
    for day in pd.date_range(start, end):
        session = day.date().isoformat()
        available = [row for row in candidates if row["window_start"] <= session <= row["window_end"]]
        if not available:
            raise ValueError("split response coverage unavailable")
        selected[session] = max(available, key=lambda row: (row["first_observed_at"], row["created_at"], row["coverage_id"]))["coverage_id"]
    if not selected:
        raise ValueError("split response coverage unavailable")
    chosen = set(selected.values())
    splits = {}
    for coverage in candidates:
        if coverage["coverage_id"] not in chosen:
            continue
        rows = [row for row in bundle["members"] if row["coverage_id"] == coverage["coverage_id"]]
        if len(rows) != coverage["response_action_count"] or len({row["corporate_action_id"] for row in rows}) != len(rows):
            raise ValueError("split response membership incomplete")
        for row in rows:
            if row["security_id"] != security_id or row["ticker"] != ticker or row["action_type"] != "SPLIT" \
                    or max(utc(row[field]) for field in ("first_observed_at", "created_at")) > utc(now) \
                    or (row.get("revised_observed_at") and utc(row["revised_observed_at"]) > utc(now)):
                raise ValueError("split member identity or clock mismatch")
            if not all(isinstance(row[field], (int, float)) and math.isfinite(row[field]) and row[field] > 0 for field in ("split_from", "split_to")):
                raise ValueError("invalid split ratio")
            if start < row["effective_date"] <= end and selected[row["effective_date"]] == coverage["coverage_id"]:
                if row["effective_date"] in splits:
                    raise ValueError("ambiguous split on one date")
                splits[row["effective_date"]] = row
    if any(row["ticker"] == ticker and start <= row["effective_date"] <= end for row in bundle["other"]):
        raise ValueError("non-split corporate event requires manual research review")
    return list(splits.values()), sorted(chosen)


def checked_bars(rows, ticker, security_id, dates, now):
    selected = [row for row in rows if row["ticker"] == ticker and row["session_date"] in dates]
    by_date = {row["session_date"]: row for row in selected}
    if len(by_date) != len(selected) or set(by_date) != set(dates):
        raise ValueError("missing or ambiguous daily prices")
    calendar = exchange_calendars.get_calendar("XNYS")
    for row in selected:
        prices = [row[field] for field in ("open", "high", "low", "close", "volume")]
        if row["security_id"] != security_id or not np.isfinite(prices).all() or min(prices[:4]) <= 0 or prices[4] < 0 \
                or row["high"] < max(row["open"], row["close"], row["low"]) or row["low"] > min(row["open"], row["close"], row["high"]):
            raise ValueError("invalid OHLCV or security identity")
        if utc(row["bar_end"]) != calendar.session_close(row["session_date"]).to_pydatetime() \
                or utc(row["system_observed_at"]) < utc(row["bar_end"]) \
                or max(utc(row[field]) for field in ("bar_end", "system_observed_at", "created_at")) > utc(now):
            raise ValueError("invalid price availability clocks")
    return [by_date[session] for session in dates]


def build_signal(manifest, cohort, now, run, members, bars, actions):
    from research.price_diagnostic import score_causal_ridge

    if capture_state(cohort, now) != "CAPTURE_ALLOWED":
        raise ValueError("cannot backdate a paper signal outside its capture window")
    calendar = exchange_calendars.get_calendar("XNYS")
    dates = [calendar.session_offset(cohort["session"], offset).date().isoformat() for offset in range(-252, 1)]
    actions = attach_native_split_coverage(actions, {row["ticker"]: row["security_id"] for row in members}, dates[0], dates[-1], now)
    ready, excluded = [], []
    for member in members:
        ticker, identity = member["ticker"], member["security_id"]
        try:
            history = checked_bars(bars, ticker, identity, dates, now)
            if history[-1]["availability_mode"] != "LIVE_OBSERVED":
                raise ValueError("decision bar must be live observed")
            splits, coverage_id = covered_splits(actions, ticker, identity, dates[0], dates[-1], now)
            closes = np.array([row["close"] for row in history], dtype=float)
            for split in splits:
                closes[np.array(dates) < split["effective_date"]] *= split["split_from"] / split["split_to"]
            ready.append(dict(ticker=ticker, security_id=identity, session=cohort["session"], feature_ready=True,
                              mom_12_1=float(closes[-22] / closes[0] - 1), rev_5=float(-(closes[-1] / closes[-6] - 1)),
                              split_coverage_id=coverage_id, price_ids=[row["bar_revision_id"] for row in history]))
        except ValueError as error:
            excluded.append(dict(ticker=ticker, reason=str(error)))
    if len(ready) < manifest["minimum_eligible"] or len(ready) < math.ceil(len(members) * manifest["minimum_feature_coverage"]):
        raise ValueError(f"insufficient causal feature coverage: {len(ready)}/{len(members)}; examples: {excluded[:3]}")
    spy_rows = [row for row in bars if row["ticker"] == "SPY" and row["session_date"] == cohort["session"]]
    if len(spy_rows) != 1 or spy_rows[0]["availability_mode"] != "LIVE_OBSERVED":
        raise ValueError("live SPY decision price unavailable")
    checked_bars(spy_rows, "SPY", spy_rows[0]["security_id"], [cohort["session"]], now)
    scored = score_causal_ridge(pd.DataFrame(ready), dict(manifest["model"], session=cohort["session"]))
    count = math.ceil(len(scored) * .1)
    portfolios = {}
    for strategy, column in (("RIDGE", "ridge_score"), ("MOMENTUM", "mom_12_1")):
        top = scored.sort_values([column, "ticker"], ascending=[False, True]).head(count)
        portfolios[strategy] = [dict(ticker=row.ticker, security_id=row.security_id, weight=.75 / count) for row in top.itertuples()]
    portfolios["SPY"] = [dict(ticker="SPY", security_id=spy_rows[0]["security_id"], weight=.75)]
    return dict(cohort=cohort["cohort"], session=cohort["session"], observed_at=utc(now).isoformat(),
                manifest_sha256=digest(manifest), model_sha256=manifest["model"]["model_sha256"],
                implementation_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                universe=run, members=members, exclusions=excluded, candidates=scored.to_dict("records"),
                portfolios=portfolios, input_bars=bars, input_actions=actions,
                status="PROSPECTIVE_SIGNAL_RECORDED", broker_orders=0)


def simulate_fill(position, cohort, rows, now):
    entry = checked_bars(rows, position["ticker"], position["security_id"], [cohort["entry_session"]], now)[0]
    if entry["availability_mode"] != "LIVE_OBSERVED":
        raise ValueError("paper entry cannot use retrospectively reconstructed prices")
    return dict(ticker=position["ticker"], security_id=position["security_id"], entry_session=cohort["entry_session"],
                price=entry["open"] if entry["volume"] > 0 else None, observed_at=utc(now).isoformat(), bar=entry,
                status="SIMULATED_DAILY_OPEN" if entry["volume"] > 0 else "NO_FILL_ZERO_VOLUME_POLICY")


def mark_portfolios(manifest, cohort, signal, fills, rows, actions, session, now):
    if not cohort["entry_session"] <= session <= cohort["exit_session"]:
        raise ValueError("mark outside the reserved holding window")
    results, evidence, issues = {}, {}, []
    if actions.get("native_split_evidence"):
        scope = actions["native_split_evidence"]
        identities = {row["ticker"]: row["security_id"] for group in signal["portfolios"].values() for row in group}
        actions = attach_native_split_coverage(actions, identities, scope["start"], scope["end"], now)
    for strategy, positions in signal["portfolios"].items():
        wealth, exposure = 1.0, 0.0
        for position in positions:
            ticker, identity, weight = position["ticker"], position["security_id"], position["weight"]
            try:
                fill = fills.get(identity)
                if not fill or fill["security_id"] != identity or fill["ticker"] != ticker or fill["entry_session"] != cohort["entry_session"]:
                    raise ValueError("entry observation unresolved")
                if fill["status"] == "NO_FILL_ZERO_VOLUME_POLICY":
                    continue
                mark = checked_bars(rows, ticker, identity, [session], now)[0]
                if mark["availability_mode"] != "LIVE_OBSERVED":
                    raise ValueError("daily mark must be live observed")
                splits, coverage_ids = covered_splits(actions, ticker, identity, cohort["entry_session"], session, now)
                share_multiplier = math.prod(row["split_to"] / row["split_from"] for row in splits)
                relative = share_multiplier * mark["close"] / fill["price"]
                fee = manifest["cost_bps"] / 20000 * (2 if session == cohort["exit_session"] else 1)
                wealth += weight * (relative - 1 - fee)
                exposure += weight
                evidence[identity] = dict(mark=mark, split_coverage_ids=coverage_ids, applied_splits=splits,
                                          fill_sha256=digest(fill), share_multiplier=share_multiplier)
            except ValueError as error:
                issues.append(dict(strategy=strategy, ticker=ticker, session=session, reason=str(error)))
        if not math.isfinite(wealth) or wealth <= 0:
            issues.append(dict(strategy=strategy, session=session, reason="paper capital depleted"))
        results[strategy] = dict(relative_equity=wealth, exposure=exposure,
                                 entry_relative_equity=1 - exposure * manifest["cost_bps"] / 20000)
    if issues:
        return None, issues
    return dict(cohort=cohort["cohort"], session=session, observed_at=utc(now).isoformat(), signal_sha256=digest(signal),
                status="SIMULATED_CLOSE_WITH_MODELLED_COSTS", portfolios=results, price_evidence=evidence,
                action_bundle_sha256=digest(actions), corporate_action_scope="Recorded split coverage; other known events block. No dividend income.",
                broker_orders=0), []


def report_study(ledger):
    manifest = ledger.read("manifest")
    if manifest is None:
        return dict(status="NOT_ENROLLED")
    calendar = exchange_calendars.get_calendar("XNYS")
    capital = dict(RIDGE=1.0, MOMENTUM=1.0, SPY=1.0)
    observed_capital = dict(capital)
    peaks = dict(capital)
    worst = dict.fromkeys(capital, 0.0)
    cohorts = []
    chain_complete = True
    marked_sessions, latest_session = 0, None
    for cohort in manifest["cohorts"]:
        signal = ledger.read(f"signal_{cohort['cohort']}")
        missed = ledger.read(f"missed_{cohort['cohort']}")
        dates = [value.date().isoformat() for value in calendar.sessions_in_range(cohort["entry_session"], cohort["exit_session"])]
        marks = [ledger.read(f"mark_{cohort['cohort']}_{session}") for session in dates]
        complete = all(mark is not None for mark in marks)
        if chain_complete and signal is not None:
            last_values = dict(capital)
            for index, mark in enumerate(marks):
                if mark is None:
                    break
                latest_session = mark["session"]
                marked_sessions += 1
                for strategy in capital:
                    values = [capital[strategy] * mark["portfolios"][strategy]["relative_equity"]]
                    if index == 0:
                        values.insert(0, capital[strategy] * mark["portfolios"][strategy]["entry_relative_equity"])
                    for value in values:
                        peaks[strategy] = max(peaks[strategy], value)
                        worst[strategy] = min(worst[strategy], 100 * (value / peaks[strategy] - 1))
                    last_values[strategy] = values[-1]
                    observed_capital[strategy] = values[-1]
            if complete:
                capital = last_values
        if not complete:
            chain_complete = False
        status = "COMPLETE" if complete else "MISSED_NO_BACKFILL" if missed else "CAPTURED_AWAITING_MARKS" if signal else "AWAITING_SIGNAL"
        cohorts.append(dict(cohort=cohort["cohort"], decision=cohort["session"], entry=cohort["entry_session"], exit=cohort["exit_session"],
                            status=status, marked_sessions=sum(mark is not None for mark in marks)))
    completed = sum(row["status"] == "COMPLETE" for row in cohorts)
    return dict(study_id=manifest["study_id"], enrolled_at=manifest["enrolled_at"], drawdown_limit_pct=20,
                status="CHECKPOINT_REACHED_REVIEW_REQUIRED" if completed == 6 else "OBSERVING",
                completed_cohorts=completed, checkpoint_cohorts=6, cohorts=cohorts,
                contiguous_marked_sessions=marked_sessions, latest_contiguous_mark=latest_session,
                cumulative_return_completed_cohorts_pct={key: (value - 1) * 100 for key, value in capital.items()} if completed else None,
                cumulative_return_through_contiguous_marks_pct={key: (value - 1) * 100 for key, value in observed_capital.items()} if marked_sessions else None,
                drawdown_through_contiguous_marks_pct=worst if marked_sessions else None,
                drawdown_breached={key: value < -20 for key, value in worst.items()} if marked_sessions else None,
                ridge_beats_spy_through_contiguous_marks=observed_capital["RIDGE"] > observed_capital["SPY"] if marked_sessions else None,
                performance_status="PARTIAL_OR_UNAVAILABLE" if not chain_complete else "SIX_COHORT_CHECKPOINT_NOT_DEPLOYMENT_APPROVAL",
                prospective_model=manifest["model"]["model_sha256"], broker_orders=0)


def run_once(ledger, *, now=None, inputs=None):
    now = utc(now or datetime.now(timezone.utc))
    manifest = ledger.read("manifest")
    if manifest is None:
        raise ValueError("enroll the paper study before observing")
    if now < utc(manifest["enrolled_at"]):
        raise ValueError("clock precedes enrollment")
    calendar = exchange_calendars.get_calendar("XNYS")
    statuses = []
    for cohort in manifest["cohorts"]:
        signal_name = f"signal_{cohort['cohort']}"
        signal = ledger.read(signal_name)
        if signal is None:
            state = capture_state(cohort, now)
            if state == "WAITING_FOR_DECISION":
                statuses.append(dict(cohort=cohort["cohort"], status=state))
                continue
            if state == "MISSED_SIGNAL_WINDOW":
                name = f"missed_{cohort['cohort']}"
                if ledger.read(name) is None:
                    ledger.append(name, dict(observed_at=now.isoformat(), status=state, cohort=cohort["cohort"]))
                statuses.append(dict(cohort=cohort["cohort"], status=state))
                continue
            try:
                run, members = inputs.universe(manifest, cohort, now)
                start = calendar.session_offset(cohort["session"], -252).date().isoformat()
                tickers = [row["ticker"] for row in members] + ["SPY"]
                bars = inputs.bars(tickers, start, cohort["session"], now)
                actions = inputs.actions(tickers, start, cohort["session"], now)
                captured_at = datetime.now(timezone.utc) if isinstance(inputs, ReadOnlyInputs) else now
                signal = build_signal(manifest, cohort, captured_at, run, members, bars, actions)
                written_at = utc(datetime.now(timezone.utc)) if isinstance(inputs, ReadOnlyInputs) else now
                if capture_state(cohort, written_at) != "CAPTURE_ALLOWED":
                    raise ValueError("input collection exceeded signal deadline")
                signal["recorded_at"] = written_at.isoformat()
                ledger.append(signal_name, signal)
            except ValueError as error:
                statuses.append(dict(cohort=cohort["cohort"], status="BLOCKED_INPUTS", reason=str(error)))
                continue
        if utc(signal.get("recorded_at", signal["observed_at"])) >= utc(cohort["signal_deadline"]) or signal["manifest_sha256"] != digest(manifest):
            raise ValueError("saved signal fails timing or manifest integrity")
        due = [value.date().isoformat() for value in calendar.sessions_in_range(cohort["entry_session"], cohort["exit_session"])
               if calendar.session_close(value).to_pydatetime() + timedelta(minutes=15) <= now
               and ledger.read(f"mark_{cohort['cohort']}_{value.date().isoformat()}") is None]
        if not due:
            statuses.append(dict(cohort=cohort["cohort"], status="NO_MARK_DUE"))
            continue
        positions = {row["security_id"]: row for group in signal["portfolios"].values() for row in group}
        bars = inputs.bars([row["ticker"] for row in positions.values()], cohort["entry_session"], due[-1], now)
        actions = inputs.actions([row["ticker"] for row in positions.values()], cohort["entry_session"], due[-1], now)
        mark_time = datetime.now(timezone.utc) if isinstance(inputs, ReadOnlyInputs) else now
        fills = {}
        for identity, position in positions.items():
            name = f"fill_{cohort['cohort']}_{identity}"
            fill = ledger.read(name)
            if fill is None:
                try:
                    fill = simulate_fill(position, cohort, bars, mark_time)
                    ledger.append(name, fill)
                except ValueError:
                    continue
            fills[identity] = fill
        for session in due:
            mark, issues = mark_portfolios(manifest, cohort, signal, fills, bars, actions, session, mark_time)
            if issues:
                issue_name = f"issue_{cohort['cohort']}_{session}_{digest(issues)[:12]}"
                if ledger.read(issue_name) is None:
                    ledger.append(issue_name, dict(first_observed_at=mark_time.isoformat(), issues=issues))
                statuses.append(dict(cohort=cohort["cohort"], session=session, status="UNRESOLVED_MARK", issues=issues[:3]))
            else:
                ledger.append(f"mark_{cohort['cohort']}_{session}", mark)
                statuses.append(dict(cohort=cohort["cohort"], session=session, status="MARK_RECORDED"))
    return dict(observed_at=now.isoformat(), updates=statuses, report=report_study(ledger))


def observe(ledger):
    from database import get_db_cursor

    with ledger.locked():
        with get_db_cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '120s'")
            return run_once(ledger, inputs=ReadOnlyInputs(cursor))


def check_current_inputs(ledger):
    from database import get_db_cursor
    from equity.calendar import latest_expected_market_time

    manifest = ledger.read("manifest")
    if manifest is None:
        raise ValueError("enroll before checking inputs")
    now = datetime.now(timezone.utc)
    close = latest_expected_market_time(now - timedelta(minutes=15), "1d")
    cohort = plan_cohorts(close - timedelta(seconds=1), count=1)[0]
    if capture_state(cohort, now) != "CAPTURE_ALLOWED":
        return dict(status="CHECK_AFTER_CLOSE_BEFORE_NEXT_OPEN", candidate_signal_written=False)
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '120s'")
        inputs = ReadOnlyInputs(cursor)
        run, members = inputs.universe(manifest, cohort, now)
        calendar = exchange_calendars.get_calendar("XNYS")
        start = calendar.session_offset(cohort["session"], -252).date().isoformat()
        tickers = [row["ticker"] for row in members] + ["SPY"]
        bars = inputs.bars(tickers, start, cohort["session"], now)
        actions = inputs.actions(tickers, start, cohort["session"], now)
        try:
            signal = build_signal(manifest, cohort, datetime.now(timezone.utc), run, members, bars, actions)
            return dict(status="INPUTS_READY_ON_LAST_CLOSED_SESSION", session=cohort["session"],
                        universe_members=len(members), eligible_candidates=len(signal["candidates"]),
                        excluded=len(signal["exclusions"]), candidate_signal_written=False)
        except ValueError as error:
            return dict(status="BLOCKED_INPUTS", session=cohort["session"], universe_members=len(members),
                        reason=str(error), candidate_signal_written=False)