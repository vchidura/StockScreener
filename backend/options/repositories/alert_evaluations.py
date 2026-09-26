from __future__ import annotations

from datetime import timedelta
from zoneinfo import ZoneInfo

from psycopg2.extras import Json, execute_values

from options.analytics.alert_selection import (
    O1IndicatorEvaluationEvidence, O3CreditEvaluationEvidence, StockSetupIndicatorEvaluationEvidence, SurfaceEvaluationEvidence,
    load_evaluation_evidence, load_detector_run,
)
from .base import PostgresRepository


class OptionAlertEvaluationRepository(PostgresRepository):
    def schema_readiness(self):
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute("""SELECT clock_timestamp() AS reviewed_at,
                current_setting('server_version') AS server_version,
                current_setting('transaction_read_only')::boolean AS read_only,
                to_regclass('public.option_detector_evaluations') IS NOT NULL AS table_present,
                to_regclass('public.option_o1_indicator_observations') IS NOT NULL AS o1_table_present,
                to_regclass('public.option_o3_credit_observations') IS NOT NULL AS o3_table_present,
                to_regclass('public.option_stock_setup_indicator_observations') IS NOT NULL AS stock_setup_table_present,
                to_regclass('public.option_strategy_candidates') IS NOT NULL AS candidates_present,
                to_regclass('public.option_analysis_runs') IS NOT NULL AS matrices_present,
                to_regprocedure('public.reject_option_alert_mutation()') IS NOT NULL AS mutation_guard_present,
                EXISTS(SELECT 1 FROM public.schema_migrations WHERE version='053_option_detector_evaluations') AS registered,
                EXISTS(SELECT 1 FROM public.schema_migrations WHERE version='054_option_o1_indicator_observations') AS o1_registered,
                EXISTS(SELECT 1 FROM public.schema_migrations WHERE version='056_option_o3_credit_observations') AS o3_registered,
                EXISTS(SELECT 1 FROM public.schema_migrations WHERE version='057_option_o3_indicative_admission') AS o3_admission_registered,
                EXISTS(SELECT 1 FROM public.schema_migrations WHERE version='055_option_stock_setup_indicator_observations') AS stock_setup_registered""")
            report = dict(cursor.fetchone())
            cursor.execute("""SELECT COUNT(*) AS waiting_locks FROM pg_locks
                WHERE NOT granted AND relation IN (
                    to_regclass('public.option_detector_evaluations'),
                    to_regclass('public.option_o1_indicator_observations'),
                    to_regclass('public.option_o3_credit_observations'),
                    to_regclass('public.option_stock_setup_indicator_observations'),
                    to_regclass('public.option_strategy_candidates'),to_regclass('public.option_analysis_runs'))""")
            report.update(dict(cursor.fetchone()))
            if report["table_present"]:
                cursor.execute("""SELECT tgname AS name,tgenabled AS enabled FROM pg_trigger
                    WHERE tgrelid='public.option_detector_evaluations'::regclass AND NOT tgisinternal ORDER BY tgname""")
                report["triggers"] = [dict(row) for row in cursor.fetchall()]
                cursor.execute("""SELECT indexrelid::regclass::text AS name,indisvalid AS valid,indisready AS ready
                    FROM pg_index WHERE indrelid='public.option_detector_evaluations'::regclass ORDER BY indexrelid::text""")
                report["indexes"] = [dict(row) for row in cursor.fetchall()]
                cursor.execute("""SELECT COUNT(*) FILTER (WHERE NOT convalidated) AS unvalidated_constraints
                    FROM pg_constraint WHERE conrelid='public.option_detector_evaluations'::regclass""")
                report.update(dict(cursor.fetchone()))
                cursor.execute("""SELECT has_table_privilege(current_user,'public.option_detector_evaluations','SELECT') AS runtime_select,
                    has_table_privilege(current_user,'public.option_detector_evaluations','INSERT') AS runtime_insert""")
                report.update(dict(cursor.fetchone()))
                if report["runtime_select"]:
                    cursor.execute("SELECT COUNT(*) AS evaluation_rows FROM public.option_detector_evaluations")
                    report.update(dict(cursor.fetchone()))
            if report.get("o1_table_present", False):
                cursor.execute("""SELECT has_table_privilege(current_user,'public.option_o1_indicator_observations','SELECT') AS o1_runtime_select,
                    has_table_privilege(current_user,'public.option_o1_indicator_observations','INSERT') AS o1_runtime_insert""")
                report.update(dict(cursor.fetchone()))
                cursor.execute("""SELECT tgname AS name,tgenabled AS enabled FROM pg_trigger
                    WHERE tgrelid='public.option_o1_indicator_observations'::regclass AND NOT tgisinternal ORDER BY tgname""")
                report["o1_triggers"] = [dict(row) for row in cursor.fetchall()]
                if report["o1_runtime_select"]:
                    cursor.execute("SELECT COUNT(*) AS o1_observation_rows FROM public.option_o1_indicator_observations")
                    report.update(dict(cursor.fetchone()))
            if report.get("o3_table_present", False):
                cursor.execute("""SELECT has_table_privilege(current_user,'public.option_o3_credit_observations','SELECT') AS o3_runtime_select,
                    has_table_privilege(current_user,'public.option_o3_credit_observations','INSERT') AS o3_runtime_insert""")
                report.update(dict(cursor.fetchone()))
                cursor.execute("""SELECT tgname AS name,tgenabled AS enabled FROM pg_trigger
                    WHERE tgrelid='public.option_o3_credit_observations'::regclass AND NOT tgisinternal ORDER BY tgname""")
                report["o3_triggers"] = [dict(row) for row in cursor.fetchall()]
            if report.get("stock_setup_table_present", False):
                cursor.execute("""SELECT has_table_privilege(current_user,'public.option_stock_setup_indicator_observations','SELECT') AS stock_setup_runtime_select,
                    has_table_privilege(current_user,'public.option_stock_setup_indicator_observations','INSERT') AS stock_setup_runtime_insert""")
                report.update(dict(cursor.fetchone()))
                cursor.execute("""SELECT tgname AS name,tgenabled AS enabled FROM pg_trigger
                    WHERE tgrelid='public.option_stock_setup_indicator_observations'::regclass AND NOT tgisinternal ORDER BY tgname""")
                report["stock_setup_triggers"] = [dict(row) for row in cursor.fetchall()]
                if report["stock_setup_runtime_select"]:
                    cursor.execute("SELECT COUNT(*) AS stock_setup_observation_rows FROM public.option_stock_setup_indicator_observations")
                    report.update(dict(cursor.fetchone()))
            return report

    def persist_run(self, records):
        records = tuple(load_evaluation_evidence(row.canonical_json()) for row in records)
        if not records:
            return 0
        if (len(records) > 5000 or len({row.run_id for row in records}) != 1
            or len({(row.dataset_id, row.selector_sha256, row.selected_at) for row in records}) != 1
                or len({row.evaluation_id for row in records}) != len(records)
            or sum(row.selection_status == "SELECTED" for row in records) > 20
            or sum(len(row.canonical_json().encode("ascii")) for row in records) > 67108864):
            raise ValueError("evaluation write requires one bounded complete selection batch")
        payloads = {row.evaluation_id: (row.canonical_json(), row.sha256) for row in records}
        with self._cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute("SET LOCAL lock_timeout = '2s'")
            cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (str(records[0].run_id),))
            return self._persist_records(cursor, records, payloads)

    @staticmethod
    def _persist_records(cursor, records, payloads, *, allow_insert=True):
        if not records:
            return 0
        o1_records = tuple(row for row in records if isinstance(row, O1IndicatorEvaluationEvidence))
        o3_records = tuple(row for row in records if isinstance(row, O3CreditEvaluationEvidence))
        setup_records = tuple(row for row in records if isinstance(row, StockSetupIndicatorEvaluationEvidence))
        evaluation_records = tuple(row for row in records
            if not isinstance(row, (O1IndicatorEvaluationEvidence, O3CreditEvaluationEvidence, StockSetupIndicatorEvaluationEvidence)))
        cursor.execute("SELECT evaluation_id,payload_text,payload_sha256 FROM option_detector_evaluations WHERE run_id=%s", (records[0].run_id,))
        existing = {row["evaluation_id"]: (row["payload_text"], row["payload_sha256"]) for row in cursor.fetchall()}
        cursor.execute("SELECT evaluation_id,payload_text,payload_sha256 FROM option_o1_indicator_observations WHERE run_id=%s", (records[0].run_id,))
        existing.update({row["evaluation_id"]: (row["payload_text"], row["payload_sha256"]) for row in cursor.fetchall()})
        cursor.execute("SELECT evaluation_id,payload_text,payload_sha256 FROM option_o3_credit_observations WHERE run_id=%s", (records[0].run_id,))
        existing.update({row["evaluation_id"]: (row["payload_text"], row["payload_sha256"]) for row in cursor.fetchall()})
        cursor.execute("SELECT evaluation_id,payload_text,payload_sha256 FROM option_stock_setup_indicator_observations WHERE run_id=%s", (records[0].run_id,))
        existing.update({row["evaluation_id"]: (row["payload_text"], row["payload_sha256"]) for row in cursor.fetchall()})
        if existing:
            if existing != payloads:
                raise ValueError("stored evaluation run differs from original selection evidence")
            return 0
        if not allow_insert:
            raise ValueError("completed detector run is missing retained evaluation rows")
        if evaluation_records:
            execute_values(cursor, """INSERT INTO option_detector_evaluations (
                evaluation_id,dataset_id,run_id,scheduled_cycle,session_date,selector_sha256,
                detector_id,candidate_id,matrix_id,recurrence_sha256,selection_status,selection_reason,
                selected_at,payload_text,payload_sha256) VALUES %s""", [
                (row.evaluation_id, row.dataset_id, row.run_id, row.scheduled_cycle,
                 row.scheduled_cycle.astimezone(ZoneInfo("America/New_York")).date(), row.selector_sha256,
                 row.detector_id, row.candidate_id, row.matrix_id, row.recurrence_sha256,
                 row.selection_status, row.selection_reason, row.selected_at, *payloads[row.evaluation_id])
                for row in evaluation_records])
        if o1_records:
            execute_values(cursor, """INSERT INTO option_o1_indicator_observations (
                evaluation_id,dataset_id,run_id,scheduled_cycle,session_date,selector_sha256,
                matrix_id,recurrence_sha256,selected_at,payload_text,payload_sha256) VALUES %s""", [
                (row.evaluation_id, row.dataset_id, row.run_id, row.scheduled_cycle,
                 row.scheduled_cycle.astimezone(ZoneInfo("America/New_York")).date(), row.selector_sha256,
                 row.matrix_id, row.recurrence_sha256, row.selected_at, *payloads[row.evaluation_id])
                for row in o1_records])
        if o3_records:
            execute_values(cursor, """INSERT INTO option_o3_credit_observations (
                evaluation_id,dataset_id,run_id,scheduled_cycle,session_date,selector_sha256,
                candidate_id,matrix_id,recurrence_sha256,selected_at,payload_text,payload_sha256) VALUES %s""", [
                (row.evaluation_id, row.dataset_id, row.run_id, row.scheduled_cycle,
                 row.scheduled_cycle.astimezone(ZoneInfo("America/New_York")).date(), row.selector_sha256,
                 row.candidate_id, row.matrix_id, row.recurrence_sha256, row.selected_at, *payloads[row.evaluation_id])
                for row in o3_records])
        if setup_records:
            execute_values(cursor, """INSERT INTO option_stock_setup_indicator_observations (
                evaluation_id,dataset_id,run_id,scheduled_cycle,session_date,selector_sha256,
                detector_id,matrix_id,recurrence_sha256,selected_at,payload_text,payload_sha256) VALUES %s""", [
                (row.evaluation_id, row.dataset_id, row.run_id, row.scheduled_cycle,
                 row.scheduled_cycle.astimezone(ZoneInfo("America/New_York")).date(), row.selector_sha256,
                 row.detector_id, row.matrix_id, row.recurrence_sha256, row.selected_at, *payloads[row.evaluation_id])
                for row in setup_records])
        return len(records)

    @staticmethod
    def _read_run(row):
        evidence = row["selection_evidence"]
        if evidence.get("kind") != "DUAL_ORIGIN_COMPLETE_RUN":
            raise ValueError("unsupported detector run evidence")
        run = load_detector_run(evidence["payload_text"])
        if (run.sha256 != evidence["payload_sha256"] or run.canonical_json() != evidence["payload_text"]
                or str(run.run_id) != str(row["publication_id"]) or run.scope_sha256 != row["selector_sha256"]
            or run.dataset_id != evidence["dataset_id"]
            or run.scheduled_cycle != row["scheduled_cycle"] or run.selected_at != row["published_at"]
            or run.configuration_sha256 != row["configuration_sha256"]
            or run.strategy_policy_sha256 != row["strategy_policy_sha256"]
            or list(matrix for _, matrix in run.source_matrices) != row["source_matrix_ids"]
            or len(run.expected_underlyers) != row["covered_underlying_count"]):
            raise ValueError("stored detector run identity/hash mismatch")
        return run

    def completed_runs(self, *, dataset_id, as_of):
        if as_of.utcoffset() is None:
            raise ValueError("detector run lookup requires an aware cutoff")
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute("""SELECT publication_id,selector_sha256,selection_evidence,scheduled_cycle,published_at,
                    configuration_sha256,strategy_policy_sha256,source_matrix_ids,covered_underlying_count
                FROM option_board_publications WHERE status='COMPLETE' AND selector_version IN ('option_detector_run_v1','option_detector_run_v2')
                  AND selection_evidence->>'dataset_id'=%s AND scheduled_cycle>=%s AND scheduled_cycle<=%s
                  AND published_at<=%s AND created_at<=%s
                ORDER BY scheduled_cycle LIMIT 5001""", (dataset_id, as_of - timedelta(days=60), as_of, as_of, as_of))
            rows = cursor.fetchall()
            if len(rows) > 5000:
                raise ValueError("detector run lookup exceeds bound")
            return tuple(self._read_run(row) for row in rows)

    def completed_run(self, *, dataset_id, scheduled_cycle, as_of):
        if as_of.utcoffset() is None or scheduled_cycle.utcoffset() is None or scheduled_cycle > as_of:
            raise ValueError("completed run lookup requires causal cutoffs")
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute("""SELECT publication_id,selector_sha256,selection_evidence,scheduled_cycle,published_at,
                    configuration_sha256,strategy_policy_sha256,source_matrix_ids,covered_underlying_count
                FROM option_board_publications WHERE status='COMPLETE' AND selector_version IN ('option_detector_run_v1','option_detector_run_v2')
                  AND selection_evidence->>'dataset_id'=%s AND scheduled_cycle=%s
                  AND published_at<=%s AND created_at<=%s LIMIT 2""", (dataset_id, scheduled_cycle, as_of, as_of))
            headers = cursor.fetchall()
            if not headers:
                return None
            if len(headers) != 1:
                raise ValueError("completed run lookup found ambiguous dataset/cycle")
            run = self._read_run(headers[0])
            if run.dataset_id != dataset_id or run.scheduled_cycle != scheduled_cycle or run.selected_at > as_of:
                raise ValueError("completed run lookup scope mismatch")
            cursor.execute("""SELECT evaluation_id,payload_text,payload_sha256 FROM option_detector_evaluations
                WHERE dataset_id=%s AND run_id=%s AND selected_at<=%s AND recorded_at<=%s
                UNION ALL
                SELECT evaluation_id,payload_text,payload_sha256 FROM option_o1_indicator_observations
                WHERE dataset_id=%s AND run_id=%s AND selected_at<=%s AND recorded_at<=%s
                UNION ALL
                SELECT evaluation_id,payload_text,payload_sha256 FROM option_o3_credit_observations
                WHERE dataset_id=%s AND run_id=%s AND selected_at<=%s AND recorded_at<=%s
                UNION ALL
                SELECT evaluation_id,payload_text,payload_sha256 FROM option_stock_setup_indicator_observations
                WHERE dataset_id=%s AND run_id=%s AND selected_at<=%s AND recorded_at<=%s
                ORDER BY evaluation_id LIMIT 5001""", (dataset_id, run.run_id, as_of, as_of,
                    dataset_id, run.run_id, as_of, as_of, dataset_id, run.run_id, as_of, as_of,
                    dataset_id, run.run_id, as_of, as_of))
            rows = cursor.fetchall()
            if len(rows) > 5000 or sum(len(row["payload_text"].encode("ascii")) for row in rows) > 67108864:
                raise ValueError("completed run payload bound exceeded")
            records = []
            for row in rows:
                record = load_evaluation_evidence(row["payload_text"])
                if (record.sha256 != row["payload_sha256"] or record.evaluation_id != row["evaluation_id"]
                        or record.canonical_json() != row["payload_text"] or record.dataset_id != dataset_id
                        or record.run_id != run.run_id or record.selected_at != run.selected_at):
                    raise ValueError("completed run evaluation identity mismatch")
                records.append(record)
            if tuple(sorted(((row.evaluation_id, row.sha256) for row in records), key=lambda item: str(item[0]))) != run.record_sha256s:
                raise ValueError("completed run is missing or has changed evaluation rows")
            return run, tuple(records)

    @staticmethod
    def _prior_selected(cursor, dataset_id, scheduled_cycle, as_of):
        cursor.execute("""SELECT DISTINCT ON (evaluation.recurrence_sha256) evaluation.payload_text,evaluation.payload_sha256
            FROM option_detector_evaluations AS evaluation
            JOIN option_board_publications AS publication ON publication.publication_id=evaluation.run_id
            WHERE evaluation.dataset_id=%s AND evaluation.selection_status='SELECTED'
              AND publication.status='COMPLETE' AND publication.selector_version IN ('option_detector_run_v1','option_detector_run_v2')
              AND publication.selection_evidence->>'dataset_id'=%s
              AND evaluation.scheduled_cycle>=%s AND evaluation.scheduled_cycle<%s
              AND evaluation.selected_at<=%s AND evaluation.recorded_at<=%s AND publication.created_at<=%s
            ORDER BY evaluation.recurrence_sha256,evaluation.scheduled_cycle LIMIT 100001""",
            (dataset_id, dataset_id, as_of - timedelta(days=61), scheduled_cycle, as_of, as_of, as_of))
        rows = cursor.fetchall()
        if len(rows) > 100000:
            raise ValueError("prior detector alerts exceed bound")
        prior = {}
        payload_bytes = 0
        for row in rows:
            payload_bytes += len(row["payload_text"].encode("ascii"))
            if payload_bytes > 67108864:
                raise ValueError("prior detector payload exceeds bound")
            record = load_evaluation_evidence(row["payload_text"])
            if record.sha256 != row["payload_sha256"] or record.selection_status != "SELECTED" or record.dataset_id != dataset_id:
                raise ValueError("prior alert evidence identity mismatch")
            prior[record.recurrence_sha256] = record.package
        return prior

    def prior_selected(self, *, dataset_id, scheduled_cycle, as_of):
        if as_of.utcoffset() is None or scheduled_cycle.utcoffset() is None or scheduled_cycle > as_of:
            raise ValueError("prior alert lookup requires causal cutoffs")
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            return self._prior_selected(cursor, dataset_id, scheduled_cycle, as_of)

    def prior_o1_observed(self, *, dataset_id, scheduled_cycle, as_of):
        if as_of.utcoffset() is None or scheduled_cycle.utcoffset() is None or scheduled_cycle > as_of:
            raise ValueError("prior O1 observation lookup requires causal cutoffs")
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute("""SELECT recurrence_sha256 FROM option_o1_indicator_observations
                WHERE dataset_id=%s AND scheduled_cycle>=%s AND scheduled_cycle<%s
                  AND selected_at<=%s AND recorded_at<=%s
                ORDER BY scheduled_cycle,recurrence_sha256 LIMIT 10001""",
                (dataset_id, as_of - timedelta(days=2), scheduled_cycle, as_of, as_of))
            rows = cursor.fetchall()
            if len(rows) > 10000:
                raise ValueError("prior O1 observations exceed bound")
            return {row["recurrence_sha256"] for row in rows}

    def prior_o3_observed(self, *, dataset_id, scheduled_cycle, as_of):
        if as_of.utcoffset() is None or scheduled_cycle.utcoffset() is None or scheduled_cycle > as_of:
            raise ValueError("prior O3 observation lookup requires causal cutoffs")
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute("""SELECT recurrence_sha256 FROM option_o3_credit_observations
                WHERE dataset_id=%s AND scheduled_cycle>=%s AND scheduled_cycle<%s
                  AND selected_at<=%s AND recorded_at<=%s
                ORDER BY scheduled_cycle,recurrence_sha256 LIMIT 10001""",
                (dataset_id, as_of - timedelta(days=31), scheduled_cycle, as_of, as_of))
            rows = cursor.fetchall()
            if len(rows) > 10000:
                raise ValueError("prior O3 observations exceed bound")
            return {row["recurrence_sha256"] for row in rows}

    def prior_stock_setup_observed(self, *, dataset_id, scheduled_cycle, as_of):
        if as_of.utcoffset() is None or scheduled_cycle.utcoffset() is None or scheduled_cycle > as_of:
            raise ValueError("prior stock setup observation lookup requires causal cutoffs")
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute("""SELECT recurrence_sha256 FROM option_stock_setup_indicator_observations
                WHERE dataset_id=%s AND scheduled_cycle>=%s AND scheduled_cycle<%s
                  AND selected_at<=%s AND recorded_at<=%s
                ORDER BY scheduled_cycle,recurrence_sha256 LIMIT 10001""",
                (dataset_id, as_of - timedelta(days=2), scheduled_cycle, as_of, as_of))
            rows = cursor.fetchall()
            if len(rows) > 10000:
                raise ValueError("prior stock setup observations exceed bound")
            return {row["recurrence_sha256"] for row in rows}

    def o1_indicator_observations(self, *, start_date, end_date, as_of, dataset_id=None):
        if (as_of.utcoffset() is None or start_date > end_date or (end_date - start_date).days > 31
                or (dataset_id is not None and (not dataset_id or len(dataset_id) > 80))):
            raise ValueError("O1 indicator review requires a bounded period and causal cutoff")
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            where_dataset = " AND dataset_id=%s" if dataset_id is not None else ""
            parameters = [start_date, end_date, as_of, as_of]
            if dataset_id is not None:
                parameters.append(dataset_id)
            cursor.execute(f"""SELECT evaluation_id,dataset_id,run_id,scheduled_cycle,selected_at,
                    payload_text,payload_sha256 FROM option_o1_indicator_observations
                WHERE session_date BETWEEN %s AND %s AND selected_at<=%s AND recorded_at<=%s{where_dataset}
                ORDER BY session_date,scheduled_cycle,evaluation_id LIMIT 5001""", parameters)
            rows = cursor.fetchall()
            if len(rows) > 5000:
                raise ValueError("O1 indicator review exceeds row bound; narrow the period or dataset")
            payload_bytes = 0
            records = []
            for row in rows:
                payload_bytes += len(row["payload_text"].encode("ascii"))
                if payload_bytes > 67108864:
                    raise ValueError("O1 indicator review exceeds payload bound; narrow the period or dataset")
                record = load_evaluation_evidence(row["payload_text"])
                if (not isinstance(record, O1IndicatorEvaluationEvidence)
                        or record.canonical_json() != row["payload_text"] or record.sha256 != row["payload_sha256"]
                        or record.evaluation_id != row["evaluation_id"] or record.dataset_id != row["dataset_id"]
                        or record.run_id != row["run_id"] or record.scheduled_cycle != row["scheduled_cycle"]
                        or record.selected_at != row["selected_at"]):
                    raise ValueError("O1 indicator review evidence identity mismatch")
                records.append(record)
            return tuple(records)

    def stock_setup_indicator_observations(self, *, start_date, end_date, as_of, dataset_id=None):
        if (as_of.utcoffset() is None or start_date > end_date or (end_date - start_date).days > 31
                or (dataset_id is not None and (not dataset_id or len(dataset_id) > 80))):
            raise ValueError("stock setup indicator review requires a bounded period and causal cutoff")
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            where_dataset = " AND dataset_id=%s" if dataset_id is not None else ""
            parameters = [start_date, end_date, as_of, as_of]
            if dataset_id is not None:
                parameters.append(dataset_id)
            cursor.execute(f"""SELECT evaluation_id,dataset_id,run_id,scheduled_cycle,selected_at,
                    payload_text,payload_sha256 FROM option_stock_setup_indicator_observations
                WHERE session_date BETWEEN %s AND %s AND selected_at<=%s AND recorded_at<=%s{where_dataset}
                ORDER BY session_date,scheduled_cycle,evaluation_id LIMIT 5001""", parameters)
            rows = cursor.fetchall()
            if len(rows) > 5000:
                raise ValueError("stock setup indicator review exceeds row bound; narrow the period or dataset")
            payload_bytes = 0
            records = []
            for row in rows:
                payload_bytes += len(row["payload_text"].encode("ascii"))
                if payload_bytes > 67108864:
                    raise ValueError("stock setup indicator review exceeds payload bound; narrow the period or dataset")
                record = load_evaluation_evidence(row["payload_text"])
                if (not isinstance(record, StockSetupIndicatorEvaluationEvidence)
                        or record.canonical_json() != row["payload_text"] or record.sha256 != row["payload_sha256"]
                        or record.evaluation_id != row["evaluation_id"] or record.dataset_id != row["dataset_id"]
                        or record.run_id != row["run_id"] or record.scheduled_cycle != row["scheduled_cycle"]
                        or record.selected_at != row["selected_at"]):
                    raise ValueError("stock setup indicator review evidence identity mismatch")
                records.append(record)
            return tuple(records)

    def repeat_counts(self, *, dataset_id, records, as_of):
        if (as_of.utcoffset() is None or len(records) > 200
                or any(row.dataset_id != dataset_id or row.selection_status != "SELECTED" or row.selected_at > as_of for row in records)):
            raise ValueError("repeat lookup requires bounded original selected alerts")
        if not records:
            return {}
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute("""SELECT original.evaluation_id,COUNT(DISTINCT hit.run_id) AS repeats,
                    MAX(hit.selected_at) AS last_seen_at
                FROM option_detector_evaluations AS original
                JOIN option_board_publications AS origin_run ON origin_run.publication_id=original.run_id
                LEFT JOIN (
                    SELECT evaluation.* FROM option_detector_evaluations AS evaluation
                    JOIN option_board_publications AS publication ON publication.publication_id=evaluation.run_id
                    WHERE evaluation.dataset_id=%s AND evaluation.selection_status='REPEAT'
                      AND evaluation.selected_at<=%s AND evaluation.recorded_at<=%s
                      AND publication.selector_version IN ('option_detector_run_v1','option_detector_run_v2') AND publication.status='COMPLETE'
                      AND publication.selection_evidence->>'dataset_id'=%s
                      AND publication.published_at<=%s AND publication.created_at<=%s
                ) AS hit ON hit.dataset_id=original.dataset_id AND hit.detector_id=original.detector_id
                  AND hit.recurrence_sha256=original.recurrence_sha256 AND hit.scheduled_cycle>original.scheduled_cycle
                  AND hit.selected_at>=original.selected_at
                  AND hit.selected_at<(original.payload_text::jsonb->'package'->>'expires_at')::timestamptz
                  AND hit.payload_text::jsonb->>'first_candidate_id'=original.candidate_id::text
                WHERE original.dataset_id=%s AND original.evaluation_id=ANY(%s::uuid[])
                  AND original.selection_status='SELECTED' AND original.selected_at<=%s AND original.recorded_at<=%s
                  AND origin_run.selector_version IN ('option_detector_run_v1','option_detector_run_v2') AND origin_run.status='COMPLETE'
                  AND origin_run.selection_evidence->>'dataset_id'=%s
                  AND origin_run.published_at<=%s AND origin_run.created_at<=%s
                GROUP BY original.evaluation_id""",
                (dataset_id, as_of, as_of, dataset_id, as_of, as_of, dataset_id,
                 [row.evaluation_id for row in records], as_of, as_of, dataset_id, as_of, as_of))
            results = {row["evaluation_id"]: dict(repeats=int(row["repeats"]), last_seen_at=row["last_seen_at"])
                for row in cursor.fetchall()}
            if set(results) != {row.evaluation_id for row in records}:
                raise ValueError("repeat lookup cannot resolve every original completed alert")
            return results

    def persist_completed_run(self, run, records):
        from collections import Counter

        run = load_detector_run(run.canonical_json())
        records = tuple(load_evaluation_evidence(row.canonical_json()) for row in records)
        payloads = {row.evaluation_id: (row.canonical_json(), row.sha256) for row in records}
        counts = Counter(row.selection_status for row in records)
        matrices_by_name = dict(run.source_matrices)
        if (tuple(sorted(((row.evaluation_id, row.sha256) for row in records), key=lambda item: str(item[0]))) != run.record_sha256s
                or any(row.run_id != run.run_id or row.selected_at != run.selected_at or row.selector_sha256 != run.selector_sha256 for row in records)
            or any(counts[status] != count for status, count in run.selection_counts)
            or any(matrices_by_name.get(row.observation.underlyer if isinstance(row, (
                O1IndicatorEvaluationEvidence, O3CreditEvaluationEvidence, StockSetupIndicatorEvaluationEvidence, SurfaceEvaluationEvidence))
                else row.package.underlyer) != row.matrix_id for row in records)
                or sum(len(text.encode("ascii")) for text, _ in payloads.values()) > 67108864):
            raise ValueError("complete run records do not match frozen header")
        with self._cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute("SET LOCAL lock_timeout = '2s'")
            cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (f"detector-dataset:{run.dataset_id}",))
            cursor.execute("""SELECT publication_id,selector_sha256,selection_evidence,scheduled_cycle,published_at,
                    configuration_sha256,strategy_policy_sha256,source_matrix_ids,covered_underlying_count FROM option_board_publications
                WHERE publication_id=%s""", (run.run_id,))
            existing = cursor.fetchone()
            if existing:
                if self._read_run(existing) != run:
                    raise ValueError("complete run retry differs from retained evidence")
                self._persist_records(cursor, records, payloads, allow_insert=False)
                return dict(status="ALREADY_RECORDED", run_id=str(run.run_id), new_alerts=dict(run.selection_counts)["SELECTED"])
            cursor.execute("""SELECT scheduled_cycle,selector_sha256,created_at FROM option_board_publications
                WHERE selector_version IN ('option_detector_run_v1','option_detector_run_v2') AND selection_evidence->>'dataset_id'=%s
                ORDER BY scheduled_cycle DESC LIMIT 1""", (run.dataset_id,))
            latest = cursor.fetchone()
            if latest and (latest["scheduled_cycle"] >= run.scheduled_cycle or latest["selector_sha256"] != run.scope_sha256):
                raise ValueError("detector dataset cannot change scope or record an out-of-order cycle")
            if latest and latest["created_at"] > run.selected_at:
                raise ValueError("detector selection predates the previous run receipt; reselect")
            cursor.execute("""SELECT analysis.matrix_id,analysis.underlying,analysis.market_time,analysis.observed_time
                FROM option_analysis_runs AS analysis JOIN option_ingestion_runs AS ingestion USING(batch_id)
                JOIN option_work_items AS work ON work.subject_id=analysis.matrix_id::text AND work.stage='STRATEGY'
                  AND work.business_key='strategy:'||analysis.matrix_id::text||':'||%s
                WHERE analysis.matrix_id=ANY(%s::uuid[]) AND ingestion.scheduled_cycle=%s
                  AND ingestion.configuration_sha256=%s AND ingestion.policy_sha256=%s AND analysis.policy_sha256=%s
                  AND ingestion.status='COMPLETE' AND analysis.status='COMPLETE' AND work.status='COMPLETED'
                  AND ingestion.completed_at<=%s AND analysis.completed_at<=%s AND work.completed_at<=%s
                  AND analysis.created_at<=%s AND analysis.observed_time<=%s""",
                (run.strategy_version, [matrix for _, matrix in run.source_matrices], run.scheduled_cycle,
                 run.configuration_sha256, run.market_policy_sha256, run.market_policy_sha256,
                 run.selected_at, run.selected_at, run.selected_at, run.selected_at, run.selected_at))
            matrices = [dict(row) for row in cursor.fetchall()]
            if (tuple(sorted((row["underlying"], row["matrix_id"]) for row in matrices)) != run.source_matrices
                    or max(row["market_time"] for row in matrices) != run.market_time
                    or max(row["observed_time"] for row in matrices) != run.observed_time):
                raise ValueError("detector run source matrices are not exactly complete at selection")
            candidate_ids = {row.candidate_id for row in records if row.candidate_id is not None}
            if candidate_ids:
                cursor.execute("""SELECT candidate_id FROM option_strategy_candidates
                    WHERE candidate_id=ANY(%s::uuid[]) AND policy_sha256=%s AND status='SELECTED'
                      AND matrix_id=ANY(%s::uuid[])""", (list(candidate_ids), run.strategy_policy_sha256,
                    [matrix for _, matrix in run.source_matrices]))
                if {row["candidate_id"] for row in cursor.fetchall()} != candidate_ids:
                    raise ValueError("detector evaluation candidates differ from the run strategy scope")
            prior = self._prior_selected(cursor, run.dataset_id, run.scheduled_cycle, run.selected_at)
            for record in records:
                if isinstance(record, (O1IndicatorEvaluationEvidence, O3CreditEvaluationEvidence, StockSetupIndicatorEvaluationEvidence, SurfaceEvaluationEvidence)):
                    continue
                previous = prior.get(record.recurrence_sha256)
                if record.selection_status == "REPEAT":
                    if (previous is None or previous.candidate_id != record.first_candidate_id
                            or previous.exposure_sha256 != record.package.exposure_sha256
                            or previous.expires_at != record.package.expires_at or run.selected_at >= previous.expires_at):
                        raise ValueError("repeat does not reference an active original alert")
                elif previous is not None and run.selected_at < previous.expires_at:
                    raise ValueError("new selection became stale after another run; reselect with retained prior alerts")
            self._persist_records(cursor, records, payloads)
            evidence = dict(kind="DUAL_ORIGIN_COMPLETE_RUN", dataset_id=run.dataset_id,
                payload_text=run.canonical_json(), payload_sha256=run.sha256)
            cursor.execute("""INSERT INTO option_board_publications (
                publication_id,scheduled_cycle,as_of_session,status,selector_version,selector_sha256,
                strategy_policy_sha256,configuration_sha256,expected_underlying_count,covered_underlying_count,
                source_matrix_ids,market_data_time,observed_time,selection_evidence,published_at,created_at)
                VALUES (%s,%s,%s,'COMPLETE',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,clock_timestamp())""",
                (run.run_id, run.scheduled_cycle, run.scheduled_cycle.astimezone(ZoneInfo("America/New_York")).date(),
                 run.schema_version, run.scope_sha256, run.strategy_policy_sha256, run.configuration_sha256,
                 len(run.expected_underlyers), len(run.source_matrices), [matrix for _, matrix in run.source_matrices],
                 run.market_time, run.observed_time, Json(evidence), run.selected_at))
            deadlines = [row.package.entry_deadline for row in records
                if row.selection_status in ("SELECTED", "REPEAT") and not isinstance(row, O3CreditEvaluationEvidence)]
            if deadlines:
                cursor.execute("SELECT clock_timestamp()<%s AS timely", (min(deadlines),))
                if not cursor.fetchone()["timely"]:
                    raise ValueError("alert entry deadline elapsed during complete-run persistence")
            return dict(status="RECORDED", run_id=str(run.run_id), new_alerts=dict(run.selection_counts)["SELECTED"])

    def review_inputs(self, *, as_of, session_date=None, dataset_id=None):
        if as_of.utcoffset() is None:
            raise ValueError("evaluation read requires an aware cutoff")
        today = as_of.astimezone(ZoneInfo("America/New_York")).date()
        if session_date is not None and not today - timedelta(days=60) <= session_date <= today:
            raise ValueError("evaluation review is bounded to sixty days")
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute("SELECT to_regclass('option_detector_evaluations') IS NOT NULL AS ready")
            if not cursor.fetchone()["ready"]:
                return dict(ready=False, datasets=[], sessions=[], records=[], dataset_id=None, session_date=None)
            cursor.execute("""SELECT dataset_id,session_date FROM (
                SELECT dataset_id,session_date FROM option_detector_evaluations
                WHERE session_date>=%s AND session_date<=%s AND selected_at<=%s AND recorded_at<=%s
                UNION
                SELECT dataset_id,session_date FROM option_o1_indicator_observations
                WHERE session_date>=%s AND session_date<=%s AND selected_at<=%s AND recorded_at<=%s
                UNION
                SELECT dataset_id,session_date FROM option_o3_credit_observations
                WHERE session_date>=%s AND session_date<=%s AND selected_at<=%s AND recorded_at<=%s
                UNION
                SELECT dataset_id,session_date FROM option_stock_setup_indicator_observations
                WHERE session_date>=%s AND session_date<=%s AND selected_at<=%s AND recorded_at<=%s
                UNION
                SELECT selection_evidence->>'dataset_id',as_of_session FROM option_board_publications
                WHERE selector_version IN ('option_detector_run_v1','option_detector_run_v2') AND status='COMPLETE'
                  AND as_of_session>=%s AND as_of_session<=%s AND published_at<=%s AND created_at<=%s
                ) AS sessions ORDER BY dataset_id,session_date LIMIT 1001""",
                (today - timedelta(days=60), today, as_of, as_of,
                 today - timedelta(days=60), today, as_of, as_of,
                 today - timedelta(days=60), today, as_of, as_of,
                 today - timedelta(days=60), today, as_of, as_of,
                 today - timedelta(days=60), today, as_of, as_of))
            index = [dict(row) for row in cursor.fetchall()]
            if len(index) > 1000:
                raise ValueError("evaluation dataset index exceeds bound")
            datasets = sorted({row["dataset_id"] for row in index})
            chosen = dataset_id if dataset_id is not None else datasets[0] if len(datasets) == 1 else None
            sessions = sorted({row["session_date"] for row in index if row["dataset_id"] == chosen})
            selected_date = session_date or (sessions[-1] if sessions else None)
            if chosen is None or selected_date is None:
                return dict(ready=True, datasets=datasets, sessions=sessions, records=[], dataset_id=chosen, session_date=selected_date)
            cursor.execute("""SELECT evaluation_id,payload_text,payload_sha256 FROM option_detector_evaluations
                WHERE dataset_id=%s AND session_date=%s AND selected_at<=%s AND recorded_at<=%s
                UNION ALL
                SELECT evaluation_id,payload_text,payload_sha256 FROM option_o1_indicator_observations
                WHERE dataset_id=%s AND session_date=%s AND selected_at<=%s AND recorded_at<=%s
                UNION ALL
                SELECT evaluation_id,payload_text,payload_sha256 FROM option_o3_credit_observations
                WHERE dataset_id=%s AND session_date=%s AND selected_at<=%s AND recorded_at<=%s
                UNION ALL
                SELECT evaluation_id,payload_text,payload_sha256 FROM option_stock_setup_indicator_observations
                WHERE dataset_id=%s AND session_date=%s AND selected_at<=%s AND recorded_at<=%s
                ORDER BY evaluation_id LIMIT 10001""", (chosen, selected_date, as_of, as_of,
                    chosen, selected_date, as_of, as_of, chosen, selected_date, as_of, as_of,
                    chosen, selected_date, as_of, as_of))
            records, payload_bytes = [], 0
            for row in cursor.fetchall():
                payload_bytes += len(row["payload_text"].encode("ascii"))
                if len(records) >= 10000 or payload_bytes > 67108864:
                    raise ValueError("evaluation read exceeds row/payload bound")
                record = load_evaluation_evidence(row["payload_text"])
                if (record.evaluation_id != row["evaluation_id"] or record.sha256 != row["payload_sha256"]
                        or record.canonical_json() != row["payload_text"] or record.dataset_id != chosen
                        or record.scheduled_cycle.astimezone(ZoneInfo("America/New_York")).date() != selected_date
                        or record.selected_at > as_of):
                    raise ValueError("evaluation read identity/hash mismatch")
                records.append(record)
            cursor.execute("""SELECT publication_id,selector_sha256,selection_evidence,scheduled_cycle,published_at,
                    configuration_sha256,strategy_policy_sha256,source_matrix_ids,covered_underlying_count
                FROM option_board_publications WHERE selector_version IN ('option_detector_run_v1','option_detector_run_v2') AND status='COMPLETE'
                  AND selection_evidence->>'dataset_id'=%s AND as_of_session=%s
                  AND published_at<=%s AND created_at<=%s ORDER BY scheduled_cycle LIMIT 5001""",
                (chosen, selected_date, as_of, as_of))
            runs = []
            records_by_run = {}
            for record in records:
                records_by_run.setdefault(record.run_id, []).append((record.evaluation_id, record.sha256))
            for row in cursor.fetchall():
                payload_bytes += len(row["selection_evidence"]["payload_text"].encode("ascii"))
                if len(runs) >= 5000 or payload_bytes > 67108864:
                    raise ValueError("complete run read exceeds row/payload bound")
                run = self._read_run(row)
                if (run.dataset_id != chosen or run.selected_at > as_of
                        or run.scheduled_cycle.astimezone(ZoneInfo("America/New_York")).date() != selected_date
                        or tuple(sorted(records_by_run.get(run.run_id, []), key=lambda item: str(item[0]))) != run.record_sha256s):
                    raise ValueError("complete run read does not reconcile with evaluation rows")
                runs.append(run)
            return dict(ready=True, datasets=datasets, sessions=sessions, records=records,
                dataset_id=chosen, session_date=selected_date, runs=runs)