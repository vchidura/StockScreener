-- Generated 2026-10-12 from the verified v1 final schema (pg_dump 17.11) by
-- backend/scripts/_generate_v2_kept_baseline.py. These tables keep their v1 definition.
-- Edit by hand only before the V2 freeze point.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';


CREATE TRIGGER trg_ref_corporate_action_coverage_immutable BEFORE DELETE OR UPDATE ON ref.corporate_action_coverage FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_ref_corporate_action_coverage_no_truncate BEFORE TRUNCATE ON ref.corporate_action_coverage FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY ref.corporate_action_coverage
    ADD CONSTRAINT corporate_action_coverage_security_id_fkey FOREIGN KEY (security_id) REFERENCES ref.securities(security_id);

CREATE INDEX idx_ref_corporate_action_security_date ON ref.corporate_action_revisions USING btree (security_id, effective_date, observed_at DESC);

CREATE TRIGGER trg_ref_corporate_action_revisions_immutable BEFORE DELETE OR UPDATE ON ref.corporate_action_revisions FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_ref_corporate_action_revisions_no_truncate BEFORE TRUNCATE ON ref.corporate_action_revisions FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY ref.corporate_action_revisions
    ADD CONSTRAINT corporate_action_revisions_security_id_fkey FOREIGN KEY (security_id) REFERENCES ref.securities(security_id);

ALTER TABLE ONLY ref.corporate_action_revisions
    ADD CONSTRAINT corporate_action_revisions_supersedes_action_id_fkey FOREIGN KEY (supersedes_action_id) REFERENCES ref.corporate_action_revisions(action_id);

CREATE INDEX idx_ref_fundamental_coverage_asof ON ref.fundamental_report_coverage USING btree (security_id, checked_at DESC);

CREATE TRIGGER trg_ref_fundamental_coverage_immutable BEFORE DELETE OR UPDATE ON ref.fundamental_report_coverage FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_ref_fundamental_coverage_no_truncate BEFORE TRUNCATE ON ref.fundamental_report_coverage FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY ref.fundamental_report_coverage
    ADD CONSTRAINT fundamental_report_coverage_security_id_fkey FOREIGN KEY (security_id) REFERENCES ref.securities(security_id);

CREATE INDEX idx_ref_fundamental_reports_asof ON ref.fundamental_reports USING btree (security_id, availability_time DESC, observed_at DESC);

CREATE TRIGGER trg_ref_fundamental_reports_immutable BEFORE DELETE OR UPDATE ON ref.fundamental_reports FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_ref_fundamental_reports_no_truncate BEFORE TRUNCATE ON ref.fundamental_reports FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY ref.fundamental_reports
    ADD CONSTRAINT fundamental_reports_security_id_fkey FOREIGN KEY (security_id) REFERENCES ref.securities(security_id);

ALTER TABLE ONLY ref.fundamental_reports
    ADD CONSTRAINT fundamental_reports_security_revision_id_fkey FOREIGN KEY (security_revision_id) REFERENCES ref.security_revisions(revision_id);

CREATE TRIGGER trg_ref_market_event_coverage_immutable BEFORE DELETE OR UPDATE ON ref.market_event_coverage FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_ref_market_event_coverage_no_truncate BEFORE TRUNCATE ON ref.market_event_coverage FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY ref.market_event_coverage
    ADD CONSTRAINT market_event_coverage_security_id_fkey FOREIGN KEY (security_id) REFERENCES ref.securities(security_id);

CREATE INDEX idx_ref_market_event_security_date ON ref.market_event_revisions USING btree (security_id, event_date, observed_at DESC);

CREATE TRIGGER trg_ref_market_event_revisions_immutable BEFORE DELETE OR UPDATE ON ref.market_event_revisions FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_ref_market_event_revisions_no_truncate BEFORE TRUNCATE ON ref.market_event_revisions FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY ref.market_event_revisions
    ADD CONSTRAINT market_event_revisions_security_id_fkey FOREIGN KEY (security_id) REFERENCES ref.securities(security_id);

ALTER TABLE ONLY ref.market_event_revisions
    ADD CONSTRAINT market_event_revisions_supersedes_revision_id_fkey FOREIGN KEY (supersedes_revision_id) REFERENCES ref.market_event_revisions(revision_id);

CREATE INDEX idx_ref_oi_baseline_contract_session ON ref.oi_baselines USING btree (contract_id, settlement_session DESC, observed_at DESC);

CREATE TRIGGER trg_ref_oi_baselines_immutable BEFORE DELETE OR UPDATE ON ref.oi_baselines FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_ref_oi_baselines_no_truncate BEFORE TRUNCATE ON ref.oi_baselines FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY ref.oi_baselines
    ADD CONSTRAINT oi_baselines_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES ref.option_contracts(contract_id);

ALTER TABLE ONLY ref.oi_baselines
    ADD CONSTRAINT oi_baselines_supersedes_baseline_id_fkey FOREIGN KEY (supersedes_baseline_id) REFERENCES ref.oi_baselines(baseline_id);

CREATE INDEX idx_ref_option_contract_version_expiry ON ref.option_contract_versions USING btree (expiration, contract_id) WHERE ((effective_to IS NULL) AND ((eligibility)::text = 'VALIDATED_ACTIVE'::text));

CREATE UNIQUE INDEX uq_ref_option_contract_current_version ON ref.option_contract_versions USING btree (contract_id) WHERE (effective_to IS NULL);

CREATE TRIGGER trg_ref_option_contract_versions_immutable BEFORE DELETE OR UPDATE ON ref.option_contract_versions FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_ref_option_contract_versions_no_truncate BEFORE TRUNCATE ON ref.option_contract_versions FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY ref.option_contract_versions
    ADD CONSTRAINT option_contract_versions_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES ref.option_contracts(contract_id);

CREATE INDEX idx_ref_option_contract_underlying ON ref.option_contracts USING btree (underlying_security_id, contract_id);

CREATE TRIGGER trg_ref_option_contracts_immutable BEFORE DELETE OR UPDATE ON ref.option_contracts FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_ref_option_contracts_no_truncate BEFORE TRUNCATE ON ref.option_contracts FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY ref.option_contracts
    ADD CONSTRAINT option_contracts_underlying_security_id_fkey FOREIGN KEY (underlying_security_id) REFERENCES ref.securities(security_id);

CREATE INDEX idx_ref_rate_curve_asof ON ref.risk_free_rate_observations USING btree (source, rate_date DESC, received_at DESC, tenor_days);

CREATE TRIGGER trg_ref_risk_free_rate_observations_immutable BEFORE DELETE OR UPDATE ON ref.risk_free_rate_observations FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_ref_risk_free_rate_observations_no_truncate BEFORE TRUNCATE ON ref.risk_free_rate_observations FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_ref_securities_immutable BEFORE DELETE OR UPDATE ON ref.securities FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_ref_securities_no_truncate BEFORE TRUNCATE ON ref.securities FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

CREATE INDEX idx_ref_security_revision_ticker ON ref.security_revisions USING btree (ticker, effective_from DESC);

CREATE UNIQUE INDEX uq_ref_security_current_revision ON ref.security_revisions USING btree (security_id) WHERE (effective_to IS NULL);

CREATE TRIGGER trg_ref_security_revisions_immutable BEFORE DELETE OR UPDATE ON ref.security_revisions FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_ref_security_revisions_no_truncate BEFORE TRUNCATE ON ref.security_revisions FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY ref.security_revisions
    ADD CONSTRAINT security_revisions_security_id_fkey FOREIGN KEY (security_id) REFERENCES ref.securities(security_id);

CREATE INDEX idx_rt_alert_current_kind_time ON rt.alert_current USING btree (cohort_id, alert_kind, as_of DESC, alert_id);

CREATE INDEX idx_rt_alert_current_stock_open ON rt.alert_current USING btree (cohort_id, as_of DESC, alert_id) WHERE (((alert_kind)::text = 'STOCK'::text) AND ((lifecycle)::text = 'OPEN'::text));

ALTER TABLE ONLY rt.alert_current
    ADD CONSTRAINT alert_current_cohort_id_fkey FOREIGN KEY (cohort_id) REFERENCES rt.cohorts(cohort_id);

ALTER TABLE ONLY rt.alert_current
    ADD CONSTRAINT alert_current_event_id_fkey FOREIGN KEY (event_id) REFERENCES rt.detector_events(event_id);

CREATE TRIGGER trg_rt_alert_management_assessments_immutable BEFORE DELETE OR UPDATE ON rt.alert_management_assessments FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_alert_management_assessments_no_truncate BEFORE TRUNCATE ON rt.alert_management_assessments FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.alert_management_assessments
    ADD CONSTRAINT alert_management_assessments_cohort_id_fkey FOREIGN KEY (cohort_id) REFERENCES rt.cohorts(cohort_id);

ALTER TABLE ONLY rt.alert_management_current
    ADD CONSTRAINT alert_management_current_assessment_id_fkey FOREIGN KEY (assessment_id) REFERENCES rt.alert_management_assessments(assessment_id);

CREATE INDEX idx_rt_alert_outbox_pending ON rt.alert_outbox USING btree (cohort_id, next_attempt_at, outbox_id) WHERE ((delivered_at IS NULL) AND (abandoned_at IS NULL));

ALTER TABLE ONLY rt.alert_outbox
    ADD CONSTRAINT alert_outbox_cohort_id_fkey FOREIGN KEY (cohort_id) REFERENCES rt.cohorts(cohort_id);

ALTER TABLE ONLY rt.alert_outbox
    ADD CONSTRAINT alert_outbox_event_id_fkey FOREIGN KEY (event_id) REFERENCES rt.detector_events(event_id);

CREATE INDEX idx_rt_archive_segment_retention ON rt.archive_segments USING btree (journal_generation, shard_id, last_offset);

CREATE TRIGGER trg_rt_archive_segments_immutable BEFORE DELETE OR UPDATE ON rt.archive_segments FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_archive_segments_no_truncate BEFORE TRUNCATE ON rt.archive_segments FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_capability_checks_immutable BEFORE DELETE OR UPDATE ON rt.capability_checks FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.capability_checks
    ADD CONSTRAINT capability_checks_profile_id_fkey FOREIGN KEY (profile_id) REFERENCES rt.capability_profiles(profile_id);

CREATE INDEX idx_rt_capability_profile_lookup ON rt.capability_profiles USING btree (account_alias, universe_sha256, checked_at DESC);

CREATE TRIGGER trg_rt_capability_profiles_immutable BEFORE DELETE OR UPDATE ON rt.capability_profiles FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_cohorts_immutable BEFORE DELETE OR UPDATE ON rt.cohorts FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.cohorts
    ADD CONSTRAINT cohorts_manifest_id_fkey FOREIGN KEY (manifest_id) REFERENCES rt.subscription_manifests(manifest_id);

CREATE INDEX idx_rt_consumer_checkpoint_retention ON rt.consumer_checkpoints USING btree (journal_generation, shard_id, committed_offset);

ALTER TABLE ONLY rt.consumer_checkpoints
    ADD CONSTRAINT consumer_checkpoints_cohort_id_fkey FOREIGN KEY (cohort_id) REFERENCES rt.cohorts(cohort_id);

CREATE INDEX idx_rt_detector_dataset_latest ON rt.detector_datasets USING btree (cohort_id, started_at DESC, dataset_id);

CREATE INDEX idx_rt_detector_dataset_session ON rt.detector_datasets USING btree (cohort_id, session_date DESC, started_at DESC);

CREATE TRIGGER trg_rt_detector_datasets_immutable BEFORE DELETE OR UPDATE ON rt.detector_datasets FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_detector_datasets_no_truncate BEFORE TRUNCATE ON rt.detector_datasets FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.detector_datasets
    ADD CONSTRAINT detector_datasets_cohort_id_fkey FOREIGN KEY (cohort_id) REFERENCES rt.cohorts(cohort_id);

CREATE INDEX idx_rt_detector_evaluation_filter ON rt.detector_evaluations USING btree (dataset_id, detector_id, underlyer, observed_at DESC, evaluation_id);

CREATE INDEX idx_rt_detector_evaluation_time ON rt.detector_evaluations USING btree (dataset_id, observed_at DESC, evaluation_id);

CREATE TRIGGER trg_rt_detector_evaluations_immutable BEFORE DELETE OR UPDATE ON rt.detector_evaluations FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_detector_evaluations_no_truncate BEFORE TRUNCATE ON rt.detector_evaluations FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.detector_evaluations
    ADD CONSTRAINT detector_evaluations_dataset_id_fkey FOREIGN KEY (dataset_id) REFERENCES rt.detector_datasets(dataset_id);

ALTER TABLE ONLY rt.detector_evaluations
    ADD CONSTRAINT detector_evaluations_event_id_fkey FOREIGN KEY (event_id) REFERENCES rt.detector_events(event_id);

CREATE INDEX idx_rt_detector_event_detector_time ON rt.detector_events USING btree (cohort_id, detector_sha256, decision_at DESC);

CREATE INDEX idx_rt_detector_event_instrument_time ON rt.detector_events USING btree (cohort_id, instrument_id, decision_at DESC);

CREATE TRIGGER trg_rt_detector_events_immutable BEFORE DELETE OR UPDATE ON rt.detector_events FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_detector_events_no_truncate BEFORE TRUNCATE ON rt.detector_events FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.detector_events
    ADD CONSTRAINT detector_events_cohort_id_fkey FOREIGN KEY (cohort_id) REFERENCES rt.cohorts(cohort_id);

ALTER TABLE ONLY rt.detector_events
    ADD CONSTRAINT detector_events_detector_sha256_fkey FOREIGN KEY (detector_sha256) REFERENCES rt.detector_versions(detector_sha256);

ALTER TABLE ONLY rt.detector_events
    ADD CONSTRAINT detector_events_instrument_id_fkey FOREIGN KEY (instrument_id) REFERENCES rt.instruments(instrument_id);

ALTER TABLE ONLY rt.detector_state
    ADD CONSTRAINT detector_state_cohort_id_fkey FOREIGN KEY (cohort_id) REFERENCES rt.cohorts(cohort_id);

ALTER TABLE ONLY rt.detector_state
    ADD CONSTRAINT detector_state_detector_sha256_fkey FOREIGN KEY (detector_sha256) REFERENCES rt.detector_versions(detector_sha256);

CREATE TRIGGER trg_rt_detector_versions_immutable BEFORE DELETE OR UPDATE ON rt.detector_versions FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_detector_versions_no_truncate BEFORE TRUNCATE ON rt.detector_versions FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_feature_definitions_immutable BEFORE DELETE OR UPDATE ON rt.feature_definitions FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_feature_definitions_no_truncate BEFORE TRUNCATE ON rt.feature_definitions FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

CREATE UNIQUE INDEX uq_rt_instrument_contract ON rt.instruments USING btree (contract_id) WHERE (contract_id IS NOT NULL);

CREATE UNIQUE INDEX uq_rt_instrument_security ON rt.instruments USING btree (security_id) WHERE (security_id IS NOT NULL);

-- An instrument identity is never changed or removed; app holds UPDATE on every writable table, so the
-- trigger is what keeps this table append-only (writers insert with ON CONFLICT DO NOTHING).
CREATE TRIGGER trg_rt_instruments_immutable BEFORE DELETE OR UPDATE ON rt.instruments FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_instruments_no_truncate BEFORE TRUNCATE ON rt.instruments FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.instruments
    ADD CONSTRAINT instruments_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES ref.option_contracts(contract_id);

ALTER TABLE ONLY rt.instruments
    ADD CONSTRAINT instruments_security_id_fkey FOREIGN KEY (security_id) REFERENCES ref.securities(security_id);

CREATE TRIGGER trg_rt_local_mock_dispositions_immutable BEFORE DELETE OR UPDATE ON rt.local_mock_dispositions FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_local_mock_dispositions_no_truncate BEFORE TRUNCATE ON rt.local_mock_dispositions FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.local_mock_dispositions
    ADD CONSTRAINT local_mock_dispositions_request_id_fkey FOREIGN KEY (request_id) REFERENCES rt.local_mock_requests(request_id);

CREATE TRIGGER trg_rt_local_mock_fills_immutable BEFORE DELETE OR UPDATE ON rt.local_mock_fills FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_local_mock_fills_no_truncate BEFORE TRUNCATE ON rt.local_mock_fills FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.local_mock_fills
    ADD CONSTRAINT local_mock_fills_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES ref.option_contracts(contract_id);

ALTER TABLE ONLY rt.local_mock_fills
    ADD CONSTRAINT local_mock_fills_instrument_id_fkey FOREIGN KEY (instrument_id) REFERENCES rt.instruments(instrument_id);

ALTER TABLE ONLY rt.local_mock_fills
    ADD CONSTRAINT local_mock_fills_request_id_fkey FOREIGN KEY (request_id) REFERENCES rt.local_mock_requests(request_id);

ALTER TABLE ONLY rt.local_mock_fills
    ADD CONSTRAINT local_mock_fills_request_id_source_evidence_id_fkey FOREIGN KEY (request_id, source_evidence_id) REFERENCES rt.local_mock_request_inputs(request_id, evidence_id);

ALTER TABLE ONLY rt.local_mock_fills
    ADD CONSTRAINT local_mock_fills_source_evidence_id_fkey FOREIGN KEY (source_evidence_id) REFERENCES rt.option_mark_evidence(evidence_id);

CREATE TRIGGER trg_rt_local_mock_request_inputs_immutable BEFORE DELETE OR UPDATE ON rt.local_mock_request_inputs FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_local_mock_request_inputs_no_truncate BEFORE TRUNCATE ON rt.local_mock_request_inputs FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.local_mock_request_inputs
    ADD CONSTRAINT local_mock_request_inputs_evidence_id_fkey FOREIGN KEY (evidence_id) REFERENCES rt.option_mark_evidence(evidence_id);

ALTER TABLE ONLY rt.local_mock_request_inputs
    ADD CONSTRAINT local_mock_request_inputs_request_id_fkey FOREIGN KEY (request_id) REFERENCES rt.local_mock_requests(request_id);

CREATE INDEX idx_rt_local_mock_request_alert ON rt.local_mock_requests USING btree (alert_id, requested_at DESC, request_id);

CREATE INDEX idx_rt_local_mock_request_candidate ON rt.local_mock_requests USING btree (candidate_id, requested_at DESC, request_id);

CREATE TRIGGER trg_rt_local_mock_requests_immutable BEFORE DELETE OR UPDATE ON rt.local_mock_requests FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_local_mock_requests_no_truncate BEFORE TRUNCATE ON rt.local_mock_requests FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.local_mock_requests
    ADD CONSTRAINT local_mock_requests_alert_id_fkey FOREIGN KEY (alert_id) REFERENCES rt.alert_current(alert_id);

ALTER TABLE ONLY rt.local_mock_requests
    ADD CONSTRAINT local_mock_requests_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES rt.option_packages(candidate_id);

ALTER TABLE ONLY rt.local_mock_requests
    ADD CONSTRAINT local_mock_requests_cohort_id_fkey FOREIGN KEY (cohort_id) REFERENCES rt.cohorts(cohort_id);

CREATE INDEX idx_rt_option_discovery_underlying_cycle ON rt.option_discovery_batches USING btree (underlying_security_id, scheduled_cycle DESC, discovery_batch_id);

CREATE TRIGGER trg_rt_option_discovery_batches_immutable BEFORE DELETE OR UPDATE ON rt.option_discovery_batches FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_option_discovery_batches_no_truncate BEFORE TRUNCATE ON rt.option_discovery_batches FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.option_discovery_batches
    ADD CONSTRAINT option_discovery_batches_profile_id_fkey FOREIGN KEY (profile_id) REFERENCES rt.capability_profiles(profile_id);

ALTER TABLE ONLY rt.option_discovery_batches
    ADD CONSTRAINT option_discovery_batches_underlying_security_id_fkey FOREIGN KEY (underlying_security_id) REFERENCES ref.securities(security_id);

CREATE TRIGGER trg_rt_option_discovery_coverage_immutable BEFORE DELETE OR UPDATE ON rt.option_discovery_coverage FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_option_discovery_coverage_no_truncate BEFORE TRUNCATE ON rt.option_discovery_coverage FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.option_discovery_coverage
    ADD CONSTRAINT option_discovery_coverage_discovery_batch_id_fkey FOREIGN KEY (discovery_batch_id) REFERENCES rt.option_discovery_batches(discovery_batch_id);

CREATE INDEX idx_rt_option_manifest_disposition_underlying ON rt.option_manifest_contract_dispositions USING btree (selection_id, underlyer, expiration_date, call_put, strike);

CREATE TRIGGER trg_rt_option_manifest_contract_dispositions_immutable BEFORE DELETE OR UPDATE ON rt.option_manifest_contract_dispositions FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_option_manifest_contract_dispositions_no_truncate BEFORE TRUNCATE ON rt.option_manifest_contract_dispositions FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.option_manifest_contract_dispositions
    ADD CONSTRAINT option_manifest_contract_dispositions_catalog_version_id_fkey FOREIGN KEY (catalog_version_id) REFERENCES ref.option_contract_versions(version_id);

ALTER TABLE ONLY rt.option_manifest_contract_dispositions
    ADD CONSTRAINT option_manifest_contract_dispositions_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES ref.option_contracts(contract_id);

ALTER TABLE ONLY rt.option_manifest_contract_dispositions
    ADD CONSTRAINT option_manifest_contract_dispositions_selection_id_fkey FOREIGN KEY (selection_id) REFERENCES rt.option_manifest_selection_audits(selection_id);

CREATE TRIGGER trg_rt_option_manifest_coverage_immutable BEFORE DELETE OR UPDATE ON rt.option_manifest_coverage FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_option_manifest_coverage_no_truncate BEFORE TRUNCATE ON rt.option_manifest_coverage FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.option_manifest_coverage
    ADD CONSTRAINT option_manifest_coverage_selection_id_fkey FOREIGN KEY (selection_id) REFERENCES rt.option_manifest_selection_audits(selection_id);

ALTER TABLE ONLY rt.option_manifest_coverage
    ADD CONSTRAINT option_manifest_coverage_underlying_security_id_fkey FOREIGN KEY (underlying_security_id) REFERENCES ref.securities(security_id);

CREATE INDEX idx_rt_option_manifest_selection_session ON rt.option_manifest_selection_audits USING btree (session_date DESC, provider_tier, selection_id);

CREATE TRIGGER trg_rt_option_manifest_selection_audits_immutable BEFORE DELETE OR UPDATE ON rt.option_manifest_selection_audits FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_option_manifest_selection_audits_no_truncate BEFORE TRUNCATE ON rt.option_manifest_selection_audits FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.option_manifest_selection_audits
    ADD CONSTRAINT option_manifest_selection_audits_manifest_id_fkey FOREIGN KEY (manifest_id) REFERENCES rt.subscription_manifests(manifest_id);

ALTER TABLE ONLY rt.option_manifest_selection_audits
    ADD CONSTRAINT option_manifest_selection_audits_profile_id_fkey FOREIGN KEY (profile_id) REFERENCES rt.capability_profiles(profile_id);

CREATE INDEX idx_rt_option_mark_evidence_instrument_time ON rt.option_mark_evidence USING btree (instrument_id, market_time DESC, evidence_id);

CREATE UNIQUE INDEX uq_rt_option_mark_evidence_advanced ON rt.option_mark_evidence USING btree (journal_generation, source_offset) WHERE ((source_kind)::text = 'ADVANCED_NBBO'::text);

CREATE UNIQUE INDEX uq_rt_option_mark_evidence_developer ON rt.option_mark_evidence USING btree (source_batch_id, source_revision_id) WHERE ((source_kind)::text = 'DEVELOPER_ALIGNED_AGGREGATE'::text);

CREATE TRIGGER trg_rt_option_mark_evidence_immutable BEFORE DELETE OR UPDATE ON rt.option_mark_evidence FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_option_mark_evidence_no_truncate BEFORE TRUNCATE ON rt.option_mark_evidence FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.option_mark_evidence
    ADD CONSTRAINT option_mark_evidence_catalog_version_id_fkey FOREIGN KEY (catalog_version_id) REFERENCES ref.option_contract_versions(version_id);

ALTER TABLE ONLY rt.option_mark_evidence
    ADD CONSTRAINT option_mark_evidence_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES ref.option_contracts(contract_id);

ALTER TABLE ONLY rt.option_mark_evidence
    ADD CONSTRAINT option_mark_evidence_instrument_id_fkey FOREIGN KEY (instrument_id) REFERENCES rt.instruments(instrument_id);

ALTER TABLE ONLY rt.option_mark_evidence
    ADD CONSTRAINT option_mark_evidence_stream_session_id_fkey FOREIGN KEY (stream_session_id) REFERENCES rt.stream_sessions(session_id);

CREATE TRIGGER trg_rt_option_package_legs_immutable BEFORE DELETE OR UPDATE ON rt.option_package_legs FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_option_package_legs_no_truncate BEFORE TRUNCATE ON rt.option_package_legs FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.option_package_legs
    ADD CONSTRAINT option_package_legs_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES rt.option_packages(candidate_id);

ALTER TABLE ONLY rt.option_package_legs
    ADD CONSTRAINT option_package_legs_catalog_version_id_fkey FOREIGN KEY (catalog_version_id) REFERENCES ref.option_contract_versions(version_id);

ALTER TABLE ONLY rt.option_package_legs
    ADD CONSTRAINT option_package_legs_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES ref.option_contracts(contract_id);

ALTER TABLE ONLY rt.option_package_legs
    ADD CONSTRAINT option_package_legs_instrument_id_fkey FOREIGN KEY (instrument_id) REFERENCES rt.instruments(instrument_id);

ALTER TABLE ONLY rt.option_package_mark_current
    ADD CONSTRAINT option_package_mark_current_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES rt.option_packages(candidate_id);

ALTER TABLE ONLY rt.option_package_mark_current
    ADD CONSTRAINT option_package_mark_current_mark_id_fkey FOREIGN KEY (mark_id) REFERENCES rt.option_package_mark_revisions(mark_id);

CREATE TRIGGER trg_rt_option_package_mark_inputs_immutable BEFORE DELETE OR UPDATE ON rt.option_package_mark_inputs FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_option_package_mark_inputs_no_truncate BEFORE TRUNCATE ON rt.option_package_mark_inputs FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.option_package_mark_inputs
    ADD CONSTRAINT option_package_mark_inputs_evidence_id_fkey FOREIGN KEY (evidence_id) REFERENCES rt.option_mark_evidence(evidence_id);

ALTER TABLE ONLY rt.option_package_mark_inputs
    ADD CONSTRAINT option_package_mark_inputs_mark_id_fkey FOREIGN KEY (mark_id) REFERENCES rt.option_package_mark_revisions(mark_id);

CREATE INDEX idx_rt_option_package_mark_revision_candidate_time ON rt.option_package_mark_revisions USING btree (candidate_id, valuation_policy_sha256, market_time DESC, observed_time DESC);

CREATE TRIGGER trg_rt_option_package_mark_revisions_immutable BEFORE DELETE OR UPDATE ON rt.option_package_mark_revisions FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_option_package_mark_revisions_no_truncate BEFORE TRUNCATE ON rt.option_package_mark_revisions FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.option_package_mark_revisions
    ADD CONSTRAINT option_package_mark_revisions_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES rt.option_packages(candidate_id);

ALTER TABLE ONLY rt.option_package_mark_revisions
    ADD CONSTRAINT option_package_mark_revisions_candidate_id_provider_tier_fkey FOREIGN KEY (candidate_id, provider_tier) REFERENCES rt.option_packages(candidate_id, provider_tier);

CREATE TRIGGER trg_rt_option_packages_immutable BEFORE DELETE OR UPDATE ON rt.option_packages FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_option_packages_no_truncate BEFORE TRUNCATE ON rt.option_packages FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.option_packages
    ADD CONSTRAINT option_packages_cohort_id_fkey FOREIGN KEY (cohort_id) REFERENCES rt.cohorts(cohort_id);

ALTER TABLE ONLY rt.option_packages
    ADD CONSTRAINT option_packages_cohort_id_provider_tier_fkey FOREIGN KEY (cohort_id, provider_tier) REFERENCES rt.provider_tier_cohorts(cohort_id, provider_tier);

ALTER TABLE ONLY rt.option_pre_session_account_usage
    ADD CONSTRAINT option_pre_session_account_usage_run_id_fkey FOREIGN KEY (run_id) REFERENCES rt.option_pre_session_runs(run_id);

CREATE TRIGGER trg_rt_option_pre_session_finalizations_immutable BEFORE DELETE OR UPDATE ON rt.option_pre_session_finalizations FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_option_pre_session_finalizations_no_truncate BEFORE TRUNCATE ON rt.option_pre_session_finalizations FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.option_pre_session_finalizations
    ADD CONSTRAINT option_pre_session_finalizations_run_id_fkey FOREIGN KEY (run_id) REFERENCES rt.option_pre_session_runs(run_id);

ALTER TABLE ONLY rt.option_pre_session_finalizations
    ADD CONSTRAINT option_pre_session_finalizations_selection_id_fkey FOREIGN KEY (selection_id) REFERENCES rt.option_manifest_selection_audits(selection_id);

CREATE TRIGGER trg_rt_option_pre_session_packages_immutable BEFORE DELETE OR UPDATE ON rt.option_pre_session_packages FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_option_pre_session_packages_no_truncate BEFORE TRUNCATE ON rt.option_pre_session_packages FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.option_pre_session_packages
    ADD CONSTRAINT option_pre_session_packages_run_id_underlyer_fkey FOREIGN KEY (run_id, underlyer) REFERENCES rt.option_pre_session_work(run_id, underlyer);

ALTER TABLE ONLY rt.option_pre_session_packages
    ADD CONSTRAINT option_pre_session_packages_security_id_fkey FOREIGN KEY (security_id) REFERENCES ref.securities(security_id);

CREATE TRIGGER trg_rt_option_pre_session_runs_immutable BEFORE DELETE OR UPDATE ON rt.option_pre_session_runs FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_option_pre_session_runs_no_truncate BEFORE TRUNCATE ON rt.option_pre_session_runs FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.option_pre_session_runs
    ADD CONSTRAINT option_pre_session_runs_profile_id_fkey FOREIGN KEY (profile_id) REFERENCES rt.capability_profiles(profile_id);

CREATE INDEX idx_rt_option_pre_session_work_pending ON rt.option_pre_session_work USING btree (run_id, status, underlyer) WHERE ((status)::text = ANY ((ARRAY['PENDING'::character varying, 'CLAIMED'::character varying])::text[]));

ALTER TABLE ONLY rt.option_pre_session_work
    ADD CONSTRAINT option_pre_session_work_run_id_fkey FOREIGN KEY (run_id) REFERENCES rt.option_pre_session_runs(run_id);

ALTER TABLE ONLY rt.option_pre_session_work
    ADD CONSTRAINT option_pre_session_work_security_id_fkey FOREIGN KEY (security_id) REFERENCES ref.securities(security_id);

ALTER TABLE ONLY rt.portal_cohort_current
    ADD CONSTRAINT portal_cohort_current_activation_id_fkey FOREIGN KEY (activation_id) REFERENCES rt.provider_tier_activations(activation_id);

ALTER TABLE ONLY rt.portal_cohort_current
    ADD CONSTRAINT portal_cohort_current_cohort_id_provider_tier_fkey FOREIGN KEY (cohort_id, provider_tier) REFERENCES rt.provider_tier_cohorts(cohort_id, provider_tier);

CREATE TRIGGER trg_rt_provider_tier_activations_immutable BEFORE DELETE OR UPDATE ON rt.provider_tier_activations FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_provider_tier_activations_no_truncate BEFORE TRUNCATE ON rt.provider_tier_activations FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.provider_tier_activations
    ADD CONSTRAINT provider_tier_activations_previous_cohort_id_fkey FOREIGN KEY (previous_cohort_id) REFERENCES rt.provider_tier_cohorts(cohort_id);

ALTER TABLE ONLY rt.provider_tier_activations
    ADD CONSTRAINT provider_tier_activations_target_cohort_id_fkey FOREIGN KEY (target_cohort_id) REFERENCES rt.provider_tier_cohorts(cohort_id);

CREATE TRIGGER trg_rt_provider_tier_cohorts_immutable BEFORE DELETE OR UPDATE ON rt.provider_tier_cohorts FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_provider_tier_cohorts_no_truncate BEFORE TRUNCATE ON rt.provider_tier_cohorts FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.provider_tier_cohorts
    ADD CONSTRAINT provider_tier_cohorts_cohort_id_fkey FOREIGN KEY (cohort_id) REFERENCES rt.cohorts(cohort_id);

ALTER TABLE ONLY rt.provider_tier_cohorts
    ADD CONSTRAINT provider_tier_cohorts_profile_id_fkey FOREIGN KEY (profile_id) REFERENCES rt.capability_profiles(profile_id);

CREATE INDEX idx_rt_provider_tier_transition_time ON rt.provider_tier_transitions USING btree (transitioned_at DESC, transition_id);

CREATE TRIGGER trg_rt_provider_tier_transitions_immutable BEFORE DELETE OR UPDATE ON rt.provider_tier_transitions FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_provider_tier_transitions_no_truncate BEFORE TRUNCATE ON rt.provider_tier_transitions FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.provider_tier_transitions
    ADD CONSTRAINT provider_tier_transitions_from_profile_id_fkey FOREIGN KEY (from_profile_id) REFERENCES rt.capability_profiles(profile_id);

ALTER TABLE ONLY rt.provider_tier_transitions
    ADD CONSTRAINT provider_tier_transitions_to_profile_id_fkey FOREIGN KEY (to_profile_id) REFERENCES rt.capability_profiles(profile_id);

ALTER TABLE ONLY rt.quote_current
    ADD CONSTRAINT quote_current_instrument_id_fkey FOREIGN KEY (instrument_id) REFERENCES rt.instruments(instrument_id);

ALTER TABLE ONLY rt.quote_current
    ADD CONSTRAINT quote_current_session_id_fkey FOREIGN KEY (session_id) REFERENCES rt.stream_sessions(session_id);

ALTER TABLE ONLY rt.readiness_current
    ADD CONSTRAINT readiness_current_profile_id_fkey FOREIGN KEY (profile_id) REFERENCES rt.capability_profiles(profile_id);

CREATE INDEX idx_rt_readiness_transition_scope ON rt.readiness_transitions USING btree (scope_type, scope_id, capability, transitioned_at DESC);

CREATE TRIGGER trg_rt_readiness_transitions_immutable BEFORE DELETE OR UPDATE ON rt.readiness_transitions FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.readiness_transitions
    ADD CONSTRAINT readiness_transitions_profile_id_fkey FOREIGN KEY (profile_id) REFERENCES rt.capability_profiles(profile_id);

ALTER TABLE ONLY rt.screening_current
    ADD CONSTRAINT screening_current_generation_id_fkey FOREIGN KEY (generation_id) REFERENCES rt.screening_generations(generation_id);

CREATE TRIGGER trg_rt_screening_generations_immutable BEFORE DELETE OR UPDATE ON rt.screening_generations FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_screening_generations_no_truncate BEFORE TRUNCATE ON rt.screening_generations FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.screening_generations
    ADD CONSTRAINT screening_generations_cohort_id_fkey FOREIGN KEY (cohort_id) REFERENCES rt.cohorts(cohort_id);

CREATE INDEX idx_rt_screening_row_ticker ON rt.screening_rows USING btree (generation_id, ticker);

CREATE TRIGGER trg_rt_screening_rows_immutable BEFORE DELETE OR UPDATE ON rt.screening_rows FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_screening_rows_no_truncate BEFORE TRUNCATE ON rt.screening_rows FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.screening_rows
    ADD CONSTRAINT screening_rows_generation_id_fkey FOREIGN KEY (generation_id) REFERENCES rt.screening_generations(generation_id);

ALTER TABLE ONLY rt.screening_rows
    ADD CONSTRAINT screening_rows_security_id_fkey FOREIGN KEY (security_id) REFERENCES ref.securities(security_id);

ALTER TABLE ONLY rt.seed_current
    ADD CONSTRAINT seed_current_seed_id_fkey FOREIGN KEY (seed_id) REFERENCES rt.seed_manifests(seed_id);

CREATE TRIGGER trg_rt_seed_manifest_members_immutable BEFORE DELETE OR UPDATE ON rt.seed_manifest_members FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_seed_manifest_members_no_truncate BEFORE TRUNCATE ON rt.seed_manifest_members FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE ONLY rt.seed_manifest_members
    ADD CONSTRAINT seed_manifest_members_seed_id_fkey FOREIGN KEY (seed_id) REFERENCES rt.seed_manifests(seed_id);

CREATE INDEX idx_rt_seed_manifest_session ON rt.seed_manifests USING btree (session_date DESC, effective_at DESC);

CREATE TRIGGER trg_rt_seed_manifests_immutable BEFORE DELETE OR UPDATE ON rt.seed_manifests FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_seed_manifests_no_truncate BEFORE TRUNCATE ON rt.seed_manifests FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

CREATE INDEX idx_rt_stream_gap_instrument ON rt.stream_gaps USING btree (instrument_id, gap_start DESC) WHERE (instrument_id IS NOT NULL);

CREATE INDEX idx_rt_unresolved_stream_gap ON rt.stream_gaps USING btree (session_id, gap_start, gap_id) WHERE ((recovery_status)::text = ANY ((ARRAY['UNRESOLVED'::character varying, 'WARMING'::character varying])::text[]));

ALTER TABLE ONLY rt.stream_gaps
    ADD CONSTRAINT stream_gaps_instrument_id_fkey FOREIGN KEY (instrument_id) REFERENCES rt.instruments(instrument_id);

ALTER TABLE ONLY rt.stream_gaps
    ADD CONSTRAINT stream_gaps_session_id_fkey FOREIGN KEY (session_id) REFERENCES rt.stream_sessions(session_id);

CREATE INDEX idx_rt_stream_session_manifest ON rt.stream_sessions USING btree (manifest_id, started_at DESC);

CREATE UNIQUE INDEX uq_rt_active_stream_owner ON rt.stream_sessions USING btree (provider, feed) WHERE ((ended_at IS NULL) AND ((status)::text <> ALL ((ARRAY['STOPPED'::character varying, 'FAILED'::character varying])::text[])));

ALTER TABLE ONLY rt.stream_sessions
    ADD CONSTRAINT stream_sessions_manifest_id_fkey FOREIGN KEY (manifest_id) REFERENCES rt.subscription_manifests(manifest_id);

ALTER TABLE ONLY rt.trade_current
    ADD CONSTRAINT trade_current_instrument_id_fkey FOREIGN KEY (instrument_id) REFERENCES rt.instruments(instrument_id);

ALTER TABLE ONLY rt.trade_current
    ADD CONSTRAINT trade_current_session_id_fkey FOREIGN KEY (session_id) REFERENCES rt.stream_sessions(session_id);
