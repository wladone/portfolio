# CHANGES_SUMMARY.md
- **Security & Config:** Composer DAGs (`streaming_direct_dag.py`, `streaming_flex_dag.py`) now ingest configuration via shared helpers that prioritise environment variables/Airflow Variables but default to `config.py`, keeping orchestration in lockstep with central constants and avoiding drift.
- **Observability & Monitoring:** Added a reusable Cloud Monitoring dashboard (`monitoring/dashboard_ecommerce_pipeline.json`) and alert policy (`monitoring/alert_dead_letter_policy.json`) plus `scripts/deploy_monitoring.sh`/`make deploy-monitoring` to publish them; README documents enablement alongside Beam metric export requirements.
- **Operability:** Introduced `composer/dags/streaming_smoke_dag.py`, an hourly smoke check that validates recent BigQuery windows and Bigtable schema, giving an integration-style guardrail without needing full end-to-end tests.
- **Docs & Tooling:** README now highlights the smoke DAG, monitoring deployment steps, and the new Makefile target for monitoring assets.

## Next Steps
- Parameterise Composer Variables/Connections for secrets (service accounts, notification channels) and feed them into the new DAG helpers.
- Extend the monitoring suite with notification channel wiring and budget alerts (gcloud monitoring channels/policies update).
- Layer on Dataflow-based integration tests or a pytest `@mark.integration` suite that exercises the pipeline against emulated Pub/Sub/BigQuery for CI gating.
