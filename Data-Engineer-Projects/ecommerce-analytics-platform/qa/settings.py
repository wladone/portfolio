"""QA settings module."""

from datetime import datetime, timedelta

from pydantic import AnyUrl, BaseSettings


class QASettings(BaseSettings):
    """QA settings with pydantic validation."""

    # API settings
    api_base_url: AnyUrl = "http://localhost:8000"
    database_url: str = "postgresql://postgres:postgres@localhost:5432/ecommerce"

    # Load test settings
    concurrency: int = 16
    duration_seconds: int = 20
    warmup_seconds: int = 5

    # Test data settings
    user_id_probe: int = 1
    date_window_days: int = 30

    # Feature flags
    fail_fast: bool = False
    enable_stream_checks: bool = True
    enable_security_checks: bool = True

    # Report paths
    report_markdown_path: str = "qa_report.md"
    report_junit_path: str = "qa_report.xml"

    # Cache settings
    cache_warmup_requests: int = 10

    # Test windows
    @property
    def date_range(self):
        """Get date range for tests."""
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=self.date_window_days)
        return start_date, end_date

    class Config:
        """Pydantic config."""

        env_prefix = "QA_"
        case_sensitive = False
