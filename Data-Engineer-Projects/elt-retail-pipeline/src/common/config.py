from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    mysql_host: str = os.getenv("MYSQL_HOST", "localhost")
    mysql_port: int = int(os.getenv("MYSQL_PORT", "3306"))
    mysql_db: str = os.getenv("MYSQL_DB", "magazin_db")
    mysql_user: str = os.getenv("MYSQL_USER", "user")
    mysql_password: str = os.getenv("MYSQL_PASSWORD", "password")

    staging_dir: str = os.getenv("STAGING_DIR", "staging")

    gcp_project_id: str = os.getenv("GCP_PROJECT_ID", "")
    bq_dataset: str = os.getenv("BQ_DATASET", "retail_db")
    gcs_temp_bucket: str = os.getenv("GCS_TEMP_BUCKET", "")

    google_creds: str = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")


cfg = Config()

