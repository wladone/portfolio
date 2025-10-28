from sqlalchemy import create_engine, text

engine = create_engine("postgresql://app:app_password@localhost:5432/ecom")
with engine.connect() as conn:
    conn.execute(
        text("ALTER TABLE alembic_version ALTER COLUMN version_num TYPE varchar(128)")
    )
    conn.commit()
