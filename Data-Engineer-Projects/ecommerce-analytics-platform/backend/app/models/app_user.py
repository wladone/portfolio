"""Application user model for RBAC-secured endpoints."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, CheckConstraint, DateTime, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base

VALID_ROLES = ("admin", "analyst", "app")


class AppUser(Base):
    """Represents an authenticated application user stored in the meta schema."""

    __tablename__ = "app_user"
    __table_args__ = (
        CheckConstraint(
            "role IN ('admin','analyst','app')",
            name="ck_app_user_role",
        ),
        {"schema": "meta"},
    )

    user_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(150), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[str] = mapped_column(String(20), nullable=False)
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="true"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    def __repr__(self) -> str:
        """Return textual representation useful for debugging."""
        return f"AppUser(user_id={self.user_id!r}, username={self.username!r}, role={self.role!r})"
