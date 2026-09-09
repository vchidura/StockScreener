"""Placeholder account-access endpoints.

No user store, credential verification, session, or token issuance exists yet. These routes
exist so the portal can exercise the real contract shape and fail explicitly instead of
implying that account access works. Request bodies are validated for shape only and are never
stored, logged, or compared against anything.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

router = APIRouter(prefix="/api/auth", tags=["account-access"])

ACCOUNT_STATUS = "DISABLED"
ACCOUNT_UNAVAILABLE_REASON = (
    "Account access is not available yet. Sign-in, sign-up, watchlists, and alerts are "
    "planned but not implemented."
)
PLANNED_CAPABILITIES = (
    "Per-user credential storage with salted password hashing",
    "Short-lived access tokens with rotating refresh cookies",
    "Per-user watchlists",
    "Per-user alerts",
)
_EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s.]+(\.[^@\s.]+)+$")


class AccountEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    available: bool = False
    status: Literal["DISABLED"] = ACCOUNT_STATUS
    reason: str = ACCOUNT_UNAVAILABLE_REASON
    generated_at: datetime
    planned_capabilities: list[str] = Field(default_factory=lambda: list(PLANNED_CAPABILITIES))
    user: None = None


class _Credentials(BaseModel):
    model_config = ConfigDict(extra="forbid")

    email: str = Field(min_length=3, max_length=254)
    # Deliberately unconstrained: a rejected constraint would place the secret in a 422 error
    # body. Strength rules belong with credential storage, which does not exist yet.
    password: SecretStr

    @field_validator("email")
    @classmethod
    def _normalized_email(cls, value: str) -> str:
        normalized = value.strip().lower()
        if not _EMAIL_PATTERN.match(normalized):
            raise ValueError("email must be a valid address")
        return normalized


class SignInRequest(_Credentials):
    pass


class SignUpRequest(_Credentials):
    display_name: str = Field(min_length=1, max_length=80)


def _envelope() -> AccountEnvelope:
    return AccountEnvelope(generated_at=datetime.now(timezone.utc))


@router.get("/session", response_model=AccountEnvelope)
def read_session() -> AccountEnvelope:
    """Report the account subsystem state. No cookie, header, or token is read."""
    return _envelope()


@router.post("/signin", response_model=AccountEnvelope, status_code=501)
def sign_in(credentials: SignInRequest) -> AccountEnvelope:
    _ = credentials
    return _envelope()


@router.post("/signup", response_model=AccountEnvelope, status_code=501)
def sign_up(registration: SignUpRequest) -> AccountEnvelope:
    _ = registration
    return _envelope()
