import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from auth.api import (
    ACCOUNT_UNAVAILABLE_REASON,
    SignInRequest,
    SignUpRequest,
    read_session,
    router,
    sign_in,
    sign_up,
)


def test_router_exposes_only_placeholder_account_routes():
    exposed = {(route.path, tuple(sorted(route.methods))) for route in router.routes}

    assert exposed == {
        ("/api/auth/session", ("GET",)),
        ("/api/auth/signin", ("POST",)),
        ("/api/auth/signup", ("POST",)),
    }


def test_credential_routes_declare_not_implemented():
    status_codes = {route.path: route.status_code for route in router.routes}

    assert status_codes["/api/auth/signin"] == 501
    assert status_codes["/api/auth/signup"] == 501
    assert status_codes["/api/auth/session"] is None


def test_session_reports_disabled_and_never_returns_a_user():
    envelope = read_session()

    assert envelope.available is False
    assert envelope.status == "DISABLED"
    assert envelope.user is None
    assert envelope.reason == ACCOUNT_UNAVAILABLE_REASON
    assert envelope.planned_capabilities


def test_submitted_credentials_are_absent_from_both_responses():
    password = "correct-horse-battery-staple"

    signin = sign_in(SignInRequest(email="Analyst@Example.com", password=password))
    signup = sign_up(
        SignUpRequest(email="Analyst@Example.com", display_name="Analyst", password=password)
    )

    for envelope in (signin, signup):
        serialized = envelope.model_dump_json()
        assert password not in serialized
        assert "analyst@example.com" not in serialized
        assert envelope.user is None


def test_email_is_normalized_and_malformed_addresses_are_rejected():
    assert SignInRequest(email="  Analyst@Example.COM ", password="x").email == "analyst@example.com"

    with pytest.raises(ValidationError):
        SignInRequest(email="not-an-email", password="x")


def test_unexpected_request_fields_are_rejected():
    with pytest.raises(ValidationError):
        SignUpRequest(email="analyst@example.com", display_name="Analyst", password="x", role="admin")


def test_password_is_not_serialized_by_the_request_model():
    registration = SignUpRequest(
        email="analyst@example.com", display_name="Analyst", password="a-real-secret-value"
    )

    assert "a-real-secret-value" not in str(registration)
    assert "a-real-secret-value" not in registration.model_dump_json()
    assert registration.password.get_secret_value() == "a-real-secret-value"
