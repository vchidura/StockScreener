from __future__ import annotations


NON_COMPANY_SECURITY_TYPES = frozenset({
    "ETF", "ETN", "ETV", "ETS", "FUND", "MF", "MONEY_MARKET",
})


def is_earnings_applicable_security_type(security_type: str | None) -> bool:
    return not security_type or security_type.upper() not in NON_COMPANY_SECURITY_TYPES
