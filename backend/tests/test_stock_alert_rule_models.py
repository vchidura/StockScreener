"""Nov 10: the Stock Alerts model filter matches research rules, primary or also matched."""
from research.stock_alerts import alert_page, alert_rule_models


def _alert(alert_id, model, also=()):
    return {
        "alert_id": alert_id, "run_id": "r1", "security_id": f"s-{alert_id}", "ticker": alert_id.upper(),
        "company_name": None, "direction": 1, "model": model, "interval": "1h", "status": "OPEN", "lane": "TRADE",
        "triggered_at": "2026-11-09T15:00:00+00:00", "published_at": "2026-11-09T15:00:05+00:00",
        "also_matched": [{"rule": rule} for rule in also],
    }


def _snapshot():
    return {
        "source": "SHADOW", "source_label": "Multi-model shadow", "sessions": ["2026-11-09"],
        "publications": [{"run_id": "r1", "session": "2026-11-09", "published_at": "2026-11-09T15:00:05+00:00"}],
        "alerts": [
            _alert("nvda", "B3_1h_structure", also=("SW3_B",)),
            _alert("msft", "R2_30m_structure"),
            _alert("old", "discovery"),
        ],
        "observations": [],
    }


def test_rule_models_include_also_matched():
    assert alert_rule_models(_alert("x", "B3_1h_structure", also=("SW3_B",))) == {"B3_1h_structure", "SW3_B"}


def test_model_filter_matches_primary_or_also_matched_rule():
    primary = alert_page(_snapshot(), model="B3_1h_structure")
    also = alert_page(_snapshot(), model="SW3_B")
    assert [row["ticker"] for row in primary["rows"]] == ["NVDA"]
    assert [row["ticker"] for row in also["rows"]] == ["NVDA"]
    assert alert_page(_snapshot(), model="resumption")["total"] == 0


def test_page_lists_the_models_the_alerts_carry():
    assert alert_page(_snapshot())["available_models"] == [
        "B3_1h_structure", "R2_30m_structure", "SW3_B", "discovery",
    ]
