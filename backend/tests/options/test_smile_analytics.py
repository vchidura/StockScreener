import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import numpy as np
import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.analytics.smile import (
    SmileInput,
    coefficient_payload,
    fit_smile_groups,
    qualifying_distortions,
)
from options.domain import ContractType

EXPIRATION = date(2026, 10, 2)
SPOT = Decimal("104")


def _chain(iv_by_strike, contract_type=ContractType.CALL, expiration=EXPIRATION, spot=SPOT):
    return tuple(
        SmileInput(
            contract_id=index,
            contract_type=contract_type,
            expiration_date=expiration,
            strike=Decimal(str(strike)),
            spot=spot,
            local_iv=iv,
        )
        for index, (strike, iv) in enumerate(sorted(iv_by_strike.items()), start=1)
    )


def _smooth(strikes, spot=104.0, bump=None):
    bump = bump or {}
    values = {}
    for strike in strikes:
        x = np.log(strike / spot)
        values[strike] = 0.28 + 0.35 * x * x - 0.15 * x + bump.get(strike, 0.0)
    return values


def test_group_below_minimum_strikes_is_not_fitted():
    rows = _chain(_smooth([100, 102, 104, 106, 108, 110]))
    assert fit_smile_groups(rows, minimum_strikes=7) == ()


def test_group_without_both_sides_of_spot_is_not_fitted():
    rows = _chain(_smooth([106, 108, 110, 112, 114, 116, 118]))
    assert fit_smile_groups(rows, minimum_strikes=7) == ()


def test_numerically_exact_fit_is_skipped_not_scored():
    # Residuals collapse to ~1e-16 here; scaling by that MAD manufactured robust
    # scores above the 2.5 gate from a smile with no distortion at all.
    rows = _chain(_smooth([96, 98, 100, 102, 104, 106, 108, 110, 112]))
    assert fit_smile_groups(rows, minimum_strikes=7) == ()


def test_mad_floor_is_far_below_real_dispersion_and_above_float_noise():
    from options.analytics.smile import MINIMUM_RESIDUAL_MAD

    assert MINIMUM_RESIDUAL_MAD > 1e-15
    values = _smooth(
        [96, 98, 100, 102, 104, 106, 108, 110, 112], bump={104: 0.02, 106: 0.018}
    )
    fit = fit_smile_groups(_chain(values), minimum_strikes=7)[0]
    assert fit.mad > MINIMUM_RESIDUAL_MAD * 1000


def test_fit_reports_group_geometry():
    values = _smooth(
        [96, 98, 100, 102, 104, 106, 108, 110, 112], bump={104: 0.02, 106: 0.018}
    )
    fits = fit_smile_groups(_chain(values), minimum_strikes=7)
    assert len(fits) == 1
    fit = fits[0]
    assert fit.expiration_date == EXPIRATION
    assert fit.contract_type is ContractType.CALL
    assert fit.input_count == 9
    assert fit.distinct_strike_count == 9
    assert len(fit.residuals) == 9
    assert fit.mad > 0


def test_edges_are_marked_and_never_qualify():
    values = _smooth(
        [96, 98, 100, 102, 104, 106, 108, 110, 112], bump={96: 0.05, 98: 0.05}
    )
    fit = fit_smile_groups(_chain(values), minimum_strikes=7)[0]
    assert fit.residuals[0].is_edge is True
    assert fit.residuals[-1].is_edge is True
    assert all(not entry.is_edge for entry in fit.residuals[1:-1])
    assert all(
        not entry.is_edge for entry in qualifying_distortions(fit, minimum_absolute_robust_z=0.1)
    )


def test_isolated_spike_fails_neighbour_consistency():
    strikes = [96, 98, 100, 102, 104, 106, 108, 110, 112]
    fit = fit_smile_groups(
        _chain(_smooth(strikes, bump={104: 0.06})), minimum_strikes=7
    )[0]
    spike = next(entry for entry in fit.residuals if entry.strike == Decimal("104"))
    assert abs(spike.robust_z) > 2.5
    assert spike.neighbors_consistent is False
    assert spike not in qualifying_distortions(fit, minimum_absolute_robust_z=2.5)


def test_adjacent_same_sign_region_qualifies():
    strikes = [96 + 2 * i for i in range(13)]
    fit = fit_smile_groups(
        _chain(_smooth(strikes, bump={104: 0.02, 106: 0.02})), minimum_strikes=7
    )[0]
    qualifying = qualifying_distortions(fit, minimum_absolute_robust_z=2.5)
    assert {entry.strike for entry in qualifying} >= {Decimal("104"), Decimal("106")}
    assert all(entry.neighbors_consistent for entry in qualifying)


def test_robust_z_scales_with_group_strike_count():
    # The same distortion scores far higher in a wider group because the
    # unweighted fit and MAD are less contaminated by the outliers.
    scores = {}
    for count in (9, 13, 21):
        strikes = [104 - 2 * (count // 2) + 2 * i for i in range(count)]
        fit = fit_smile_groups(
            _chain(_smooth(strikes, bump={104: 0.02, 106: 0.02})), minimum_strikes=7
        )[0]
        scores[count] = max(abs(entry.robust_z) for entry in fit.residuals)
    assert scores[9] < scores[13] < scores[21]
    assert scores[21] > 2 * scores[9]


def test_calls_and_puts_are_fitted_separately():
    values = _smooth(
        [96, 98, 100, 102, 104, 106, 108, 110, 112], bump={104: 0.02, 106: 0.018}
    )
    rows = _chain(values) + tuple(
        SmileInput(
            contract_id=100 + index,
            contract_type=ContractType.PUT,
            expiration_date=EXPIRATION,
            strike=Decimal(str(strike)),
            spot=SPOT,
            local_iv=iv,
        )
        for index, (strike, iv) in enumerate(sorted(values.items()), start=1)
    )
    fits = fit_smile_groups(rows, minimum_strikes=7)
    assert {fit.contract_type for fit in fits} == {ContractType.CALL, ContractType.PUT}
    assert len(fits) == 2


def test_residual_equals_observed_minus_fitted():
    values = _smooth(
        [96, 98, 100, 102, 104, 106, 108, 110, 112], bump={104: 0.02, 106: 0.018}
    )
    fit = fit_smile_groups(_chain(values), minimum_strikes=7)[0]
    for entry in fit.residuals:
        assert entry.residual == pytest.approx(entry.local_iv - entry.fitted_iv)


def test_coefficient_payload_is_named_and_ordered():
    assert coefficient_payload((1.5, -0.25, 0.3)) == {
        "quadratic": 1.5,
        "linear": -0.25,
        "intercept": 0.3,
    }


def test_threshold_monotonically_reduces_qualifying_rows():
    strikes = [96 + 2 * i for i in range(13)]
    fit = fit_smile_groups(
        _chain(_smooth(strikes, bump={104: 0.02, 106: 0.02})), minimum_strikes=7
    )[0]
    counts = [
        len(qualifying_distortions(fit, minimum_absolute_robust_z=threshold))
        for threshold in (0.5, 1.5, 2.5, 5.0)
    ]
    assert counts == sorted(counts, reverse=True)
