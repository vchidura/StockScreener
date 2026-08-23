"""Map SIC codes to the eleven GICS-style sector buckets.

Polygon supplies SIC codes; GICS itself is licensed by S&P/MSCI and is not available here.
This mapping reproduces the eleven sector names but is derived from SIC, so a minority of
names land differently than S&P assigns them. Ranges are checked in order, so the specific
four-digit entries must precede the broad major-group fallbacks.
"""
from __future__ import annotations

SECTORS = (
    "IT - Software & Services",
    "IT - Semiconductors & Hardware",
    "Financials",
    "Health Care",
    "Consumer Discretionary",
    "Communication Services",
    "Industrials",
    "Consumer Staples",
    "Energy",
    "Utilities",
    "Materials",
    "Real Estate",
)

# Information Technology is split because semiconductors are capex-cyclical while software is
# subscription-secular; demeaning one against the other removes the wrong common factor.
IT_SOFTWARE = "IT - Software & Services"
IT_HARDWARE = "IT - Semiconductors & Hardware"

ETF_SECTOR = "ETF"
UNCLASSIFIED_SECTOR = "Unclassified"

# Foreign private issuers file 20-F rather than 10-K, so EDGAR assigns them no SIC code and
# Polygon returns none. Assigned by hand; verify the company name before adding a row.
MANUAL_SECTORS: dict[str, str] = {
    "AEM": "Materials",               # Agnico Eagle Mines
    "AZN": "Health Care",             # AstraZeneca
    "B": "Materials",                 # Barrick Mining
    "NBIS": IT_SOFTWARE,              # Nebius Group
    "NU": "Financials",               # Nu Holdings
    "SPOT": "Communication Services",  # Spotify Technology
    "STM": IT_HARDWARE,               # STMicroelectronics
    "TSEM": IT_HARDWARE,              # Tower Semiconductor
}

# (low, high, sector) inclusive on both ends, evaluated top to bottom.
_SIC_RANGES: tuple[tuple[int, int, str], ...] = (
    # Health care carve-outs from chemicals, instruments and research services.
    (2833, 2836, "Health Care"),
    (3826, 3826, "Health Care"),
    (3841, 3851, "Health Care"),
    (5047, 5047, "Health Care"),
    (5122, 5122, "Health Care"),
    (8000, 8099, "Health Care"),
    (8731, 8731, "Health Care"),

    # Information technology carve-outs from machinery and electronics.
    (3570, 3579, IT_HARDWARE),
    (3661, 3669, IT_HARDWARE),
    (3670, 3679, IT_HARDWARE),
    (3690, 3699, IT_HARDWARE),
    (3820, 3829, IT_HARDWARE),
    (7370, 7379, IT_SOFTWARE),

    # Communication services.
    (2710, 2796, "Communication Services"),
    (4810, 4899, "Communication Services"),
    (7310, 7319, "Communication Services"),
    (7810, 7841, "Communication Services"),

    # Energy.
    (1200, 1299, "Energy"),
    (1300, 1399, "Energy"),
    (2900, 2999, "Energy"),
    (4610, 4619, "Energy"),
    (5171, 5172, "Energy"),

    # Utilities.
    (4900, 4939, "Utilities"),
    (4941, 4941, "Utilities"),
    (4991, 4991, "Utilities"),

    # Real estate.
    (6500, 6599, "Real Estate"),
    (6798, 6798, "Real Estate"),

    # Financials.
    (6000, 6499, "Financials"),
    (6700, 6799, "Financials"),

    # Consumer staples.
    (100, 999, "Consumer Staples"),
    (2000, 2199, "Consumer Staples"),
    (2840, 2844, "Consumer Staples"),
    (5140, 5149, "Consumer Staples"),
    (5400, 5499, "Consumer Staples"),
    (5912, 5912, "Consumer Staples"),

    # Materials.
    (1000, 1119, "Materials"),
    (1400, 1499, "Materials"),
    (2200, 2299, "Materials"),
    (2400, 2499, "Materials"),
    (2600, 2699, "Materials"),
    (2800, 2899, "Materials"),
    (3000, 3099, "Materials"),
    (3200, 3399, "Materials"),

    # Consumer discretionary.
    (2300, 2399, "Consumer Discretionary"),
    (2500, 2599, "Consumer Discretionary"),
    (3100, 3199, "Consumer Discretionary"),
    (3711, 3716, "Consumer Discretionary"),
    (3751, 3751, "Consumer Discretionary"),
    (3900, 3999, "Consumer Discretionary"),
    (5200, 5399, "Consumer Discretionary"),
    (5500, 5911, "Consumer Discretionary"),
    (5913, 5999, "Consumer Discretionary"),
    (7000, 7299, "Consumer Discretionary"),
    (7500, 7699, "Consumer Discretionary"),
    (7900, 7999, "Consumer Discretionary"),
    (8200, 8299, "Consumer Discretionary"),

    # Industrials absorbs the remaining manufacturing, transport and services groups.
    (1500, 1799, "Industrials"),
    (3400, 3569, "Industrials"),
    (3580, 3660, "Industrials"),
    (3680, 3689, "Industrials"),
    (3700, 3710, "Industrials"),
    (3720, 3750, "Industrials"),
    (3752, 3799, "Industrials"),
    (3800, 3819, "Industrials"),
    (3830, 3840, "Industrials"),
    (3852, 3899, "Industrials"),
    (4000, 4599, "Industrials"),
    (4620, 4789, "Industrials"),
    (4950, 4989, "Industrials"),
    (5000, 5139, "Industrials"),
    (5150, 5170, "Industrials"),
    (5173, 5199, "Industrials"),
    (7320, 7369, "Industrials"),
    (7380, 7499, "Industrials"),
    (8700, 8730, "Industrials"),
    (8732, 8799, "Industrials"),
)


def sector_for_sic(sic_code) -> str | None:
    """Return a GICS-style sector name, or None when the code is missing or unmappable."""
    try:
        code = int(str(sic_code).strip())
    except (TypeError, ValueError):
        return None
    for low, high, sector in _SIC_RANGES:
        if low <= code <= high:
            return sector
    return None
