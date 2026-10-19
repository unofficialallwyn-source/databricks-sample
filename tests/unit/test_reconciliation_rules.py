from decimal import Decimal
from pathlib import Path
import sys

import pytest

RECONCILIATION_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "trade_recon"
    / "reconciliation"
)
sys.path.insert(0, str(RECONCILIATION_PATH))

from rules import FIELD_RULES, validate_runtime_config


def test_rule_names_are_unique():
    break_types = [rule.break_type for rule in FIELD_RULES]
    assert len(break_types) == len(set(break_types))


def test_runtime_config_rejects_invalid_values():
    with pytest.raises(ValueError):
        validate_runtime_config(Decimal("-0.01"), 30)

    with pytest.raises(ValueError):
        validate_runtime_config(Decimal("0.01"), 0)


def test_runtime_config_accepts_zero_price_tolerance():
    validate_runtime_config(Decimal("0.00"), 30)
