"""TR-016 synthetic generator tests.

Only the base-trade milestone is enabled for now.
Enable the remaining scenario tests incrementally as each capability is implemented.
"""

from datetime import date
from decimal import Decimal
from random import Random

import pytest

from src.trade_recon.synthetic.generator import generate_base_trade


def test_generate_base_trade_is_deterministic_for_same_seed() -> None:
    """Same seed, index, and business date should generate the same base trade."""
    business_date = date(2026, 9, 7)

    first = generate_base_trade(1, business_date, Random(12345))
    second = generate_base_trade(1, business_date, Random(12345))

    assert first == second


def test_generate_base_trade_follows_basic_oms_contract() -> None:
    """Base trade should use deterministic IDs and contract-compatible field formats."""
    business_date = date(2026, 9, 7)

    trade = generate_base_trade(7, business_date, Random(12345))

    assert trade["event_id"] == "OMS-EVT-00000007-V001"
    assert trade["trade_id"] == "TRD00000007"
    assert trade["trade_version"] == 1
    assert trade["event_type"] == "NEW"
    assert trade["schema_version"] == "1.0"
    assert trade["trade_date"] == "2026-09-07"
    assert trade["settlement_date"] == "2026-09-09"
    assert trade["instrument_type"] == "EQUITY"
    assert trade["broker_id"] == "BROKER_A"
    assert trade["event_time"].endswith("+00:00")
    assert trade["execution_timestamp"].endswith("+00:00")
    assert trade["published_at"].endswith("+00:00")

    assert Decimal(trade["quantity"]) > 0
    assert Decimal(trade["price"]) > 0


@pytest.mark.skip(reason="Implement TR-016 dataset orchestration.")
def test_same_seed_produces_same_logical_dataset() -> None:
    """Same seed/config/business date should generate identical logical data."""
    pytest.fail("Implement deterministic generation test")


@pytest.mark.skip(reason="Implement TR-016 dataset orchestration.")
def test_different_seed_changes_generated_dataset() -> None:
    """Different seeds should produce a different logical dataset."""
    pytest.fail("Implement seed variation test")


@pytest.mark.skip(reason="Implement S-001 EXACT_MATCH.")
def test_exact_match_scenario_produces_matching_economics() -> None:
    """EXACT_MATCH should generate OMS and Broker records with matching economics."""
    pytest.fail("Implement EXACT_MATCH scenario test")


@pytest.mark.skip(reason="Implement S-002 PRICE_MISMATCH.")
def test_price_mismatch_exceeds_configured_tolerance() -> None:
    """PRICE_MISMATCH should exceed the configured reconciliation tolerance."""
    pytest.fail("Implement PRICE_MISMATCH scenario test")


@pytest.mark.skip(reason="Implement S-003 PRICE_WITHIN_TOLERANCE.")
def test_price_within_tolerance_stays_within_boundary() -> None:
    """PRICE_WITHIN_TOLERANCE should remain inside the configured tolerance."""
    pytest.fail("Implement PRICE_WITHIN_TOLERANCE scenario test")


@pytest.mark.skip(reason="Implement S-007 LATE_CONFIRMATION.")
def test_late_confirmation_uses_later_delivery_phase() -> None:
    """LATE_CONFIRMATION should place Broker delivery after the OMS/SLA phase."""
    pytest.fail("Implement staged delivery test")


@pytest.mark.skip(reason="Implement S-013 OUT_OF_ORDER_OMS.")
def test_out_of_order_oms_delivers_v2_before_v1() -> None:
    """OUT_OF_ORDER_OMS should preserve source versions but reverse delivery order."""
    pytest.fail("Implement out-of-order OMS test")


@pytest.mark.skip(reason="Implement duplicate-event scenarios.")
def test_duplicate_event_reuses_event_id_and_payload() -> None:
    """Duplicate redelivery must reuse the event ID with identical payload."""
    pytest.fail("Implement duplicate event test")


@pytest.mark.skip(reason="Implement conflicting-event scenarios.")
def test_conflicting_redelivery_reuses_id_but_changes_payload() -> None:
    """Conflicting redelivery must reuse the event ID while altering payload content."""
    pytest.fail("Implement conflicting event test")


@pytest.mark.skip(reason="Implement S-011 OMS_AMENDMENT.")
def test_oms_amendment_uses_source_local_versions() -> None:
    """OMS_AMENDMENT should generate increasing OMS versions without implying Broker version parity."""
    pytest.fail("Implement OMS amendment test")


@pytest.mark.skip(reason="Implement S-031 regression scenario.")
def test_oms_version_after_missing_break_has_staged_inputs() -> None:
    """S-031 should produce OMS v1, missing phase, OMS v2, then late Broker confirmation."""
    pytest.fail("Implement S-031 regression-input test")


@pytest.mark.skip(reason="Implement generation manifest.")
def test_manifest_counts_match_generated_source_events() -> None:
    """Manifest source-event counts should match generated OMS/Broker outputs."""
    pytest.fail("Implement manifest accounting test")


@pytest.mark.skip(reason="Implement generator self-validation.")
def test_generator_validation_rejects_internal_inconsistency() -> None:
    """Generator self-validation should fail when scenario invariants are violated."""
    pytest.fail("Implement generator validation test")
