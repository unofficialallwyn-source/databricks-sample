"""TR-016 synthetic generator tests.

Only the base-trade milestone is enabled for now.
Enable the remaining scenario tests incrementally as each capability is implemented.
"""

from datetime import date
from decimal import Decimal
from random import Random

import pytest

from src.trade_recon.synthetic.generator import generate_base_trade, generate_broker_events, generate_oms_events, write_oms_jsonl



def test_generate_base_trade_is_deterministic_for_same_seed() -> None:
    """Same seed, index, and business date should generate the same base trade."""
    business_date = date(2026, 9, 7)

    first = generate_base_trade(1, business_date, Random(12345))
    second = generate_base_trade(1, business_date, Random(12345))

    assert first == second


def test_generate_base_trade_is_source_neutral_and_valid() -> None:
    """Base trade should contain business economics, not OMS/Broker event metadata."""
    business_date = date(2026, 9, 7)

    trade = generate_base_trade(7, business_date, Random(12345))

    assert trade["business_trade_id"] == "TRD00000007"
    assert trade["trade_date"] == "2026-09-07"
    assert trade["settlement_date"] == "2026-09-09"
    assert trade["instrument_type"] == "EQUITY"
    assert trade["broker_id"] == "BROKER_A"
    assert trade["execution_timestamp"].endswith("+00:00")
    assert Decimal(trade["quantity"]) > 0
    assert Decimal(trade["price"]) > 0

    assert "event_id" not in trade
    assert "trade_version" not in trade
    assert "schema_version" not in trade

def test_oms_trade_and_broker_trade_is_exact_match() -> None:
    """OMS trade and Broker trade should contain business economics, and should exactly match."""
    business_date = date(2026, 9, 7)

    trade = generate_base_trade(7, business_date, Random(12345))
    oms_event = generate_oms_events(trade=trade,scenario=None,rng=Random(12345))
    broker_event = generate_broker_events(trade=trade,scenario=None,rng=Random(12345))

    assert oms_event[0].get("trade_id") == broker_event[0].get("client_trade_id")
    assert oms_event[0].get("instrument_id") == broker_event[0].get("instrument_code")
    assert oms_event[0].get("side") == broker_event[0].get("side")
    assert oms_event[0].get("quantity") == broker_event[0].get("quantity")
    assert oms_event[0].get("price") == broker_event[0].get("price")
    assert oms_event[0].get("currency") == broker_event[0].get("currency")
    assert oms_event[0].get("account_id") == broker_event[0].get("client_account")
    assert oms_event[0].get("broker_id") == broker_event[0].get("broker_id")
    assert oms_event[0].get("trade_version") == 1
    assert broker_event[0].get("confirmation_version") == 1
    assert oms_event[0].get("published_at") >= oms_event[0].get("event_time")
    assert broker_event[0].get("published_at") >= broker_event[0].get("confirmation_time")


def test_write_oms_jsonl_creates_expected_files():
    business_date = date(2026, 9, 7)
    trade1 = generate_base_trade(1, business_date, Random(1986))
    trade2 = generate_base_trade(2, business_date, Random(7433))
    trade3 = generate_base_trade(3, business_date, Random(5860))
    oms_event1 = generate_oms_events(trade=trade1,scenario=None,rng=Random(1986))
    oms_event2 = generate_oms_events(trade=trade2,scenario=None,rng=Random(7433))
    oms_event3 = generate_oms_events(trade=trade3,scenario=None,rng=Random(5860))
    oms_events = []
    oms_events.append(oms_event1)
    oms_events.append(oms_event2)
    oms_events.append(oms_event3)
    
    output_folder = "/workspaces/databricks-sample/oms_jsonl_trades"
    file_paths = write_oms_jsonl(output_path=output_folder,events=oms_events,records_per_file=2)

    assert len(file_paths) == 2


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
