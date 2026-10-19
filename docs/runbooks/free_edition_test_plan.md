# Free Edition reconciliation test plan

This runbook is for functional validation before the Azure deployment phase. The data is synthetic and can be regenerated at any time.

## Baseline

Start small. Keep the default 30-minute missing-side SLA and 0.01 price tolerance unless the scenario explicitly tests those values.

Example multi-broker dataset:

```python
from trade_recon.synthetic.generator import generate_dataset

generate_dataset({
    "seed": 42,
    "business_date": "2026-10-19",
    "scenario_ids": ["S-001", "S-002", "S-003", "S-004", "S-005"],
    "trade_count": 25,
    "broker_ids": ["BROKER_A", "BROKER_B", "BROKER_C"],
    "output_path": "/Volumes/workspace/landing/synthetic_trade_files/recon_test_01",
})
```

Load the generated batch folders through the existing Bronze/Silver pipelines, then run `trade_reconciliation_mvp`.

## Core scenarios

| Scenario | Expected outcome |
| --- | --- |
| Exact match | `MATCHED`, no break types |
| Price difference = 0.01 | `MATCHED` |
| Price difference > 0.01 | `BREAK / PRICE_MISMATCH` |
| Quantity mismatch | `BREAK / QUANTITY_MISMATCH` |
| Price + quantity mismatch | both break types retained |
| Broker mismatch | `BREAK / BROKER_MISMATCH` |
| Account mismatch | `BREAK / ACCOUNT_MISMATCH` |
| OMS cancel, Broker active | `BREAK / OMS_CANCEL_BROKER_ACTIVE` |
| Broker cancel, OMS active | `BREAK / BROKER_CANCEL_OMS_ACTIVE` |
| Both cancelled | `MATCHED` |

## Missing-side SLA

Run the missing-counterparty scenarios in stages.

1. Process the first batch and run reconciliation before the SLA expires. The case must be `PENDING`.
2. Re-run after the SLA boundary. The case must become `BREAK` with `MISSING_CONFIRMATION` or `MISSING_TRADE`.
3. Deliver the late counterparty batch. The missing-side break must disappear from `break_active` and a `RESOLVED` event must be appended to `break_history`.

For a quick functional test, invoke `job.py` with `--evaluation-time` rather than waiting in real time.

## Multi-broker validation

Generate at least 30 trades across `BROKER_A`, `BROKER_B`, and `BROKER_C`.

Check that:

- each broker lands under its own `broker_*` directory;
- Broker Silver sets `source_system` from `broker_id`;
- canonical current state contains all brokers;
- candidate matching does not collapse trades from different brokers;
- Gold metrics contain separate broker rows;
- deliberately changing one Broker B confirmation to Broker C produces `BROKER_MISMATCH` only for that trade.

## Duplicate and missing-key validation

Exercise `S-010`, `S-019`, and `S-020`.

The candidate builder must retain the ambiguity instead of selecting an arbitrary row. Expected break types are `MATCH_KEY_MISSING`, `DUPLICATE_INTERNAL_CANDIDATE`, and `DUPLICATE_EXTERNAL_CANDIDATE`.

## Regression scenario S-031

Process the scenario in delivery order:

1. OMS V1 arrives.
2. SLA expires and `MISSING_CONFIRMATION` opens.
3. OMS V2 arrives. The existing missing-confirmation episode remains open; a second episode must not be created.
4. Broker confirmation arrives with the latest economics.

Expected final state: `MATCHED`. `break_history` must contain one OPENED and one RESOLVED event for the missing-confirmation episode.

## Evidence to capture

For the project walkthrough, keep screenshots or query results for:

- a matched trade;
- a multi-field break;
- pending -> missing break -> auto-resolution;
- one duplicate-candidate case;
- one cancellation case;
- three brokers in the same run;
- Gold match-rate and break-type summaries.

Do not use production or client trade data for this demonstration.
