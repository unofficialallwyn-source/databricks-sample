# Trade Processing & Reconciliation Lakehouse

Enterprise-style Databricks capstone for a fictional ABC Investment Bank.

The project demonstrates how to design and build a governed, replayable, auditable trade-processing and reconciliation platform using Databricks, Delta Lake, Unity Catalog, Lakeflow, Spark/PySpark, Databricks SQL, GitHub, Declarative Automation Bundles, and infrastructure-as-code.

The project is intentionally being implemented hands-on. Architecture, governance, testing, and delivery standards are documented first; implementation code is added incrementally through feature branches and pull requests.

## Project Status

Sprint 0 — Foundation: COMPLETE.

Completed design stories:

- TR-001 — Business workflow
- TR-002 — Functional requirements
- TR-003 — Non-functional requirements
- TR-004 — Canonical trade model
- TR-005 — Source contracts
- TR-006 — Reconciliation rules
- TR-007 — Databricks architecture
- TR-008 — Unity Catalog structure
- TR-009 — DEV / TEST / PROD strategy
- TR-010 — Repository structure
- TR-011 — Deployment strategy
- TR-012 — Coding standards
- TR-013 — Testing strategy
- TR-014 — Definition of Done
- TR-015 — Synthetic data generator design

Current implementation story:

- TR-016 / Jira SCRUM-37 — Implement synthetic trade data generator

Current implementation branch:

    feature/TR-016-synthetic-data-generator

## Business Objective

The platform ingests internal OMS trades and external broker confirmations, determines whether both systems agree on trade economics, and produces an auditable reconciliation result.

Example:

    OMS
    Trade ID: T10052
    Instrument: AAPL
    Quantity: 1000
    Price: 231.42 USD

    Broker
    Client Trade ID: T10052
    Instrument: AAPL
    Quantity: 1000
    Price: 231.45 USD

Expected reconciliation:

    Status: BREAK
    Break Type: PRICE_MISMATCH
    Difference: 0.03

The initial asset class is equities. The architecture is intended to support later extension to FX, bonds, futures, options, and swaps.

## Business Workflow

    Trade Executed
          |
          v
    OMS Records Trade
          |
          v
    Broker Confirmation
          |
          v
    Source File Landing
          |
          v
    Bronze Ingestion
          |
          v
    Silver Validation / Canonicalization
          |
          v
    Matching
          |
          v
    Economic Comparison
          |
          +------------------+
          |                  |
          v                  v
       MATCHED             BREAK
                             |
                             v
                     Operations Investigation
                             |
                             v
                      RESOLVED / WAIVED

Matching and comparison are separate concerns.

Matching answers:

    Which OMS and Broker records belong together?

Comparison answers:

    Do the matched records agree economically?

## MVP Source Model

Two source systems are used initially:

1. OMS / internal trade system
2. Broker A confirmation system

Settlement feeds and additional reference sources are deferred.

Source delivery model:

- immutable files
- micro-batches approximately every five minutes or less
- at-least-once delivery
- duplicates are possible
- out-of-order events are possible
- amendments, corrections, and cancellations are new source events
- source event time and ingestion time are separate concepts

## Volume Assumption

Architecture baseline:

- approximately 5M OMS source events/day
- approximately 5M Broker source events/day
- approximately 10M source records/day baseline
- target architecture headroom up to approximately 100M source records/day

Business trade count and source event count are not the same.

One business trade may produce:

    OMS V1
    OMS V2 amendment
    Broker V1
    Broker V2 correction
    duplicate delivery

Therefore all performance results must separately report business trade count and source event count.

## Non-Functional Requirements

Primary targets:

- P95 normal reconciliation result within 5 minutes after both sides are available
- P99 within 10 minutes
- default missing-side SLA: 30 minutes, configurable
- processing availability target: 99.9 percent excluding planned/upstream outages
- near-zero logical data loss after raw landing
- RTO assumption: 4 hours
- raw/audit retention assumption: 7 years for the capstone
- replay and backfill supported
- least privilege
- no hardcoded secrets
- DEV / TEST / PROD isolation
- schema evolution and quarantine
- observable pipelines and reconciliation backlog
- versioned code/configuration/rules
- cost attribution by environment and workload

## Canonical Trade Model Principles

Important identity rules:

- source identity is not platform identity
- source record identity is separate from business matching identity
- OMS trade ID is not treated as a physical database primary key
- source event history is retained
- current state is derived using source-local ordering/version
- broker-only records are supported
- reconciliation parent state is separate from break child records
- deterministic identifiers are preferred where practical

Important canonical concepts include:

- canonical_record_id
- record_role: INTERNAL or EXTERNAL
- source_system
- source_record_id
- source_trade_id
- business_trade_id
- source_version
- event_type
- event_sequence
- event_time
- source_created_at / source_updated_at
- ingestion_timestamp
- business_date
- instrument_id / instrument_type
- symbol
- side
- quantity
- price
- currency
- account_id
- portfolio_id
- broker_id
- venue_id
- trade_date
- execution_timestamp
- settlement_date
- trade_status
- schema_version
- payload_hash
- data_quality_status
- is_current
- effective_from / effective_to

Price and quantity use DECIMAL semantics rather than floating point comparison.

## Source Contracts

### OMS

Format: JSONL

Core fields:

- event_id
- trade_id
- trade_version
- event_type: NEW / AMEND / CORRECT / CANCEL
- event_time
- schema_version
- instrument_id
- instrument_type
- side
- quantity
- price
- currency
- account_id
- portfolio_id
- broker_id
- venue_id
- trade_date
- execution_timestamp
- settlement_date
- published_at

OMS version is authoritative only within OMS.

### Broker A

Format: CSV

Core fields:

- confirmation_event_id
- broker_trade_id
- client_trade_id
- confirmation_version
- confirmation_type: CONFIRM / CORRECT / CANCEL
- confirmation_time
- schema_version
- instrument_code
- instrument_type
- side
- quantity
- price
- currency
- client_account
- broker_id
- venue
- trade_date
- execution_timestamp
- settlement_date
- published_at

Broker version is authoritative only within the Broker source.

There is no requirement that OMS V1 equals Broker V1 or OMS V2 equals Broker V2.

The reconciliation engine compares the latest eligible current state from each source.

## Duplicate and Conflict Semantics

Same event ID + same payload/version:

    normal redelivery / duplicate

Same event ID + different payload:

    CONFLICTING_EVENT_ID

A real correction uses:

    new event ID + higher source-local version

## Data Quality

Structural error examples:

- PARSE_ERROR
- MISSING_REQUIRED_FIELD
- INVALID_DATA_TYPE
- INVALID_TIMESTAMP
- UNSUPPORTED_SCHEMA_VERSION
- CONFLICTING_EVENT_ID

Semantic error examples:

- INVALID_ENUM
- INVALID_QUANTITY
- INVALID_REFERENCE_DATA
- INVALID_DATE_RELATIONSHIP

Invalid data must never silently disappear.

Quarantine must preserve enough information to replay/investigate:

- source
- file/path
- event ID if recoverable
- raw payload/row
- schema version
- failure code
- failure reason
- failure timestamp
- processing run ID

## Reconciliation Rules

Only eligible current records are reconciled.

Eligibility includes:

- VALID data-quality state
- is_current = true
- source ordering resolved
- not superseded

### Matching

Primary MVP matching method:

    INTERNAL.business_trade_id
    matched against
    EXTERNAL.business_trade_id

Matching is evaluated in the appropriate broker/source context.

Expected result:

- one internal + one external -> compare economics
- missing Broker match key -> MATCH_KEY_MISSING
- multiple internal candidates -> DUPLICATE_INTERNAL
- multiple external candidates -> DUPLICATE_EXTERNAL
- ambiguous legitimate candidates -> MULTIPLE_MATCH_CANDIDATES

The engine must not arbitrarily select row number 1 to hide ambiguity.

### Missing Side

OMS exists / Broker missing:

- before SLA -> PENDING
- after SLA -> MISSING_CONFIRMATION

Broker exists with usable business trade ID / OMS missing:

- before SLA -> PENDING
- after SLA -> MISSING_TRADE

SLA is anchored to the first platform observation of an eligible record.

A later amendment to the same business trade does not automatically reset the missing-side SLA.

### Economic Comparison

Initial comparisons:

- instrument
- side
- quantity
- price
- currency
- broker
- account
- trade date
- settlement date
- lifecycle / cancellation state

Initial project price-tolerance default:

    absolute difference <= 0.01 in trade currency

The value is configuration/reference data, not a universal market rule.

Multiple simultaneous breaks are supported.

### Break Types

Initial taxonomy:

- MISSING_TRADE
- MISSING_CONFIRMATION
- MATCH_KEY_MISSING
- DUPLICATE_INTERNAL
- DUPLICATE_EXTERNAL
- MULTIPLE_MATCH_CANDIDATES
- INSTRUMENT_MISMATCH
- SIDE_MISMATCH
- QUANTITY_MISMATCH
- PRICE_MISMATCH
- CURRENCY_MISMATCH
- BROKER_MISMATCH
- ACCOUNT_MISMATCH
- TRADE_DATE_MISMATCH
- SETTLEMENT_DATE_MISMATCH
- CANCELLATION_MISMATCH
- REQUIRED_VALUE_MISSING

## Break Lifecycle

Break status:

- OPEN
- RESOLVED
- WAIVED

Resolution type:

- AUTO
- MANUAL

Possible resolution reasons include:

- LATE_CONFIRMATION
- LATE_TRADE
- OMS_CORRECTION
- BROKER_CORRECTION
- SUPERSEDED_VERSION
- LATE_CANCELLATION
- MANUAL_RESOLUTION
- MANUAL_WAIVER

Historical breaks are never deleted.

Example:

    OMS V1
      |
      v
    Broker missing
      |
      v
    SLA expires
      |
      v
    MISSING_CONFIRMATION OPEN
      |
      v
    OMS V2 arrives
      |
      v
    Same break remains open
      |
      v
    Broker confirmation arrives
      |
      v
    Re-evaluate latest states
      |
      +-------------------------+
      |                         |
      v                         v
    MATCH                    ECONOMIC BREAK
      |                         |
      v                         v
    Missing break            Missing break
    AUTO RESOLVED            AUTO RESOLVED
                              +
                              new field break OPEN

Operations owns genuine business/economic reconciliation breaks.

Production/Data Support owns technical pipeline/file/schema delivery failures.

## Databricks Architecture

    OMS / Broker Files
            |
            v
    Unity Catalog Governed Landing
            |
            v
    Lakeflow + Auto Loader
            |
            v
          BRONZE
       raw/source-preserving
            |
            v
      Lakeflow Silver
      validation / normalization
      AUTO CDC / SCD2
            |
       +----+-----+
       |          |
       v          v
    Quarantine  Silver Current / History
                  |
                  v
          Reconciliation Job
           + SLA sweep
                  |
        +---------+---------+
        |         |         |
        v         v         v
    Recon       Break     Audit
    Current     Current    History
        |
        v
       GOLD
        |
        v
    Databricks SQL

### Workload Boundaries

A. Bronze ingestion

- Lakeflow Spark Declarative Pipeline
- Auto Loader
- independent of downstream failures

B. Silver canonicalization

- Lakeflow pipeline
- validation
- normalization
- AUTO CDC
- SCD Type 2 source history/current-state derivation

C. Reconciliation

- Lakeflow Job
- PySpark / Python / SQL
- incremental changed-trade processing
- SLA-due pending-case sweep
- Delta MERGE for current state
- append history for business transitions

D. Gold

- business-facing metrics
- materialized views where appropriate
- Databricks SQL serving

## Incremental Reconciliation

Routine reconciliation must not rescan all historical trades every minute.

Conceptually:

    changed business trade IDs
            +
    SLA-due PENDING cases
            |
            v
    latest current OMS/Broker states
            |
            v
    rule evaluation
            |
            v
    current reconciliation / break MERGE
            +
    append audit history

Delta Change Data Feed or an equivalent incremental mechanism may be used for changed-source identification.

## Replay and Backfill

Recovery layers:

    Raw Files -> Bronze rebuild
    Bronze -> Silver rebuild
    Silver -> Reconciliation reprocess
    Reconciliation -> Gold rebuild

Backfill uses the same business logic as normal processing.

Historical replay must not reuse a live production streaming checkpoint in a way that corrupts normal stream state.

## Unity Catalog Structure

One regional metastore for the capstone.

Environment catalogs:

- trade_recon_dev
- trade_recon_test
- trade_recon_prod

Each contains:

- landing
- bronze
- silver
- reconciliation
- gold
- reference
- quarantine
- audit

### Storage Strategy

External Volume:

- landing/source files written by external systems

Unity Catalog managed tables:

- Bronze
- Silver
- Reconciliation
- Gold
- Reference
- Quarantine
- Audit

No external Delta tables are required for the MVP.

### Identity Model

Human groups:

- trade-recon-admins
- trade-recon-developers
- trade-recon-operations
- trade-recon-analysts
- trade-recon-prod-readers
- trade-recon-auditors

Production runtime identities:

- sp-trade-recon-prod-ingestion
- sp-trade-recon-prod-transform
- sp-trade-recon-prod-reconciliation
- sp-trade-recon-prod-gold

Deployment identities:

- sp-trade-recon-dev-deployer
- sp-trade-recon-test-deployer
- sp-trade-recon-prod-deployer

Production write access belongs to approved service principals.

Operations receives read/investigation access, not broad direct MODIFY on reconciliation tables.

## Environment Strategy

Separate Databricks workspaces:

- trade-recon-dev-ws
- trade-recon-test-ws
- trade-recon-prod-ws

Mandatory workspace-catalog bindings:

- trade_recon_dev -> DEV workspace only
- trade_recon_test -> TEST workspace only
- trade_recon_prod -> PROD workspace only

Cross-environment access is denied by default.

Environment storage credentials and external locations are also isolated.

The same source code is promoted through:

    DEV -> TEST -> PROD

Environment differences are configuration driven.

## Deployment Strategy

Terraform owns platform/governance infrastructure.

Declarative Automation Bundles own Databricks workload resources.

Git strategy:

    main
      ^
      |
    short-lived feature branches

No long-lived dev/test/prod Git branches.

Promotion:

    feature branch
        |
        v
    pull request
        |
        v
      main
        |
        v
    exact commit -> TEST
        |
        v
    integration/security/performance gates
        |
        v
    approval
        |
        v
    same exact commit -> PROD
        |
        v
    smoke verification

DEV Bundle target uses development mode.

TEST and PROD use production-mode safeguards.

GitHub Actions is the intended CI/CD orchestrator.

Preferred CI/CD authentication:

    GitHub Actions OIDC
          |
          v
    Databricks workload identity federation
          |
          v
    environment-specific service principal

Long-lived PATs/client secrets are not the preferred CI/CD design.

## Coding Standards

Primary rules:

- production business logic must be reusable/testable outside notebooks
- notebooks are for learning, exploration, debugging, demos, and thin orchestration
- avoid notebook-state dependencies
- prefer native Spark functions over Python UDFs
- avoid unbounded collect or toPandas
- explicit schemas for production contracts
- explicit curated output columns
- meaningful DataFrame names
- join cardinality must be understood
- no arbitrary many-to-many match suppression
- do not cache/repartition without evidence
- environment-specific values are configuration
- business tolerances/SLAs are reference/configuration data
- use structured logging rather than print for production operations
- never silently suppress exceptions
- data-quality errors are quarantined; system/programming errors fail visibly
- every state-changing write must have explicit idempotency semantics
- current state and audit history are separate concepts
- technical timestamps normalize to UTC
- business dates use configured business calendar/timezone semantics

Planned tooling:

- Black
- Ruff
- pytest
- optional mypy/pyright

## Testing Strategy

Testing pyramid:

1. Python rule/unit tests
2. small PySpark transformation tests
3. source contract tests
4. data-quality tests
5. reconciliation scenario tests
6. idempotency/replay/backfill tests
7. Databricks pipeline/AUTO CDC tests
8. integration/E2E tests
9. security and workspace-binding tests
10. performance/resilience tests
11. production smoke tests

Critical reconciliation modules should target high business-scenario coverage; numeric line coverage alone is not considered sufficient.

Mandatory scenarios include:

- exact match
- price mismatch
- quantity mismatch
- multi-break
- missing confirmation
- missing trade
- late confirmation
- late trade
- missing match key
- OMS amendment
- Broker correction
- out-of-order OMS
- out-of-order Broker
- duplicate event
- conflicting event ID
- cancellation mismatch
- automatic break resolution
- OMS V1 missing break -> OMS V2 -> late Broker confirmation regression scenario

## Definition of Done

A story is Done only when:

    acceptance criteria
        +
    all applicable project DoD criteria
        =
    DONE

Applicable criteria may include:

- code quality
- tests
- data contract
- data quality
- idempotency
- replay
- Delta persistence
- Unity Catalog/security
- performance
- resilience
- observability
- auditability
- deployment
- smoke verification
- documentation/runbook

A story is not Done when mandatory tests fail, idempotency/replay is known to be unsafe, secrets are hardcoded, business history can be lost, bad data is silently discarded, or a production smoke test fails.

## Repository Structure

    databricks-sample/
    |
    +-- .github/
    |   +-- workflows/
    |
    +-- config/
    |   +-- dev/
    |   +-- test/
    |   +-- prod/
    |
    +-- data/
    |   +-- contracts/
    |   |   +-- oms/
    |   |   +-- brokers/
    |   +-- samples/
    |   +-- synthetic/
    |
    +-- docs/
    |   +-- adr/
    |   +-- architecture/
    |   +-- data_contracts/
    |   +-- diagrams/
    |   +-- runbooks/
    |
    +-- infra/
    |   +-- terraform/
    |       +-- modules/
    |       +-- environments/
    |           +-- dev/
    |           +-- test/
    |           +-- prod/
    |
    +-- notebooks/
    |   +-- explorations/
    |
    +-- resources/
    |   +-- jobs/
    |   +-- pipelines/
    |   +-- sql/
    |
    +-- scripts/
    |
    +-- src/
    |   +-- trade_recon/
    |       +-- ingestion/
    |       +-- canonicalization/
    |       +-- data_quality/
    |       +-- models/
    |       +-- reconciliation/
    |       +-- reference/
    |       +-- gold/
    |       +-- common/
    |       +-- synthetic/        <-- TR-016 implementation
    |
    +-- tests/
        +-- unit/
        +-- integration/
        +-- data_quality/
        +-- performance/

## TR-016 — Synthetic Data Generator

### Goal

Build deterministic synthetic upstream feeds for hands-on development.

The generator creates OMS and Broker source inputs only.

It must not write reconciliation answers into source payloads.

### Required Outputs

OMS:

- JSONL files

Broker A:

- CSV files

Generator metadata:

- run manifest
- expected-results/test-oracle manifest kept separate from source payloads

### Required Determinism

Same:

- generator version
- seed
- business date
- scenario configuration
- scale

must generate the same logical data.

Manifest should record:

- generator_version
- seed
- business_date
- scenario mix
- business_trade_count
- oms_event_count
- broker_event_count
- duplicate count
- amendment count
- correction count
- cancellation count
- output locations

### Synthetic Scenario Catalogue

Start with these in order:

1. EXACT_MATCH
2. PRICE_MISMATCH
3. PRICE_WITHIN_TOLERANCE
4. QUANTITY_MISMATCH
5. MULTI_FIELD_MISMATCH
6. MISSING_CONFIRMATION
7. LATE_CONFIRMATION
8. MISSING_TRADE
9. LATE_TRADE
10. MATCH_KEY_MISSING
11. OMS_AMENDMENT
12. BROKER_CORRECTION
13. OUT_OF_ORDER_OMS
14. OUT_OF_ORDER_BROKER
15. DUPLICATE_OMS_EVENT
16. DUPLICATE_BROKER_EVENT
17. CONFLICTING_OMS_EVENT
18. CONFLICTING_BROKER_EVENT
19. DUPLICATE_INTERNAL_CANDIDATE
20. DUPLICATE_EXTERNAL_CANDIDATE
21. OMS_CANCEL_BROKER_ACTIVE
22. BROKER_CANCEL_OMS_ACTIVE
23. BOTH_CANCELLED
24. LATE_CANCEL
25. REQUIRED_VALUE_MISSING
26. INVALID_SCHEMA
27. INVALID_DATA_TYPE
28. INVALID_ENUM
29. BROKER_MISMATCH
30. ACCOUNT_MISMATCH
31. OMS_VERSION_AFTER_MISSING_BREAK

### Critical Regression Scenario

OMS_VERSION_AFTER_MISSING_BREAK:

    Phase 1:
    OMS V1 arrives

    Phase 2:
    Broker absent
    SLA expires
    MISSING_CONFIRMATION should open

    Phase 3:
    OMS V2 arrives
    same missing-confirmation episode should remain

    Phase 4:
    Broker confirmation arrives reflecting latest OMS economics

Expected platform behavior:

- one reconciliation case
- no duplicate missing-confirmation break because of OMS V2
- original missing break auto-resolves
- final status MATCHED if economics agree

### Delivery Model

Logical event order and file-arrival order must be independently controllable.

This allows:

    OMS V2 event created after V1
    but
    V2 delivered before V1

and staged late-confirmation scenarios.

### Scale Profiles

Suggested progression:

- SMALL: tens to thousands
- MEDIUM: approximately 100K
- LARGE: approximately 1M
- TARGET: project target-scale configuration
- STRESS: up to architecture headroom where cost permits

Do not start at large scale.

Prove correctness at 10, 100, 1,000 records first.

### Implementation Sequence

Recommended coding sequence:

1. Generator configuration/domain representation
2. deterministic base trade generation
3. OMS V1 generation
4. Broker V1 generation
5. JSONL/CSV writers
6. EXACT_MATCH
7. PRICE_MISMATCH
8. QUANTITY_MISMATCH
9. missing/late counterpart scenarios
10. OMS amendment
11. Broker correction
12. out-of-order delivery
13. duplicates
14. conflicting event IDs
15. cancellations
16. run manifest
17. expected-results oracle
18. generator self-validation
19. scenario mix
20. scale profiles
21. performance optimization only after correctness

### Generator Method Scaffold

The feature branch contains:

    src/trade_recon/synthetic/generator.py

Only high-level method signatures are supplied.

You are expected to implement the bodies.

A corresponding test scaffold exists at:

    tests/unit/test_synthetic_generator.py

The tests are intentionally skipped until implementation begins. Remove the module-level skip incrementally as you implement behavior.

## Feature Branch / Pull Request Workflow

Current feature branch:

    feature/TR-016-synthetic-data-generator

Work only on the feature branch.

Recommended flow:

    git checkout feature/TR-016-synthetic-data-generator

    implement generator

    run tests

    commit changes

    push feature branch

    raise PR:
    feature/TR-016-synthetic-data-generator -> main

The PR will be reviewed against:

- TR-012 coding standards
- TR-013 testing strategy
- TR-014 Definition of Done
- TR-015 generator contract

Do not merge the PR until mandatory review comments are resolved.

## Main Branch Policy

The intended policy is:

- no direct pushes to main
- pull request required
- review required before merge
- required checks when CI is implemented
- feature branches for all implementation work

GitHub repository settings/rulesets must enforce this policy server-side; documentation alone is not considered sufficient protection.

## Immediate Hands-On Task

For TR-016, implement the methods in:

    src/trade_recon/synthetic/generator.py

Recommended first milestone:

- deterministic base trade
- OMS V1
- Broker V1
- EXACT_MATCH
- JSONL writer
- CSV writer
- manifest
- unit tests for determinism and exact match

Only after that milestone is green should you add mismatch, late-arrival, version, duplicate, and cancellation scenarios.
