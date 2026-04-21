# Operations Runbook

## Typical Incident Scenarios

### Late-arriving source corrections

1. Identify the impacted business date range.
2. Generate a replay plan for that bounded window.
3. Re-run Bronze and Silver only for the impacted partitions.
4. Rebuild downstream marts that depend on those partitions.
5. Compare replay manifest output against expected partition counts before closing the incident.

### Malformed payloads from an upstream API

1. Classify records into accepted and quarantined groups.
2. Persist quarantined payloads with a reason code and execution date.
3. Alert the upstream owner with sample rejected rows.
4. Replay only the corrected subset after the schema issue is resolved.

## Why This Exists

This runbook is here because owning a lakehouse is less about drawing the medallion diagram and more
about making sure bad data, retries, and partial failures do not create silent correctness issues.
