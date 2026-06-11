# Bidirectional CDC System

## Overview

Change Data Capture is a mechanism of keeping two or more distributed databases in sync, in real time. This repository explores a custom CDC system for bidirectional syncing between two databases with conflict resolution, failure recovery and self-healing. 

## Problem Statement

**Why not use AWS DMS?**

AWS Database Migration Service (DMS) has a critical limitation: it doesn't properly handle Oracle's legacy `LONG RAW` data type. When attempting to migrate or replicate tables containing `LONG RAW` columns, DMS silently truncates the data, leading to data loss and corruption.

In our use case:
- 200+ tables in the Oracle database
- ~80 tables (40%) contain `LONG RAW` binary data
- Cannot afford data loss or corruption
- Required bidirectional sync (not just one-way migration)

## Solution Architecture (Representative)

### Core Components

1. **Change Monitors**
   - Oracle Redo Log monitor: Detects INSERT/UPDATE/DELETE operations
   - Postgres WAL monitor: Detects changes in target database
   - Filters for the 80 tables containing binary data

2. **Replication Engine**
   - Applies changes from source to target database

3. **Resilience Layer**
   - `audit_log`: Tracks all successful operations
   - `error_log`: Tracks failed operations with PENDING status, keeps track of retries 
   - `orchestrator`: clears out stale jobs, runs retry poller to clear failed jobs with failures < THRESHOLD 

## Key Features

- **Bidirectional Sync**: Changes flow Oracle -> Postgres AND Postgres -> Oracle
- **Conflict Resolution**: Last-write-wins based on update timestamps
- **Binary Data Support**: Custom handling for LONG RAW and other legacy types
- **Fault Tolerance**: Automatic retry with exponential backoff
- **Audit Trail**: Complete history of all replicated operations
- **Alerting**: Escalation for operations that fail after max retries

## Data Flow

```
Oracle DB                          Postgres DB
    |                                  |
    | Redo Log                         | WAL
    v                                  v
[Redo Monitor] ←--conflict?--→ [WAL Monitor]
    |                                  |
    v                                  v
[Conflict Resolver]               [Conflict Resolver]
    |                                  |
    v                                  v
[LONG RAW Handler] → [SQS Queue] → [Apply to Postgres]
                         ↓
                    [Audit Log]
                         ↓
                  (on failure)
                    [Error Log]
                         ↓
                   [Retry Poller]
                         ↓
                  (after 3 retries)
                      [Alert]
```

## Repository Contents

This repository contains sanitized versions of the core components:

- **monitors/**: Redo Log and WAL monitoring logic
- **replication/**: Conflict resolution and binary data handling
- **resilience/**: Retry mechanism, error tracking, and audit logging

## Production Stats (Representative)

- Replicates 80 tables bidirectionally
- Handles 10K+ operations per hour during peak
- 99.9% success rate with retry mechanism
- < 30 second replication lag under normal load

## Getting Started

See individual module READMEs for implementation details and usage examples.

---

**Note**: This repository contains sanitized code with generic table/column names and placeholder business logic. The architecture and technical patterns are production-tested.
