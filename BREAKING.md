# Failure Testing and Verification Report

## 1. Purpose

This document records the failure-testing strategy, observed results, verified behavior, and known limitations of the embedded key-value database.

The objective is to verify that the database maintains data integrity and recovers correctly after unexpected process termination, incomplete transactions, concurrent access, and storage failures.

Only completed tests and observed results are marked as verified. Implemented mechanisms and experimentally verified behavior are documented separately.

## 2. Test Environment

| Component | Details |
|---|---|
| Operating System | Windows |
| Python Version | Python 3.14.0 |
| Test Framework | pytest 9.2.0 |
| Database Type | Embedded append-only key-value database |
| Storage Format | Binary records with CRC32 checksums |
| Concurrency | Multiple processes and threads |
| Recovery | Log replay and incomplete-tail truncation |

## 3. Overall Test Results

The complete automated test suite was executed successfully.

**Latest full test execution:**

```text
python -m pytest -q

57 passed in 29.44s
```

No test failures were reported.

## 4. Group A — Individual Write Crash Testing

The following scenarios were tested:

| ID | Scenario | Observed Result |
|---|---|---|
| A1 | Process killed before the first physical PUT write | Previous database state preserved |
| A2 | Process killed during a partial record write | Incomplete record discarded during recovery |
| A3 | Complete record written before fsync | Complete record was recovered in the observed test; this does not establish durability before fsync |
| A4 | Process killed after fsync but before acknowledgement | Record recovered successfully |
| A5 | Process killed after successful PUT acknowledgement | Acknowledged records survived |
| A6 | Process killed during DELETE record write | Previous value remained available |
| A7 | Process killed during a 1 MB PUT | Partial value discarded; previous valid data preserved |

**Result:** All seven controlled individual-write scenarios were tested successfully.

## 5. Group B — Atomic Batch Crash Testing

Batch operations use BEGIN and COMMIT records. Recovery applies a batch only when a valid COMMIT record is present.

| ID | Scenario | Observed Result |
|---|---|---|
| B1 | Kill after 3 of 10 batch operations were written | Entire incomplete batch discarded |
| B2 | Kill after all operations but before COMMIT | Entire batch discarded |
| B3 | Kill after COMMIT bytes were written but before fsync | All batch operations recovered in the observed test |
| B4 | Kill after successful batch fsync but before return | Entire batch recovered |
| B5 | PUT X followed by DELETE X in one batch | X remained absent after recovery |
| B6 | Kill halfway through a 10,000-operation batch | Incomplete batch discarded; previous records preserved |

### B6 Recovery Measurement

The 10,000-operation batch crash test completed with:

- Existing records preserved: 100
- Recovery time: approximately 0.0253 seconds
- Incomplete batch operations: discarded

**Result:** All six controlled batch scenarios passed.

Note: B3 demonstrates the observed process-kill behavior before fsync. It does not prove what would happen under every possible power-loss or storage-device failure.

## 6. Group C — Recovery and Compaction Crash Testing

| ID | Scenario | Observed Result |
|---|---|---|
| C1 | Kill during recovery replay | Database recovered successfully on the next open |
| C2 | Kill after truncating an incomplete tail | Database recovered successfully on the next open |
| C3 | Kill during compaction before replacement | Original database remained recoverable |
| C4 | Kill around the compaction replacement boundary | Repeated pre-replacement termination tests passed; termination inside the OS rename operation was not directly tested |
| C5 | Kill after replacement before temporary-file cleanup | Database remained recoverable |
| C6 | Kill during close | Database reopened with its records intact |

### Compaction Stress Test

The compaction replacement-boundary test was repeated 20 times. Each iteration terminated the child process before replacement and verified that the database could be reopened.

**Result:** C1, C2, C3, C5, and C6 passed their controlled tests. C4 received partial verification; termination inside the operating-system replacement operation was not directly established.

## 7. Group D — Concurrent Writer Testing

| ID | Scenario | Observed Result |
|---|---|---|
| D1 | Two concurrent writers with forced process termination | Acknowledged writes recovered and oracle verification passed |
| D2 | Two writers updating the same key | Final value was one complete value from the competing writers |
| D3 | Long batch and individual PUT operations with batch termination | Incomplete batch discarded; individual writer completed its operations |
| D4 | GET while a batch was executing | Reader observed the completed batch state, not a partial batch |

### Additional Concurrent Writer Test

A separate multi-process test used four processes, each performing 125 operations.

- Total operations: 500
- PUT operations: 400
- DELETE operations: 100
- Expected remaining records: 300
- Independent oracle verification: passed

**Result:** All four concurrent-access scenarios passed with the qualifications described above.

## 8. Group E — Database Size Limit and Disk-Full Testing

The database supports a configurable maximum storage-file size.

| ID | Scenario | Observed Result |
|---|---|---|
| E1 | Single PUT reaches the configured size limit | Write rejected cleanly |
| E2 | Batch exceeds the configured size limit | Batch rejected without partial logical application |
| E3 | Size limit reached, followed by process restart | Existing database data remained recoverable |
| E4 | Size limit reached during compaction | Original database remained intact |
| E5 | Delete records, compact, and write again | New writes succeeded after compaction |

### Disk-Full Simulation

The disk-full tests simulated an `OSError` with error code 28 after a partial physical write.

The tests verified:

- Partial PUT rollback.
- Partial batch rollback.
- Recovery after a failed write.

Test output:

```text
9 passed in 0.18s
```

**Result:** The size-limit and simulated disk-full tests passed. E3 verifies restart after a failed write, rather than a forced kill at the exact moment the limit is reached.

## 9. Group F — Long-Running and Large-Scale Testing

### F1 — 500 Iterations of Random Crash Testing

The random crash torture test ran 500 iterations using seed 42.

Observed results:

- Crash iterations: 500
- Acknowledged writes recovered: 2,729
- Independent oracle verification: passed

The termination points were selected around acknowledged operations. This does not represent random termination at every possible physical write instruction.

### F2 — Repeated Recovery Interruption

A controlled recovery interruption was tested by terminating recovery during record replay and reopening the database.

The database successfully recovered on the next open.

Repeated kill/restart/kill cycles during recovery have not yet been fully established.

### F3 — One Million Key Benchmark

The database was tested with one million keys.

| Measurement | Result |
|---|---:|
| Number of keys | 1,000,000 |
| Insertion time | 7.16 seconds |
| Database file size | 56.13 MB |
| Recovery time | 1.64 seconds |

The recovered key count and selected values were checked.

This was a large-scale recovery benchmark, not a process-killed benchmark at one million keys.

### F4 — Final-Byte Truncation

A dedicated test that truncates exactly the final byte of the database file has not yet been established. Existing incomplete-record and torn-tail tests provide partial coverage of this behavior.

## 10. Additional Large-Data and Batch Testing

The large-data runner tested values of the following sizes:

```text
1 byte
1 KB
64 KB
256 KB
512 KB
1 MB
```

A 10,000-operation PUT batch was also tested.

Observed results:

- Recovered records: 10,006
- Oracle verification: passed
- Maximum tested value size: 1 MB

## 11. Independent Oracle Verification

The torture-testing system uses an independent expected-state oracle.

The oracle compares the expected key-value state with the recovered database state after process termination and restart.

This helps detect:

- Missing acknowledged writes.
- Unexpected values.
- Incorrect DELETE behavior.
- Partially applied batches.
- Data corruption caused by incomplete records.

## 12. Recovery and Storage Mechanisms

The implementation uses the following mechanisms to handle failures:

1. Append-only record storage.
2. CRC32 checksums for record validation.
3. BEGIN and COMMIT markers for batch transactions.
4. Recovery-time validation and incomplete-tail truncation.
5. File synchronization using `fsync`.
6. File locking for concurrent writers.
7. Temporary-file-based compaction and atomic replacement.
8. Configurable maximum database size.
9. Rollback attempts after failed physical writes.

These mechanisms are implemented in the database, storage, recovery, and file-locking modules.

## 13. Known Limitations and Unverified Scenarios

The following cases should not be claimed as fully verified:

- C4: Process termination inside the operating-system replacement operation.
- E3: Forced termination exactly when the storage limit is reached.
- F2: Multiple repeated interruptions during recovery.
- F3: Process-killed recovery benchmark with one million keys.
- F4: Dedicated final-byte truncation test.

Other exclusions from the project specification include:

- Random corruption of arbitrary middle-of-file bytes.
- Network-based database access.
- Sharding or multiple authoritative database files.
- Multi-batch transactions and advanced isolation levels.
- Performance guarantees beyond the specified recovery target.

## 14. Final Verification Summary

The implementation has passed 57 automated tests in the latest full test execution.

The tests cover:

- Individual write durability and crash recovery.
- Atomic batch recovery.
- Recovery interruption.
- Crash-safe compaction.
- Concurrent writers.
- GET visibility during batch operations.
- Database size limits.
- Simulated disk-full failures.
- Large values and large batches.
- Random crash testing.
- One-million-key recovery benchmarking.

The test results provide evidence of the implemented database's behavior under the tested failure conditions. Scenarios listed as partially verified or unverified remain explicit limitations and should not be represented as proven.

---

**Latest test result:** `57 passed in 29.44s`