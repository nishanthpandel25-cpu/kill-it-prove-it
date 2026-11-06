# BREAKING.md — Failure Testing and Recovery Evidence

## 1. Purpose

This document records the failure-testing strategy, observed results and known limitations of the Kill It and Prove It database engine.

The objective is to verify that the database maintains data integrity and recovers correctly after unexpected process termination, incomplete transactions, concurrent access and storage failures.

Only completed tests and observed results are marked as verified.

## 2. Testing Environment

| Component | Environment |
|---|---|
| Operating system | Windows |
| Runtime | Python 3.14.0 |
| Test framework | pytest 9.2.0 |
| Project language | Python |
| Database architecture | Append-only key-value database |
| Storage format | Custom binary record format |
| Version control | Git |

## 3. Automated Test Suite

Command:

```powershell
py -m pytest -q
```

Latest recorded result:

```text
36 passed in 10.97s
```

All 36 tests passed after the stale compaction temporary-file fix.

The test suite covers:

- Basic PUT, GET and DELETE operations.
- Atomic batch operations.
- Crash recovery.
- Concurrent access.
- Thread synchronization.
- Process synchronization.
- Database durability.
- Compaction.
- Compaction crash recovery.
- Database size limits.
- Disk-full handling.
- Large values and large batches.

## 4. Random Crash and Restart Testing

The random crash runner performs repeated database process termination and restart experiments.

Recorded configuration:

| Parameter | Value |
|---|---:|
| Iterations | 500 |
| Random seed | 42 |
| PUT operations per iteration | 10 |
| Total acknowledged writes | 2,729 |
| Recovered acknowledged writes | 2,729 |

### Observed Result

```text
500 crash/restart iterations completed.
Acknowledged writes: 2729
Recovered writes: 2729
Oracle verification: PASSED
```

The independent oracle confirmed that all acknowledged writes were recovered.

**Limitation:** These kill points occurred after acknowledged writes. This experiment does not prove that the process was terminated at every possible point inside the physical file-write operation.

## 5. Concurrent Writer Testing

The concurrent writer runner tested multiple independent processes accessing the same database.

Configuration:

| Parameter | Value |
|---|---:|
| Concurrent processes | 4 |
| Operations per process | 125 |
| Total operations | 500 |
| PUT operations per process | 100 |
| DELETE operations per process | 25 |

The independent oracle verified the resulting database state.

### Observed Result

```text
Concurrent writer test completed.
Total operations: 500
Expected records: 300
Oracle verification: PASSED
```

This confirms that the tested concurrent writer workload produced the expected final state.

## 6. Large Value and Batch Testing

The large-data runner tested the following value sizes:

| Test | Value size |
|---|---:|
| 1 | 1 byte |
| 2 | 1 KB |
| 3 | 64 KB |
| 4 | 256 KB |
| 5 | 512 KB |
| 6 | 1 MB |

A batch containing 10,000 PUT operations was also tested.

### Observed Result

```text
Value-size tests: PASSED
Batch operations: 10000
Recovered records: 10006
Oracle verification: PASSED
```

The recovered database matched the independent oracle for the tested workload.

## 7. Large Database Recovery Benchmark

A one-million-key benchmark was completed.

| Measurement | Result |
|---|---:|
| Records inserted | 1,000,000 |
| Insertion time | 7.16 seconds |
| Database size | 56.13 MB |
| Recovery time | 1.64 seconds |
| Recovered records | 1,000,000 |

### Observed Result

The database recovered one million records in approximately 1.64 seconds in the recorded development environment.

The benchmark checked the recovered record count and selected values. It did not independently compare every stored value.

## 8. Atomic Batch Crash Testing

The large-batch crash tests use controlled checkpoints:

- `before_write`
- `after_sync`
- `after_commit`

The tests use a child process and terminate it at the selected checkpoint.

### Observed Results

| Checkpoint | Expected state | Result |
|---|---|---|
| before_write | Original database state | PASSED |
| after_sync | Complete batch recovered | PASSED |
| after_commit | Complete batch recovered | PASSED |

The tested batch recovery preserved the original state before writing and recovered the complete batch after synchronization.

**Limitation:** These checkpoints do not terminate the process at arbitrary positions inside the operating-system write loop.

## 9. Compaction Crash Testing

Compaction is tested at three checkpoints:

| Checkpoint | Expected state | Result |
|---|---|---|
| `temp_written` | Original database remains valid | PASSED |
| `before_replace` | Original database remains valid | PASSED |
| `after_replace` | New database remains valid | PASSED |

The tests verify that the database can be reopened and that the expected live records remain available after termination at these checkpoints.

## 10. Stale Temporary-File Failure

### 10.1 Bug Discovery

A regression test simulated an interrupted previous compaction by creating a corrupted temporary file:

```text
database.db.compact.tmp
```

A subsequent compaction appended new records to the existing temporary file.

The replacement database contained corrupted leading bytes, and reopening the database caused previously valid keys to disappear.

### 10.2 Root Cause

The temporary compaction file was opened for append without first removing an existing stale temporary file.

### 10.3 Fix

The compaction implementation now removes an existing temporary file before writing a fresh compacted database.

```python
if os.path.exists(temp_path):
    os.remove(temp_path)
```

The original database remains authoritative until the new temporary file is ready for replacement.

### 10.4 Verification

Regression test:

```powershell
py -m pytest tests/test_compaction.py::test_compaction_replaces_stale_temporary_file -v
```

Observed result:

```text
1 passed
```

Full compaction tests:

```powershell
py -m pytest tests/test_compaction.py tests/test_compaction_crash.py -v
```

Observed result:

```text
7 passed in 4.02s
```

Full test suite after the fix:

```text
36 passed in 10.97s
```

Git commit:

```text
0ce712b fix: handle stale compaction temporary files
```

## 11. Disk-Full and Size-Limit Testing

The project includes tests for:

- Configurable database size limits.
- Rejection of writes exceeding the configured limit.
- Batch size-limit failures.
- Preservation of existing data after rejected operations.
- Recovery after disk-full test conditions.

The storage layer checks the expected append size before writing.

If an append raises an exception after writing begins, it attempts to truncate the file to its original length and synchronize the rollback.

**Limitation:** These tests do not establish that every possible physical disk failure or sudden power loss can be rolled back. Process termination does not execute Python exception handlers.

## 12. Git Verification History

Relevant recorded commits:

```text
9240a91 Initial project structure
010d13d Implement basic PUT GET DELETE operations
3b6c400 Implement append-only storage and recovery
701224d Add crash recovery and checksum tests
90b5b02 feat: add atomic batches and concurrency safety
1af8ea2 feat: add crash-safe database compaction
ec5fb6a feat: add configurable database size limits
687d512 test: add 500-iteration random crash torture
e4766ee test: add concurrent writer torture runner
0b9d42f test: add large value and batch torture runner
aa15fc8 test: verify atomic batch crash recovery
4361ad0 test: add concurrency diagnostic utilities
0ce712b fix: handle stale compaction temporary files
aa822a7 docs: add database architecture and scenario proof matrix
```

## 13. Known Limitations and Remaining Experiments

The following experiments should be completed before claiming full failure coverage:

1. Terminate a process during an actual partial record write.
2. Terminate a process between writing COMMIT and completing `fsync()`.
3. Test two writers updating the same key concurrently.
4. Test GET operations during an active large batch.
5. Terminate a process during recovery truncation.
6. Terminate a process during the actual atomic replacement call.
7. Repeat termination and restart during recovery.
8. Test compaction failure when the temporary file cannot be fully written.
9. Test exact one-million-key recovery on the final demonstration computer.
10. Verify the final database using a complete independent value-by-value comparison.

## 14. Final Assessment

The database has passed its current 36-test automated suite.

The recorded experiments demonstrate:

- Successful recovery of acknowledged writes in the random crash workload.
- Correct results for the tested concurrent writer workload.
- Successful recovery of large values and large batches.
- Recovery of one million records within the recorded benchmark time.
- Successful compaction recovery at the tested checkpoints.
- Successful handling of the stale temporary-file regression.

These results provide evidence of the implemented durability, transaction and recovery mechanisms.

However, the remaining experiments listed above must be completed before claiming that every possible crash point and failure condition has been verified.

**Testing principle:** A feature is implemented when its code exists; it is verified only when a test or experiment demonstrates the expected behavior.