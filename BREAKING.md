# Failure Testing and Verification Report

## 1. Purpose

This document records the failure-testing evidence for the embedded single-process key-value database.

The objective is to verify that acknowledged writes survive process termination, atomic batches recover consistently, concurrent access does not corrupt data, compaction preserves the database, and recovery handles interrupted or incomplete records.

The tests use controlled process termination, independent expected-state verification, file inspection, and recovery after restart.

## 2. Test Environment

- Operating system: Windows
- Python: 3.14.0
- Test framework: pytest 9.2.0
- Database implementation: Python standard library
- Storage model: append-only binary file with checksums
- Concurrency: thread locking and process-level file locking

## 3. Failure-Test Results

### A. Record Storage and Durability

The storage tests verify that records are appended using the defined binary format and that acknowledged data remains available after reopening the database.

Verified scenarios include:

- PUT and GET operations.
- DELETE operations.
- Record checksum validation.
- Recovery from incomplete trailing records.
- Preservation of previously acknowledged records after interrupted writes.

**Result:** The tested record-storage and durability scenarios passed.

Qualification: A process termination after a complete record has been written but before the operation's durability boundary is not equivalent to a real power-loss test. The observed recovery behavior does not establish power-loss guarantees beyond the implemented synchronization mechanism.

### B. Atomic Batch Operations

Batch operations use BEGIN and COMMIT records. Recovery applies a batch only when a valid matching COMMIT record is present.

Verified scenarios include:

- Successful batch execution.
- Recovery of committed batches.
- Discarding incomplete batches.
- Preservation of the pre-batch state when a batch is incomplete.
- Batch recovery after process termination.

**Result:** The tested atomic-batch recovery scenarios passed.

Qualification: A complete COMMIT record observed after process termination may be recovered even if the process was terminated before the operation returned. This is not proof of behavior under every possible hardware or power-loss condition.

### C. Crash-Safe Compaction

Compaction writes the current database state to a temporary file and replaces the original database file.

Verified scenarios include:

- Compaction preserves the logical database contents.
- Reopening after compaction returns the expected records.
- Stale temporary files are handled.
- Interrupted compaction preserves a recoverable database.

**Result:** The tested compaction scenarios passed.

Qualification: Repeated termination around the replacement boundary was tested. The test does not forcibly interrupt execution inside the operating system's atomic file-replacement operation.

### D. Concurrent Access

The concurrency tests exercise simultaneous database access and process-level locking.

Verified scenarios include:

- Multiple writers.
- Concurrent database operations.
- GET operations while a batch is being processed.
- Process-lock waiting and release.
- Consistent database state after concurrent activity.

**Result:** The tested concurrency scenarios passed.

Qualification: The tests establish behavior for the tested worker counts and workloads. They do not constitute a formal proof for every possible scheduling interleaving.

### E. Database Size Limits and Large Data

The tests verify configurable database size limits and handling of large records and batches.

Verified scenarios include:

- Configured maximum database size.
- Rejection of writes that exceed the configured limit.
- Preservation of existing data after a rejected write.
- Large values and large batches.
- Recovery after a failed write.

**Result:** The tested size-limit and large-data scenarios passed.

Qualification: The disk-full scenario verifies application-level size-limit rejection and rollback behavior. It does not reproduce every physical disk-full condition or filesystem failure. A separate forced process termination at the exact rejection point was not performed.

### F. Recovery and Torture Testing

#### F1. 500 Random Crash-and-Restart Iterations

A randomized crash test was executed for 500 iterations using an independent oracle.

Test procedure:

1. Start a database worker process.
2. Wait until the worker is ready.
3. Allow a randomized number of operations to execute.
4. Update the independent oracle only after an operation is acknowledged.
5. Terminate the worker while it is waiting for the next operation.
6. Reopen the database.
7. Compare the recovered state against the oracle.
8. Repeat for 500 iterations.

Results:

- Crash iterations: 500
- Acknowledged operations: 1,204
- Final oracle records: 25
- Oracle verification after every restart: PASSED

**Result: PASSED**

Qualification: The randomized terminations occur between acknowledged operations. This test does not randomly terminate the process during every possible physical-write instruction. Torn-record and incomplete-record recovery are covered by separate tests.

#### F2. Repeated Interruptions During Recovery

A recovery-interruption test verifies that recovery remains repeatable when the process is terminated multiple times during replay.

Test procedure:

1. Create 100 acknowledged records.
2. Start a separate recovery process.
3. Terminate the recovery process after three record-replay checkpoints.
4. Repeat the interruption three consecutive times.
5. Perform a final normal recovery.
6. Verify all 100 records and their values.

Results:

- Initial records: 100
- Consecutive recovery interruptions: 3
- Final recovered records: 100
- Final value verification: PASSED

**Result: PASSED**

#### F3. One Million Keys: Forced Process Termination and Recovery

This test verifies recovery performance with one million acknowledged keys after the writer process is forcibly terminated.

Test procedure:

1. Start a writer process.
2. Insert one million keys in batches of 1,000.
3. Wait until all writes have been acknowledged.
4. Forcefully terminate the writer process.
5. Start a separate recovery process.
6. Measure the database-open and recovery duration.
7. Verify all one million keys and their values.

Results:

- Acknowledged keys: 1,000,000
- Recovered keys: 1,000,000
- Verified key/value pairs: 1,000,000
- Recovery time: 1.7412 seconds
- Required recovery limit: Less than 2 seconds

**Result: PASSED**

The measured recovery time was below the two-second target in this test environment.

Qualification: The timing measures database construction and recovery, not the entire process startup and shutdown lifecycle. Performance can vary across machines and storage devices.

#### F4. Final-Byte Truncation

This test verifies recovery when the final byte of an appended, unacknowledged record is missing.

Test procedure:

1. Create a database containing three acknowledged records.
2. Close the database and record its acknowledged file size.
3. Append an additional record that has not been acknowledged.
4. Truncate exactly one byte from the end of the file.
5. Reopen the database.
6. Verify that the three acknowledged records remain intact.
7. Verify that the incomplete record is absent.
8. Verify that the file is restored to its acknowledged size.

Results:

- Acknowledged records preserved: 3
- Incomplete record recovered as data: No
- File size restored to acknowledged size: Yes

**Result: PASSED**

## 4. Full Test Suite

The complete pytest suite was executed after the F1-F4 additions and recovery optimization.

Command:

```powershell
python -m pytest -q
```

Final result:

```text
64 passed in 101.58s
```

**Overall result: 64 tests passed.**

## 5. Recovery Performance Optimization

The one-million-key recovery test initially measured 2.0436 seconds, slightly above the required two-second target.

The recovery implementation was optimized so that recovery test checkpoints are invoked only when a test hook is configured. This avoids an unnecessary function call for every standalone record during normal recovery.

After the optimization:

- Previous recovery time: 2.0436 seconds
- Optimized recovery time: 1.7412 seconds
- Improvement: Approximately 14.8%

The optimized implementation passed the one-million-key recovery test and the complete 64-test suite.

## 6. Known Limitations

The following limitations should remain explicit:

1. **Power-loss testing:** Process termination tests do not fully simulate sudden power loss, storage-controller behavior, or every filesystem failure.
2. **Compaction replacement boundary:** The tests do not forcibly interrupt the operating system while it is executing the file-replacement operation.
3. **Physical disk exhaustion:** Configured size-limit rejection is tested, but every possible physical disk-full and filesystem failure condition is not reproduced.
4. **Concurrency coverage:** The tested concurrency workloads do not prove correctness for every possible thread and process scheduling interleaving.
5. **Recovery performance:** The 1.7412-second result is specific to the tested machine and storage environment.

These limitations do not invalidate the passing tests, but they define the scope of the evidence.

## 7. Final Verification Summary

| Area | Verification result |
|---|---|
| A - Record storage and durability | Tested scenarios passed |
| B - Atomic batches | Tested scenarios passed |
| C - Crash-safe compaction | Tested scenarios passed, with replacement-boundary qualification |
| D - Concurrent access | Tested scenarios passed |
| E - Size limits and large data | Tested scenarios passed, with physical disk-full qualification |
| F1 - 500 random crash/restart iterations | PASSED |
| F2 - Repeated recovery interruptions | PASSED |
| F3 - One million keys, forced termination and recovery | PASSED |
| F4 - Final-byte truncation | PASSED |
| Full pytest suite | 61 passed |

## 8. Conclusion

The database passed all 61 automated tests in the current test suite.

The additional F1–F4 tests provide evidence for randomized crash-and-restart behavior, repeated recovery interruptions, recovery of one million acknowledged keys within the two-second target, and recovery from final-byte truncation.

The results support the tested durability, atomicity, concurrency, compaction, size-limit, and recovery behaviors. The qualifications in this report identify conditions that have not been fully simulated and should not be represented as proven.

**Final test status: 61 passed.**

## 9. Standalone Torture Launcher Verification

The project provides a standalone launcher for randomized operation testing and process-termination testing.

**Windows command:**
``powershell
.\torture.cmd --iterations 500 --seed 42
``

**Observed result (seed 42):**
- Crash-test iterations: 500
- Acknowledged writes: 2,729
- Recovered records: 2,729
- Independent oracle verification: PASSED

The runner terminates the writer process between acknowledged operations, then reopens the database and compares recovered data with the independent oracle. This result does not establish recovery from termination during an individual physical write or from sudden power loss.
