# Kill It and Prove It — Database Architecture

## 1. Project Overview

This project implements a lightweight, embedded key-value database engine from scratch using Python.

The database stores key-value pairs in a binary file and uses an append-only storage design. An in-memory hash index provides fast lookups, while the append-only log provides persistence and crash recovery.

The implementation supports:

- PUT — insert or update a key.
- GET — retrieve a value using its key.
- DELETE — remove a key.
- BATCH — execute multiple operations atomically.
- OPEN — initialize the database and recover its state.
- CLOSE — close the database.
- COMPACTION — remove obsolete records and reduce file size.
- CONCURRENT ACCESS — coordinate access between threads and processes.

The implementation uses Python standard-library modules and does not depend on an external database engine.

## 2. High-Level Architecture

```text
                 APPLICATION / CLI
                         |
                         v
                   DATABASE ENGINE
                     database.py
                         |
              +----------+----------+
              |          |          |
              v          v          v
           PUT/GET     BATCH    COMPACTION
           DELETE     HANDLER     HANDLER
              |          |          |
              +----------+----------+
                         |
                         v
                    FILE LOCKING
                     file_lock.py
                         |
                         v
                    STORAGE LAYER
                     storage.py
                         |
                         v
                  AUTHORITATIVE FILE
                    database.db
                         |
                         v
                   RECOVERY ENGINE
                     recovery.py
                         |
                         v
                   IN-MEMORY INDEX
                     Python dict
```

### Main Components

| Component | Responsibility |
|---|---|
| `database.py` | Main database API and operation coordination |
| `storage.py` | Binary record encoding, appending and synchronization |
| `recovery.py` | Reads the database file and reconstructs the index |
| `file_lock.py` | Coordinates access between processes |
| In-memory dictionary | Maintains the current key-value state |
| `database.db` | Authoritative persistent database file |
| `database.db.compact.tmp` | Temporary file used during compaction |

## 3. Storage Architecture

### 3.1 Append-Only Log

The database uses an append-only log.

When a key is inserted, updated or deleted, a new record is appended to the database file.

Existing records are not modified in place.

For example:

```text
PUT(name, Nishanth)
PUT(project, Hackathon)
PUT(name, Mohith)
DELETE(project)
```

The file contains the history of these operations.

The latest valid operation determines the current value of each key.

The in-memory dictionary represents the latest recovered state.

### 3.2 Binary Record Format

Each record contains a header, key, value and checksum.

| Field | Size |
|---|---:|
| Magic number | 4 bytes |
| Operation type | 1 byte |
| Key length | 4 bytes |
| Value length | 4 bytes |
| Key | Variable |
| Value | Variable |
| CRC32 checksum | 4 bytes |

The fixed header uses the Python structure format:

```python
struct.Struct(">4sBII")
```

The magic number is:

```text
KVR1
```

Operation codes:

| Operation | Code |
|---|---:|
| PUT | 1 |
| DELETE | 2 |
| BEGIN | 3 |
| COMMIT | 4 |

The checksum is calculated using CRC32 to detect corrupted or incomplete records.

Total record size:

```text
17 + key_length + value_length bytes
```

The key and value are stored as byte strings.

### 3.3 Data Flow for PUT

```text
Client requests PUT
        |
        v
Validate key and value
        |
        v
Acquire thread lock
        |
        v
Acquire process lock
        |
        v
Check database file state
        |
        v
Encode binary record
        |
        v
Append record to file
        |
        v
Synchronize using fsync()
        |
        v
Update in-memory dictionary
        |
        v
Return success
```

The in-memory index is updated only after the append operation completes successfully.

## 4. Atomic Batch Architecture

A batch can contain up to 10,000 operations.

The database uses transaction boundary records to identify complete batches.

### 4.1 Batch File Structure

```text
BEGIN(transaction_id, operation_count)

PUT(key_1, value_1)
DELETE(key_2)
PUT(key_3, value_3)

COMMIT(transaction_id, operation_count)
```

The BEGIN and COMMIT records contain the same transaction ID and expected operation count.

### 4.2 Batch Execution Sequence

1. Acquire the thread lock.
2. Acquire the process lock.
3. Validate all operations.
4. Generate a unique transaction ID.
5. Encode the BEGIN record.
6. Encode all batch operations.
7. Encode the COMMIT record.
8. Append the complete transaction sequence.
9. Synchronize the file using `fsync()`.
10. Update the in-memory dictionary.
11. Return success.

The implementation writes the transaction records and commit marker in one append sequence followed by synchronization.

### 4.3 Batch Recovery

During recovery:

- BEGIN starts a pending transaction.
- Operations inside the transaction are buffered.
- COMMIT is checked against the transaction ID and operation count.
- Only a complete, valid transaction is applied to the in-memory index.
- An incomplete transaction is discarded by truncating the file back to its BEGIN position.

This prevents partially recovered batches from becoming visible.

## 5. Concurrency Architecture

The implementation uses two levels of locking.

### 5.1 Thread-Level Locking

Python's `threading.RLock` protects operations within a process.

It prevents simultaneous threads from modifying the in-memory index and database state without coordination.

### 5.2 Process-Level Locking

The `FileLock` implementation uses platform-specific locking.

**Windows:**
- Uses a named Windows mutex.
- The mutex name is derived from the normalized database path.
- Processes accessing the same database coordinate through the same mutex.

**POSIX systems:**
- Uses `fcntl.flock()` with a lock file.
- The lock is acquired before database operations and released afterward.

### 5.3 GET During Batch

A GET operation acquires the same locks as a batch operation.

Therefore, it cannot read the in-memory dictionary while a batch is being applied.

It observes the state before the batch or after the complete batch, rather than an intermediate in-memory state.

## 6. Crash Recovery Architecture

The recovery engine reconstructs the database state by scanning the authoritative file.

### Recovery Flow

```text
              DATABASE OPEN
                    |
                    v
              Acquire file lock
                    |
                    v
              Open database file
                    |
                    v
              Read record header
                    |
                    v
           Validate magic and lengths
                    |
                    v
              Read key and value
                    |
                    v
              Validate CRC32
                    |
                    v
              Identify operation
                    |
               +----+----+
               |         |
               v         v
            PUT/DELETE  BEGIN/COMMIT
               |         |
               v         v
          Apply valid  Buffer transaction
          standalone        |
          operation         v
                       Validate COMMIT
                           |
                       +---+---+
                       |       |
                       v       v
                     Valid   Invalid/
                     commit  incomplete
                       |       |
                       v       v
                     Apply   Discard batch
                     batch   from BEGIN
                       |       |
                       +---+---+
                           |
                           v
                    Continue scanning
                           |
                           v
                    Truncate invalid tail
                           |
                           v
                    Rebuild in-memory index
                           |
                           v
                      OPEN COMPLETE
```

### Recovery Guarantees

The recovery process checks:

- Record magic numbers.
- Operation types.
- Key and value lengths.
- CRC32 checksums.
- Transaction IDs.
- Transaction operation counts.
- Incomplete transaction boundaries.

If the final record is incomplete or invalid, the file is truncated to the last valid committed position.

Previously valid operations remain available.

The recovery implementation also avoids unnecessary crash-test checkpoint calls when no test hook is configured. This reduces overhead during normal database recovery.

## 7. Durability, Atomicity and Recoverability

### 7.1 Durability

PUT and DELETE operations append their records and call `fsync()` before updating the in-memory index and returning success.

Batch operations append their transaction records and commit marker, synchronize the file and then update the in-memory index.

This provides the intended durability boundary: successful operations are synchronized before success is returned.

Process-termination tests provide evidence for the tested crash points. They do not constitute a complete power-loss or hardware-failure guarantee.

### 7.2 Atomicity

A batch is represented by BEGIN and COMMIT records.

The recovery engine does not apply pending batch operations until it finds a matching valid COMMIT record.

If the process terminates before a complete transaction is committed, recovery discards that transaction.

### 7.3 Recoverability

The database rebuilds its in-memory dictionary by scanning the persistent log.

Checksums and record validation help identify corrupted or incomplete records.

Incomplete trailing records and transactions are removed during recovery.

## 8. Crash-Safe Compaction

The append-only design creates obsolete records when keys are updated or deleted.

Compaction rewrites only the currently active key-value pairs.

### Compaction Sequence

```text
          CURRENT DATABASE
                  |
                  v
             Acquire locks
                  |
                  v
         Remove stale temporary file
                  |
                  v
        Collect active key-value pairs
                  |
                  v
        Write new temporary database
                  |
                  v
          Complete temporary file
                  |
                  v
         Atomically replace database
                  |
                  v
          Synchronize directory
                  |
                  v
           Update file signature
                  |
                  v
            COMPACTION COMPLETE
```

The original database remains authoritative until the temporary database is complete and the replacement operation occurs.

The temporary file is cleaned up after normal completion or an exception.

A stale temporary file from a previous interrupted compaction is removed before a new compaction begins.

Testing covers checkpoints before and after replacement. It does not prove termination at every possible instruction inside the operating-system replacement call.

## 9. Database Size Management

The database supports an optional configurable maximum file size.

The storage layer:

1. Calculates the size of the records being appended.
2. Checks the current file size.
3. Checks whether the append would exceed the configured limit.
4. Rejects the operation if the limit would be exceeded.
5. Attempts to restore the original file length if an append raises an exception after writing has started.

Batch operations are checked as a complete append sequence before writing.

Compaction also applies the configured size limit to its temporary database file.

DELETE records consume log space. Space is made reusable by compaction, not immediately by deletion.

## 10. Implementation Constraints

| Requirement | Implementation |
|---|---|
| Key size | 1–1024 bytes |
| Value size | 0–1 MB |
| Maximum batch size | 10,000 operations |
| Record integrity | CRC32 |
| Transaction boundaries | BEGIN and COMMIT |
| In-memory index | Python dictionary |
| Thread synchronization | `threading.RLock` |
| Windows process synchronization | Named Windows mutex |
| POSIX process synchronization | `fcntl.flock()` |
| File synchronization | `os.fsync()` |
| Compaction replacement | `os.replace()` |
| Recovery | Sequential log scan |

## 11. Scenario Verification Matrix

**Verification status:** The matrix distinguishes implemented mechanisms from test evidence. A mechanism being implemented does not automatically mean that every exact crash point has been tested.

### Group A — Individual writes and durability

| ID | Failure scenario | Expected recovery result | Verification status |
|---|---|---|---|
| A1 | Kill before PUT bytes are written | Old value or absent key | Passed: controlled termination before first physical write |
| A2 | Kill during a partial PUT write | Old value or absent key; no corrupted value | Passed: torn-record recovery test |
| A3 | Kill after complete record write but before fsync | Old or new value may be recovered | Passed: complete record was recovered in the tested process-termination case; not a power-loss test |
| A4 | Kill after fsync but before acknowledgement | New value present | Passed: forced termination after fsync and before return |
| A5 | Kill after successful PUT | New value present | Passed: 100 acknowledged writes survived forced termination |
| A6 | Kill during DELETE | Old value remains if DELETE is incomplete | Passed: partial DELETE recovery test |
| A7 | Kill during 1 MB PUT | Old or new value; no partial value | Passed: interrupted large-value write recovery test |

### Group B — Atomic batch operations

| ID | Failure scenario | Expected recovery result | Verification status |
|---|---|---|---|
| B1 | Kill after some batch operations are written | Entire batch discarded | Passed: termination after three of ten operations |
| B2 | Kill after all operations but before COMMIT | Entire batch discarded | Passed |
| B3 | Kill after COMMIT write but before fsync | Entire batch or no batch may be recovered | Passed: complete batch recovered in the tested process-termination case; not a power-loss test |
| B4 | Kill after successful batch synchronization but before acknowledgement | Entire batch present | Passed: after-sync checkpoint test |
| B5 | PUT and DELETE same key in one batch | Operations applied in order | Passed |
| B6 | Kill halfway through a 10,000-operation batch | Entire batch discarded | Passed: partial batch termination; recovery measured below two seconds |

### Group C — Recovery and compaction

| ID | Failure scenario | Expected recovery result | Verification status |
|---|---|---|---|
| C1 | Kill during recovery replay | Next recovery reconstructs valid state | Passed: controlled termination during replay |
| C2 | Kill during torn-tail truncation | Next recovery succeeds | Passed: termination after truncation and before flush/fsync |
| C3 | Kill before compaction replacement | Original database remains authoritative | Passed: temporary-file and pre-replacement checkpoints |
| C4 | Kill during replacement | Old or new complete database | Partial: 20 termination attempts around the replacement boundary passed; termination inside the OS call was not demonstrated |
| C5 | Kill after replacement before temporary cleanup | Data remains intact; stale temporary file handled | Passed: post-replacement checkpoint and stale temporary-file regression test |
| C6 | Kill during close | Database remains recoverable | Passed: controlled termination during close |

### Group D — Concurrent operations

| ID | Failure scenario | Expected recovery result | Verification status |
|---|---|---|---|
| D1 | Concurrent writers on different keys with random termination | Acknowledged operations remain present | Passed: two workers, 30 acknowledged writes, oracle verification; termination was coordinated rather than guaranteed during simultaneous physical writes |
| D2 | Concurrent writers on the same key | One complete value wins | Passed: two processes performed 200 PUTs; final value was one complete submitted value |
| D3 | Long batch and single PUT operations concurrently | Batch all-or-nothing; acknowledged PUTs present | Passed: partial batch write was interrupted, batch discarded and 30 individual writes recovered; interruption occurred after five bytes of the first batch record |
| D4 | GET during batch | Before or after batch state only | Passed: reader waited for batch lock and observed the complete after-state; no partial batch visibility |

### Group E — Size limits and disk-full handling

| ID | Failure scenario | Expected recovery result | Verification status |
|---|---|---|---|
| E1 | PUT exceeds configured size limit | Error; existing data intact | Passed: configured size-limit test |
| E2 | Batch exceeds configured size limit | Error; no batch operations visible | Passed: configured size-limit test |
| E3 | Kill after size-limit rejection | Previous acknowledged data remains | Partial: restart after failed write was tested; forced termination at the exact rejection point was not tested |
| E4 | Size limit prevents compaction | Original database remains intact | Passed: failed compaction preserves original database |
| E5 | Delete records and compact | Storage space becomes reusable | Passed: delete, compaction, new write and restart test |

Additional disk-full evidence:

- Partial PUT failure rollback passed.
- Partial batch failure rollback passed.
- Recovery after simulated disk-full failure passed.

These tests simulate `OSError` with error number 28 after a partial physical write. They do not simulate a real disk becoming physically full.

### Group F — Torture testing and recovery performance

| ID | Failure scenario | Expected recovery result | Verification status |
|---|---|---|---|
| F1 | 500 random kill/restart iterations | Acknowledged data remains intact | Passed: 500 consecutive process terminations and restarts; 1,204 acknowledged operations; independent oracle verified after every restart |
| F2 | Repeated kill/restart during recovery | Recovery remains idempotent | Passed: three consecutive recovery interruptions after replaying three records per attempt; final recovery verified all 100 acknowledged records |
| F3 | One million keys followed by process termination and restart | All acknowledged keys recovered within two seconds | Passed: writer forcefully terminated after one million acknowledged PUTs; separate recovery process verified all one million keys and values in 1.7412 seconds |
| F4 | Truncate final byte of database file | Recovery succeeds; incomplete final record discarded | Passed: exactly one byte removed from the end of an appended unacknowledged record; all three acknowledged records survived and the incomplete tail was removed |

#### F1 — Detailed Evidence

- Crash iterations: 500.
- Random seed: 42.
- Acknowledged operations: 1,204.
- Final oracle records: 25.
- Oracle comparison: passed after every restart.
- Execution time: 57.61 seconds.

The test randomizes termination between acknowledged database operations. It does not simulate termination at every possible physical instruction inside a record write. Separate torn-write tests cover incomplete physical writes.

#### F2 — Detailed Evidence

- Initial acknowledged records: 100.
- Recovery interruptions: 3 consecutive child-process terminations.
- Each interruption occurred after three standalone records were replayed.
- Final normal recovery: successful.
- All 100 records and their values: verified.

#### F3 — Detailed Evidence

| Measurement | Result |
|---|---:|
| Acknowledged keys written | 1,000,000 |
| Batch size | 1,000 |
| Process termination | Forced termination |
| Recovery process | Separate process |
| Recovered keys | 1,000,000 |
| Verified key-value pairs | 1,000,000 |
| Recovery time | 1.7412 seconds |
| Required recovery time | Under 2.0000 seconds |

The recovery checkpoint optimization avoids unnecessary test-hook calls when no recovery test hook is configured.

The measured recovery time improved from 2.0436 seconds to 1.7412 seconds in the recorded runs.

#### F4 — Detailed Evidence

- Three acknowledged PUT records were created.
- An additional unacknowledged record was appended.
- Exactly one final byte was removed from the database file.
- Database opening and recovery succeeded.
- All three acknowledged records remained intact.
- The incomplete unacknowledged record was discarded.
- The database file returned to the acknowledged-data boundary.

### Group G — Explicit exclusions

The following are outside the stated project scope:

- Random bit rot in the middle of the database file.
- Network access and distributed operation.
- Sharding or multiple authoritative database files.
- Multi-batch transactions and isolation levels.
- Recovery performance guarantees beyond the specified two-second target.

## 12. Performance and Test Evidence

### 12.1 One-Million-Key Crash-Recovery Benchmark

The following benchmark was executed on the development environment:

| Metric | Result |
|---|---:|
| Records written | 1,000,000 |
| Batch size | 1,000 |
| Database size in earlier benchmark | Approximately 56.13 MB |
| Earlier insertion time | 7.16 seconds |
| Forced process termination | Yes |
| Recovery process | Separate process |
| Recovered records | 1,000,000 |
| Verified values | All 1,000,000 |
| Recovery time | 1.7412 seconds |

Performance measurements are environment-dependent and should be repeated on the final demonstration machine.

### 12.2 Complete Automated Test Suite

Final full-suite command:

```powershell
python -m pytest -v
```

Latest recorded result:

| Metric | Result |
|---|---:|
| Tests collected | 64 |
| Tests passed | 64 |
| Tests failed | 0 |
| Execution time | 101.58 seconds |

**Result: 64 passed in 101.58 seconds.**

This result was recorded on Windows with Python 3.14.0 and pytest 9.2.0.

A passing automated test suite is evidence for the implemented test cases. It does not automatically establish that every possible crash timing or hardware failure has been covered.

## 13. Known Limitations and Further Verification

The following areas require additional testing before claiming complete verification:

1. Termination at every possible point inside the physical write loop.
2. Power-loss behavior between complete record writes and durable synchronization.
3. Termination inside the operating-system `os.replace()` call.
4. Forced termination at the exact size-limit rejection point.
5. Real physical disk-full behavior on the final demonstration environment.
6. Random corruption of arbitrary middle-of-file bytes.

The following Group F scenarios have dedicated passing tests: F1, F2, F3 and F4.

The implementation and the test evidence should be evaluated separately. No untested scenario should be presented as proven.

## 14. Source Code Structure

```text
Kill-It-and-Prove-It/
|
+-- src/
|   +-- __init__.py
|   +-- database.py
|   +-- storage.py
|   +-- recovery.py
|   +-- file_lock.py
|
+-- tests/
|   +-- test_basic.py
|   +-- test_batch.py
|   +-- test_crash_recovery.py
|   +-- test_concurrency.py
|   +-- test_thread_concurrency.py
|   +-- test_crash_durability.py
|   +-- test_compaction.py
|   +-- test_compaction_crash.py
|   +-- test_size_limits.py
|   +-- test_disk_full.py
|   +-- test_large_batch_crash.py
|   +-- test_d1_concurrent_crash.py
|   +-- test_same_key_concurrency.py
|   +-- test_batch_writer_crash.py
|   +-- test_get_during_batch.py
|   +-- test_recovery_crash.py
|   +-- test_recovery_truncation_crash.py
|   +-- test_repeated_recovery_crash.py
|   +-- test_close_crash.py
|   +-- test_delete_crash.py
|   +-- test_large_value_crash.py
|   +-- test_10k_batch_crash.py
|   +-- test_compaction_replace_race.py
|   +-- test_f1_500_random_crash_restarts.py
|   +-- test_f3_million_key_crash_recovery.py
|   +-- test_final_byte_truncation.py
|   +-- benchmark_database.py
|
+-- torture_tests/
|   +-- generator.py
|   +-- oracle.py
|   +-- runner.py
|   +-- crash_runner.py
|   +-- concurrent_runner.py
|   +-- large_data_runner.py
|
+-- main.py
+-- README.md
+-- ARCHITECTURE.md
+-- BREAKING.md
+-- PROMPTS.md
```

---

**End of Architecture Document**

### 12.3 Standalone Torture Launcher Verification

The project provides two independently selectable torture-test modes.

Randomized operation testing: `python torture.py --mode random --iterations 500 --seed 42`

Windows crash-torture testing: `.\torture.cmd --iterations 500 --seed 42`

Observed crash-torture result (seed 42):
- Iterations: 500
- Acknowledged writes: 2,729
- Recovered records: 2,729
- Independent oracle verification: passed

The crash runner terminates the writer process between acknowledged operations and verifies that acknowledged data survives reopening. This test does not claim to simulate power loss or termination during an individual physical write.
