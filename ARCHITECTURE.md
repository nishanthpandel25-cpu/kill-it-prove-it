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
             +-----------+-----------+
             |           |           |
             v           v           v
          PUT/GET     BATCH       COMPACTION
          DELETE      HANDLER       HANDLER
             |           |           |
             +-----------+-----------+
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
13 + key_length + value_length + 4 bytes
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
             +------+------+
             |             |
             v             v
          PUT/DELETE    BEGIN/COMMIT
             |             |
             v             v
        Apply valid     Buffer transaction
        standalone          |
        operation           v
             |         Validate COMMIT
             |             |
             |        +----+----+
             |        |         |
             |        v         v
             |     Valid     Invalid/
             |     commit    incomplete
             |        |         |
             |        v         v
             |     Apply     Discard batch
             |     batch     from BEGIN
             |        |         |
             +--------+---------+
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

## 7. Durability, Atomicity and Recoverability

### 7.1 Durability

PUT and DELETE operations append their records and call `fsync()` before updating the in-memory index and returning success.

Batch operations append their transaction records and commit marker, synchronize the file and then update the in-memory index.

This provides the intended durability boundary: successful operations are synchronized before success is returned.

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

**Verification status:** The matrix below distinguishes implemented mechanisms from test evidence. A mechanism being implemented does not automatically mean that every exact crash point has been tested.

| ID | Failure scenario | Expected recovery result | Mechanism | Verification status |
|---|---|---|---|---|
| A1 | Kill before PUT bytes are written | Old value or absent key | Append-only write | Not individually verified |
| A2 | Kill during a partial PUT write | Old value or absent key; no corrupted value | CRC32 and tail truncation | Not individually verified |
| A3 | Kill after complete record write but before fsync | Old or new value | Record recovery | Not individually verified |
| A4 | Kill after fsync but before acknowledgement | New value present | `fsync()` before success | Not individually verified |
| A5 | Kill after successful PUT | New value present | Durable append | Covered by durability testing; exact checkpoint not established |
| A6 | Kill during DELETE | Old value or absent key | Append-only DELETE record | Not individually verified |
| A7 | Kill during 1 MB PUT | Old or new value; no partial value | CRC32 and recovery | Not individually verified |
| B1 | Kill after some batch operations are written | Entire batch discarded | BEGIN boundary | Not individually verified |
| B2 | Kill after all operations but before COMMIT | Entire batch discarded | COMMIT validation | Not individually verified |
| B3 | Kill after COMMIT write but before fsync | Entire batch or no batch | Transaction recovery | Not individually verified |
| B4 | Kill after successful batch acknowledgement | Entire batch present | Append and fsync before acknowledgement | Covered by batch durability tests; exact checkpoint not established |
| B5 | PUT and DELETE same key in one batch | Operations applied in order | Sequential batch application | Not individually verified |
| B6 | Kill halfway through a 10,000-operation batch | Entire batch discarded | Transaction boundaries | Not individually verified |
| C1 | Kill during recovery replay | Next recovery reconstructs valid state | Log replay | Not individually verified |
| C2 | Kill during torn-tail truncation | Next recovery succeeds | Tail validation and truncation | Not individually verified |
| C3 | Kill before compaction replacement | Original database remains authoritative | Temporary file | Verified at compaction checkpoints |
| C4 | Kill during replacement | Old or new complete database | `os.replace()` | Verified immediately before and after replacement; not during the OS call |
| C5 | Kill after replacement before temporary cleanup | Data remains intact; stale temporary file handled | Replacement and cleanup | Replacement checkpoint tested; stale-file recovery tested separately |
| C6 | Kill during close | Database remains recoverable | Persistent log | Not individually verified |
| D1 | Concurrent writers on different keys | Acknowledged operations remain present | Process locking | Concurrent writer testing completed |
| D2 | Concurrent writers on same key | One complete value wins | Process locking | Not individually verified |
| D3 | Long batch and single PUT operations concurrently | Batch all-or-nothing; acknowledged PUTs present | Shared locks | Not individually verified |
| D4 | GET during batch | Before or after batch state only | Shared locks | Not individually verified |
| E1 | PUT exceeds configured size limit | Error; existing data intact | Size check | Disk-full and size-limit tests completed; exact scenario not separately established |
| E2 | Batch exceeds configured size limit | Error; no batch operations visible | Pre-append size check | Disk-full and batch tests completed; exact scenario not separately established |
| E3 | Kill after size-limit rejection | Previous acknowledged data remains | Append rollback and recovery | Not individually verified |
| E4 | Size limit reached during compaction | Original database remains intact if compaction fails before replacement | Temporary-file compaction | Not individually verified |
| E5 | Delete records and compact | Storage space becomes reusable | Compaction | Verified by compaction tests |
| F1 | 500 random kill/restart iterations | Acknowledged data remains intact | Recovery and oracle | Random crash runner completed; its kill points were after acknowledged writes |
| F2 | Repeated kill/restart during recovery | Recovery remains idempotent | Recovery replay | Not individually verified |
| F3 | One million keys followed by restart | All acknowledged keys recovered within 2 seconds | Log replay and in-memory index | Million-key benchmark completed; measured recovery was 1.64 seconds |
| F4 | Truncate final byte of database file | Recovery succeeds; incomplete final record discarded | CRC32 and tail truncation | Not individually verified |

## 12. Performance Evidence

The following benchmark was previously run on the development environment:

| Metric | Result |
|---|---:|
| Records inserted | 1,000,000 |
| Database size | Approximately 56.13 MB |
| Insertion time | 7.16 seconds |
| Recovery time | 1.64 seconds |
| Recovered records | 1,000,000 |

The benchmark verified the recovered record count and selected values. It did not independently compare every value.

Performance measurements are environment-dependent and should be repeated on the final demonstration machine.

## 13. Known Limitations and Further Verification

The following areas require additional testing before claiming complete verification:

1. Exact process termination during the physical write loop.
2. Exact process termination between COMMIT write and fsync.
3. Concurrent operations on the same key.
4. Concurrent GET during an active batch.
5. Process termination during recovery truncation.
6. Process termination during the operating-system replacement call.
7. Recovery idempotency across repeated interruptions.
8. Disk-full failures during physical writes and compaction.
9. Exact one-million-key recovery performance on the final demonstration machine.

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
|   +-- benchmark_database.py
|
+-- torture/
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