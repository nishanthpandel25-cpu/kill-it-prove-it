# Kill It and Prove It

A lightweight, embedded, single-process key-value database engine built from scratch using Python.

The project focuses on persistent storage, atomic batch operations, concurrent access, crash recovery, and crash-safe compaction. Its reliability is evaluated through automated tests and process-crash torture testing.

## 1. Project Overview

Kill It and Prove It is a custom database engine that stores key-value pairs in a binary file.

It uses an append-only storage architecture and an in-memory hash index to provide efficient access to stored records.

The database is designed to:

- Preserve acknowledged writes across process crashes.
- Execute batches atomically.
- Support concurrent database access.
- Recover incomplete operations after unexpected termination.
- Compact the database without losing committed data.
- Detect corrupted or incomplete storage records.

The implementation uses Python standard-library modules for storage, synchronization, locking, and recovery.

## 2. Features

### Database operations

- **PUT:** Insert or update a key-value pair.
- **GET:** Retrieve a value using its key.
- **DELETE:** Remove a key.
- **BATCH:** Execute multiple operations atomically.
- **OPEN:** Open a database and recover its state.
- **CLOSE:** Close the database.

### Storage and reliability

- Append-only binary storage.
- CRC32 record integrity checks.
- Transaction records using BEGIN and COMMIT.
- Recovery through binary log replay.
- Thread-level synchronization.
- Cross-process file locking.
- Crash-safe compaction.
- Configurable maximum database file size.
- Rollback protection for failed writes.
- Fail-stop behaviour when rollback cannot be confirmed.

### Supported limits

| Requirement | Limit |
|---|---:|
| Key size | 1–1024 bytes |
| Value size | Up to 1 MB |
| Number of keys | At least 1 million |
| Operations per batch | Up to 10,000 |

## 3. Technology Stack

| Technology | Purpose |
|---|---|
| Python 3 | Database implementation |
| Python standard library | Storage, locking, synchronization and recovery |
| pytest | Automated testing |
| Git | Version control |

No external database engine or storage library such as SQLite, MongoDB, Redis, or RocksDB is used.

## 4. Project Structure

```text
Kill-It-and-Prove-It/
│
├── src/
│   ├── __init__.py
│   ├── database.py
│   ├── storage.py
│   ├── recovery.py
│   ├── file_lock.py
│   ├── compaction.py
│   ├── index.py
│   └── transaction.py
│
├── tests/
│   ├── test_basic.py
│   ├── test_batch.py
│   ├── test_concurrency.py
│   ├── test_thread_concurrency.py
│   ├── test_crash_durability.py
│   ├── test_crash_recovery.py
│   ├── test_compaction.py
│   ├── test_compaction_crash.py
│   ├── test_compaction_replace_race.py
│   ├── test_size_limits.py
│   ├── test_disk_full.py
│   ├── test_large_batch_crash.py
│   ├── test_large_value_crash.py
│   └── ...
│
├── torture_tests/
│   ├── generator.py
│   ├── oracle.py
│   ├── runner.py
│   ├── crash_runner.py
│   ├── concurrent_runner.py
│   ├── large_data_runner.py
│   ├── process_harness.py
│   ├── process_crash_runner.py
│   ├── process_batch_crash_runner.py
│   ├── process_large_crash_runner.py
│   ├── process_compaction_crash_runner.py
│   ├── process_recovery_crash_runner.py
│   └── process_disk_full_runner.py
│
├── main.py
├── torture.py
├── batch.json
├── README.md
├── ARCHITECTURE.md
├── BREAKING.md
├── PROMPTS.md
└── .gitignore
```

The `tests/` directory contains additional focused tests beyond the examples shown above.

## 5. Installation

### Requirements

- Python 3.10 or later.
- Git.
- Windows, Linux, or macOS.
- pytest for running automated tests.

The database runtime itself uses only Python standard-library modules.

### Step 1: Clone the repository

```powershell
git clone <YOUR_GITHUB_REPOSITORY_URL>
```

Replace the placeholder with your actual repository URL.

### Step 2: Enter the project directory

```powershell
cd Kill-It-and-Prove-It
```

### Step 3: Create a virtual environment

```powershell
python -m venv .venv
```

### Step 4: Activate the virtual environment

```powershell
.venv\Scripts\Activate.ps1
```

### Step 5: Install pytest

```powershell
python -m pip install pytest
```

## 6. Command-Line Interface

The command-line interface is implemented in `main.py`.

### Available commands

| Command | Purpose |
|---|---|
| `open` | Open a database and perform recovery |
| `put` | Insert or update a key |
| `get` | Retrieve a key |
| `del` | Delete a key |
| `batch` | Execute operations from a batch file |
| `dump` | Display stored records |
| `check` | Verify database recovery and consistency |
| `serve` | Start an interactive command server |

### 6.1 Open a database

```powershell
python main.py open data/database.db
```

Configure a maximum file size:

```powershell
python main.py open data/database.db --max-bytes 10485760
```

### 6.2 Insert a record

```powershell
python main.py put data/database.db name Nishanth
```

### 6.3 Retrieve a record

```powershell
python main.py get data/database.db name
```

### 6.4 Delete a record

```powershell
python main.py del data/database.db name
```

### 6.5 Execute a batch

Create a text file containing operations:

```text
put name Nishanth
put college SIT
del temporary_key
```

Execute the batch:

```powershell
python main.py batch data/database.db batch.txt
```

The batch file supports line-oriented `put` and `del` operations. Empty lines and comments are skipped.

### 6.6 Dump database contents

```powershell
python main.py dump data/database.db
```

### 6.7 Check database integrity

```powershell
python main.py check data/database.db
```

### 6.8 Start the interactive server

```powershell
python main.py serve data/database.db
```

The server prints `READY` when it starts.

Example input:

```text
op1 put name Nishanth
op2 get name
op3 del name
op4 get name
```

Example responses:

```text
READY
ACK op1
VALUE op2 Nishanth
ACK op3
NOT_FOUND op4
```

The server returns an acknowledgement only after the requested write operation has completed successfully and its durability operation has returned.

For a batch, the server accepts the batch operation through its command protocol.

## 7. Using the Database API

The database can also be accessed directly from Python programs.

### 7.1 Create or open a database

```python
from src.database import Database

db = Database("data/database.db")
```

### 7.2 Insert records

```python
db.put(b"name", b"Nishanth")
db.put(b"age", b"20")
db.put(b"college", b"SIT")
```

### 7.3 Retrieve a record

```python
name = db.get(b"name")
print(name)
```

### 7.4 Delete a record

```python
db.delete(b"age")
```

### 7.5 Execute an atomic batch

```python
db.batch([
    ("put", b"name", b"Nishanth"),
    ("put", b"age", b"20"),
    ("put", b"college", b"SIT"),
    ("delete", b"temporary_key")
])
```

The batch is represented using BEGIN and COMMIT transaction records. Recovery applies only committed transactions.

### 7.6 Close the database

```python
db.close()
```

### 7.7 Configure the maximum database file size

```python
db = Database(
    "data/database.db",
    max_size=10 * 1024 * 1024
)
```

This example configures a maximum database file size of 10 MB.

The maximum size is a configuration for the database instance; it is not automatically persisted as database metadata.

## 8. Running Automated Tests

Run the complete test suite:

```powershell
python -m pytest -q
```

Run an individual test module:

```powershell
python -m pytest tests/test_basic.py -v
```

Run batch and compaction tests:

```powershell
python -m pytest tests/test_batch.py tests/test_compaction.py tests/test_compaction_crash.py -v
```

## 9. Torture Testing

The project includes a dedicated torture-test launcher:

```powershell
python torture.py --help
```

The launcher provides seven modes.

| Mode | Purpose |
|---|---|
| `random` | Concurrent random operations with process termination and oracle verification |
| `crash` | Randomized crash and restart testing |
| `compaction` | Process termination at compaction checkpoints |
| `recovery-crash` | Process termination during recovery checkpoints |
| `large-values` | Crash testing involving values up to 1 MB |
| `large-batch` | Crash testing involving batches of 10,000 operations |
| `disk-full` | Database capacity-limit and failed-operation verification |

### 9.1 Random process-crash testing

```powershell
python torture.py --iterations 500 --seed 12345 --workers 4
```

This mode uses concurrent writers, random PUT, DELETE and BATCH operations, random-delay process termination, an independent oracle, and post-recovery verification.

### 9.2 Random crash testing

```powershell
python torture.py --iterations 100 --seed 42 --mode crash
```

### 9.3 Compaction crash testing

```powershell
python torture.py --iterations 100 --seed 7 --mode compaction
```

### 9.4 Recovery crash testing

```powershell
python torture.py --iterations 100 --seed 42 --mode recovery-crash
```

### 9.5 Large-value crash testing

```powershell
python torture.py --iterations 100 --seed 999 --mode large-values
```

### 9.6 Large-batch crash testing

```powershell
python torture.py --iterations 100 --seed 42 --mode large-batch
```

This mode requires exactly two workers.

### 9.7 Disk-full testing

```powershell
python torture.py --iterations 100 --seed 42 --mode disk-full
```

### Available options

| Argument | Description |
|---|---|
| `--mode` | Select one of the seven torture modes |
| `--iterations` | Number of test iterations |
| `--seed` | Random seed for reproducibility |
| `--operations-per-iteration` | Writes per crash iteration |
| `--workers` | Number of concurrent workers where supported |

The default mode is `random`, with 500 iterations and four workers.

Torture testing supplements the automated test suite. Different modes exercise different failure scenarios, and a successful run does not prove correctness for every possible operating-system or hardware failure.

## 10. Verified Development Results

The following results were reported during development and integration testing.

### Automated tests

| Metric | Result |
|---|---:|
| Automated tests passed | 64 |
| Automated tests failed | 0 |
| Latest recorded execution time | 97.21 seconds |

### Process-crash torture testing

| Test | Recorded result |
|---|---|
| Random process-crash mode | 500/500 iterations passed |
| Acknowledged operations in that run | 13,085 |
| Concurrent writers | 4 |
| Compaction mode | 100/100 iterations passed |
| Recovery-crash mode | 100/100 iterations passed |
| Large-batch mode | 5/5 iterations passed |
| Disk-full mode | 5/5 iterations passed |
| Crash mode | 3/3 iterations passed |
| Large-values mode | 5/5 iterations passed in an earlier run |

The 100-iteration large-values command has not been completed as part of the latest integration sequence.

### CLI verification

The following operations were manually verified:

- Database open.
- PUT and GET.
- DELETE and missing-key response.
- Batch execution.
- Database dump.
- Database check and recovery.
- Interactive `serve` protocol with PUT, GET, DELETE and NOT_FOUND responses.

### Million-record benchmark

A million-record benchmark was recorded during development:

| Metric | Recorded result |
|---|---:|
| Records inserted | 1,000,000 |
| Insertion time | 7.16 seconds |
| Database size | 56.13 MB |
| Recovery time | 1.64 seconds |
| Recovered records | 1,000,000 |

The benchmark verified the recovered record count and selected values, rather than comparing every value.

All performance measurements are environment-dependent and should not be interpreted as universal guarantees.

## 11. Documentation

Additional project documentation:

- `ARCHITECTURE.md` — database architecture, binary record format, recovery, locking and scenario verification.
- `BREAKING.md` — crash-testing methodology, failure experiments, observed results and limitations.
- `PROMPTS.md` — development prompt history.

## 12. Reliability and Testing Limitations

The project uses process-crash testing, independent expected-state verification, transaction recovery tests, compaction checkpoints and disk-capacity tests.

Some important distinctions:

- A process kill is not identical to sudden power loss or physical storage failure.
- Checkpoint-controlled termination verifies selected internal failure boundaries; it does not represent every possible timing.
- A configured maximum database size is not identical to an actual operating-system disk-full event.
- A passing finite torture run is evidence for the tested scenarios, not a mathematical proof of correctness.
- The `serve` command processes requests through its command loop; concurrent database access is also tested separately through the database and process-level test harnesses.

See `BREAKING.md` and `ARCHITECTURE.md` for the detailed guarantees, test evidence and known limitations.

## 13. Project Objective

The objective is to build a database engine from scratch and demonstrate its reliability through systematic failure testing.

The project emphasizes:

- Data durability.
- Atomic transactions.
- Concurrency control.
- Crash recovery.
- Storage integrity.
- Independent verification.

**Kill It and Prove It — reliability demonstrated through testing, not assumptions.**
