# Kill It and Prove It

A lightweight, embedded key-value database engine built from scratch using Python.

The project focuses on persistent storage, atomic batch operations, concurrent access, crash recovery and crash-safe compaction.

## 1. Project Overview

Kill It and Prove It is a custom database engine that stores key-value pairs in a binary file.

It uses an append-only storage architecture and an in-memory hash index to provide efficient access to stored records.

The database is designed to recover its state after unexpected process termination and to maintain transaction consistency during batch operations.

## 2. Features

- PUT: Insert or update key-value pairs.
- GET: Retrieve values using keys.
- DELETE: Delete stored keys.
- BATCH: Execute multiple operations atomically.
- OPEN: Open the database and recover its state.
- CLOSE: Close the database.
- Crash recovery using binary log replay.
- CRC32 record integrity checks.
- Thread-level synchronization.
- Cross-process file locking.
- Crash-safe compaction.
- Configurable maximum database file size.
- Support for values up to 1 MB.
- Support for batches containing up to 10,000 operations.
- Independent oracle-based testing.

## 3. Technology Stack

| Technology | Purpose |
|---|---|
| Python 3 | Database implementation |
| Python standard library | Storage, locking and synchronization |
| pytest | Automated testing |
| Git | Version control |

No external database engine such as SQLite, MongoDB, Redis or RocksDB is used.

## 4. Project Structure

```text
Kill-It-and-Prove-It/
│
├── src/
│   ├── __init__.py
│   ├── database.py
│   ├── storage.py
│   ├── recovery.py
│   └── file_lock.py
│
├── tests/
│   ├── test_basic.py
│   ├── test_batch.py
│   ├── test_crash_recovery.py
│   ├── test_concurrency.py
│   ├── test_thread_concurrency.py
│   ├── test_crash_durability.py
│   ├── test_compaction.py
│   ├── test_compaction_crash.py
│   ├── test_size_limits.py
│   ├── test_disk_full.py
│   ├── test_large_batch_crash.py
│   └── benchmark_database.py
│
├── torture/
│   ├── generator.py
│   ├── oracle.py
│   ├── runner.py
│   ├── crash_runner.py
│   ├── concurrent_runner.py
│   └── large_data_runner.py
│
├── diagnostics/
│   ├── check_concurrency.py
│   └── test_mutex.py
│
├── diagrams/
├── data/
│
├── main.py
├── README.md
├── ARCHITECTURE.md
├── BREAKING.md
├── PROMPTS.md
└── .gitignore
```

## 5. Installation

### Requirements

- Python 3.10 or later.
- Git.
- Windows, Linux or macOS.

### Step 1: Clone the repository

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
```

Replace the placeholder with your actual GitHub repository URL.

### Step 2: Enter the project directory

```bash
cd Kill-It-and-Prove-It
```

### Step 3: Create a virtual environment

Windows:

```powershell
py -m venv .venv
```

### Step 4: Activate the virtual environment

```powershell
.venv\Scripts\Activate.ps1
```

### Step 5: Install pytest

```powershell
py -m pip install pytest
```

The database runtime itself uses Python standard-library modules.

## 6. Running the Database

The current `main.py` demonstrates opening the database, retrieving values and closing it.

Run:

```powershell
py main.py
```

The demonstration reads:

```python
from src.database import Database

db = Database("data/database.db")

print("Name:", db.get(b"name"))
print("Age:", db.get(b"age"))
print("College:", db.get(b"college"))

db.close()
```

If these keys have not previously been inserted, their values will be `None`.

## 7. Using the Database API

The database can also be used directly from other Python programs.

### 7.1 Create or open a database

```python
from src.database import Database

db = Database("data/database.db")
```

### 7.2 Insert a record

```python
db.put(b"name", b"Nishanth")
db.put(b"age", b"20")
db.put(b"college", b"Srinivas Institute of Technology")
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

The batch is represented using BEGIN and COMMIT transaction records.

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

## 8. Running Tests

Run the complete automated test suite:

```powershell
py -m pytest -q
```

Run individual test modules:

```powershell
py -m pytest tests/test_basic.py -v
```

```powershell
py -m pytest tests/test_batch.py -v
```

```powershell
py -m pytest tests/test_compaction.py tests/test_compaction_crash.py -v
```

## 9. Testing and Benchmarking

The project includes several testing utilities.

| Tool | Purpose |
|---|---|
| `torture/runner.py` | Randomized database operation testing |
| `torture/crash_runner.py` | Repeated crash and restart testing |
| `torture/concurrent_runner.py` | Concurrent process testing |
| `torture/large_data_runner.py` | Large values and batch testing |
| `torture/oracle.py` | Independent expected-state verification |
| `tests/benchmark_database.py` | Large database performance testing |

Use the test scripts according to their implemented command-line arguments.

## 10. Recorded Performance Results

The following results were recorded during development:

| Metric | Result |
|---|---:|
| Records inserted | 1,000,000 |
| Insertion time | 7.16 seconds |
| Database size | 56.13 MB |
| Recovery time | 1.64 seconds |
| Recovered records | 1,000,000 |
| Automated tests | 36 passed |

These measurements are environment-dependent.

The million-record benchmark verified the record count and selected values, rather than comparing every value.

## 11. Documentation

Additional technical documentation is available in:

- `ARCHITECTURE.md` — database architecture, binary record format, recovery, locking and scenario verification.
- `BREAKING.md` — crash testing, failure experiments, observed results and known limitations.
- `PROMPTS.md` — chronological record of AI prompts used during development.

## 12. Current Implementation Status

The current implementation provides a Python database API and a basic `main.py` demonstration.

A separate interactive CLI with commands such as `put`, `get`, `del`, `batch`, `dump`, `check` and `serve` is not yet documented as implemented.

Further testing is required for exact termination points during physical writes, recovery interruptions and other remaining failure scenarios. See `BREAKING.md` for the verification details.

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