# ITCS6190 — Spark SQL & Structured Streaming (L8/L9)

This project demonstrates a simple end-to-end streaming pipeline with **PySpark**:

- A Python **data generator** streams JSON ride events over a TCP socket.
- **Task 1**: Read & parse the stream → print to console **and** write CSVs.
- **Task 2**: Aggregate by `driver_id` (total fare, average distance) → console + per-epoch CSVs.
- **Task 3**: Windowed totals with **5-minute window**, **1-minute slide**, **1-minute watermark** → console + CSVs.

> ⚠️ Run **one Spark task at a time** while the generator is running (the socket server is single-client).

---

## Table of Contents
- [Repo Structure](#repo-structure)
- [Prerequisites](#prerequisites)
- [Quick Start (TL;DR)](#quick-start-tldr)
- [How to Run](#how-to-run)
  - [Terminal A — Generator](#terminal-a--generator)
  - [Terminal B — Tasks](#terminal-b--tasks)
- [Output Locations](#output-locations)
- [Verify Outputs](#verify-outputs)
- [Details & Semantics](#details--semantics)
- [Troubleshooting](#troubleshooting)
- [Submission Checklist](#submission-checklist)
- [.gitignore](#gitignore)

---

## **Setup Instructions**

### **1. Project Structure**

Ensure your project directory follows the structure below:

```
├── data_generator.py # streams valid JSON over TCP
├── task1.py # parse stream → console + CSV (append)
├── task2.py # per-driver aggregates → console + per-epoch CSV
├── task3.py # 5m window / 1m slide / 1m watermark → console + CSV
├── outputs/
│ ├── task_1/
│ ├── task_2/
│ └── task_3/
├── .gitignore
└── README.md
```



---

## Prerequisites

- **Python** 3.9+ (3.11/3.12 OK)
- **Java JDK** 11+ (GitHub Codespaces already has it)
- **Pip packages:** `pyspark`, `faker`

If Java is missing locally, install a JDK (e.g., Adoptium Temurin).

---

## Quick Start (TL;DR)

```bash
# 1) Optional: create & activate a virtual environment
python -m venv .venv
source .venv/bin/activate                # Windows: .venv\Scripts\activate

# 2) Install dependencies
pip install --upgrade pip
pip install pyspark faker

# 3) Create output folders (if not present)
mkdir -p outputs/task_1 outputs/task_2 outputs/task_3

# 4) Run generator (Terminal A)
python data_generator.py

# 5) Run tasks (Terminal B — one at a time)
python task1.py
# Ctrl+C, then:
python task2.py
# Ctrl+C, then:
python task3.py    # let it run ~7–8 minutes for first finalized windows
```

Default socket: `127.0.0.1:9999`.
If port 9999 is busy, start the generator with PORT=9998 and update tasks to use `port=9998`.

## How to Run

Open two terminals in the project root.

Terminal A — Generator
```
python data_generator.py
# Expected logs:
# "Streaming data to 127.0.0.1:9999…"
# "New client connected: ('127.0.0.1', <port>)"
```

Keep this terminal running while you test each task.


Terminal B — Tasks

Run one at a time. Stop with Ctrl+C between tasks.

Task 1 — Parse → Console + CSV (outputs/task_1/)
```
python task1.py
```
Task 2 — Aggregates → Console (COMPLETE mode) + per-epoch CSV (outputs/task_2/epoch=<n>/)
```
python task2.py
```

Task 3 — 5m/1m/1m → Console + CSV (outputs/task_3/)
```
python task3.py
# Let it run ~7–8 minutes so the first windows finalize and write rows.
```
Finally, stop the generator (Ctrl+C in Terminal A).

## Output Locations

Task 1: CSV parts → outputs/task_1/part-*.csv

Task 2: Per-epoch CSV → outputs/task_2/epoch=<id>/*.csv

Task 3: CSV parts → outputs/task_3/part-*.csv

Checkpoint (managed by Spark): outputs/task_3/_chk/


## Verify Outputs
```
# Task 1
ls -1 outputs/task_1 | head
head -n 5 outputs/task_1/part-*.csv

# Task 2 (show latest epoch)
ls -1 outputs/task_2
latest=$(ls -1 outputs/task_2 | sort -V | tail -n1)
ls -1 "outputs/task_2/$latest"
head -n 5 "outputs/task_2/$latest"/*.csv

# Task 3
ls -1 outputs/task_3 | head
head -n 5 outputs/task_3/part-*.csv
```

## Details & Semantics

Input format: Each line from the generator is valid JSON (double quotes) ending with a newline \n.

Timestamp format: YYYY-MM-DD HH:mm:ss (parsed with to_timestamp(..., "yyyy-MM-dd HH:mm:ss") in Task 3).

Task 2:

Output mode complete to show running totals per driver in console and write per-epoch CSVs via foreachBatch.

Task 3:

Window: 5 minutes, Slide: 1 minute, Watermark: 1 minute.

File sink is in append mode → writes a row only when a window is finalized (i.e., when watermark > window_end).

Expect a delay (~6 minutes from first events) before the first non-header CSV rows appear.



## Submission Checklist

✅ data_generator.py, task1.py, task2.py, task3.py included.

✅ Task 1 CSVs under outputs/task_1/.

✅ Task 2 epoch CSV(s) under outputs/task_2/.

✅ Task 3 CSVs under outputs/task_3/ (optionally consolidated windowed_fares.csv).

✅ README updated (this file).

✅ .gitignore excludes checkpoints and CRC markers.

---
- Name: Ruthwik Dovala
- 801431661
