# Async Job Scheduler

## Overview
This project is a simple command-line job scheduler implemented using Python's
`asyncio` library. It reads job definitions from a YAML configuration file and
executes them concurrently while respecting execution intervals and job
dependencies. The scheduler demonstrates core concepts behind data orchestration
tools such as Airflow and Dagster.

## Requirements
- Python 3.9 or higher
- PyYAML

## Installation
Create and activate a virtual environment, then install dependencies:

```bash
python -m venv venv
venv\Scripts\activate    # Windows
pip install -r requirements.txt

```

## Running the Scheduler
Run the scheduler using the following command:

```bash
python scheduler.py
```
The scheduler runs continuously and can be stopped with **Ctrl + C**.

