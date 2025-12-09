import asyncio
import json
import time
import sys
from dataclasses import dataclass, field
from typing import List, Dict, Optional

import yaml  # PyYAML

def log_event(level: str, event_type: str, message: str,
              job_name: Optional[str] = None,
              extra: Optional[Dict] = None) -> None:
    """
    Print a single JSON log line to stdout.
    """
    log = {
        "timestamp": time.time(),
        "level": level,
        "event": event_type,
        "message": message,
    }
    if job_name is not None:
        log["job_name"] = job_name
    if extra:
        log.update(extra)
    print(json.dumps(log), file=sys.stdout, flush=True)
    
@dataclass
class Job:
    name: str
    command: str
    interval: int
    depends_on: List[str] = field(default_factory=list)
    
def load_jobs_from_yaml(path: str) -> List[Job]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except FileNotFoundError:
        log_event("ERROR", "CONFIG_ERROR", f"Config file not found: {path}")
        sys.exit(1)
    except yaml.YAMLError as e:
        log_event("ERROR", "CONFIG_ERROR", f"YAML parse error: {e}")
        sys.exit(1)

    if not isinstance(data, dict) or "jobs" not in data:
        log_event("ERROR", "CONFIG_ERROR", "Root key 'jobs' missing in YAML")
        sys.exit(1)

    raw_jobs = data["jobs"]
    if not isinstance(raw_jobs, list):
        log_event("ERROR", "CONFIG_ERROR", "'jobs' must be a list")
        sys.exit(1)

    jobs: List[Job] = []
    names_seen = set()

    for i, job_def in enumerate(raw_jobs):
        if not isinstance(job_def, dict):
            log_event("ERROR", "CONFIG_ERROR", f"Job entry {i} is not a dict")
            sys.exit(1)

        name = job_def.get("name")
        command = job_def.get("command")
        interval = job_def.get("interval")
        depends_on = job_def.get("depends_on", [])

        # Validation
        if not name or not isinstance(name, str):
            log_event("ERROR", "CONFIG_ERROR", f"Job {i} missing valid 'name'")
            sys.exit(1)
        if name in names_seen:
            log_event("ERROR", "CONFIG_ERROR", f"Duplicate job name: {name}")
            sys.exit(1)
        names_seen.add(name)

        if not command or not isinstance(command, str):
            log_event("ERROR", "CONFIG_ERROR",
                      f"Job '{name}' missing valid 'command'")
            sys.exit(1)

        if not isinstance(interval, int) or interval <= 0:
            log_event("ERROR", "CONFIG_ERROR",
                      f"Job '{name}' has invalid 'interval': {interval}")
            sys.exit(1)

        if depends_on is None:
            depends_on = []
        if not isinstance(depends_on, list) or \
           any(not isinstance(d, str) for d in depends_on):
            log_event("ERROR", "CONFIG_ERROR",
                      f"Job '{name}' has invalid 'depends_on'")
            sys.exit(1)

        jobs.append(Job(name=name,
                        command=command,
                        interval=interval,
                        depends_on=depends_on))

    # Ensure dependencies refer to valid job names
    valid_names = {job.name for job in jobs}
    for job in jobs:
        for dep in job.depends_on:
            if dep not in valid_names:
                log_event("ERROR", "CONFIG_ERROR",
                          f"Job '{job.name}' depends on unknown job '{dep}'")
                sys.exit(1)

    log_event("INFO", "CONFIG_LOADED",
              f"Loaded {len(jobs)} jobs from {path}")
    return jobs

async def run_job(job: Job,
                  last_success: Dict[str, bool]) -> None:
    start_time = time.time()
    log_event(
        "INFO",
        "JOB_START",
        f"Starting job '{job.name}'",
        job_name=job.name,
        extra={"command": job.command}
    )

    proc = await asyncio.create_subprocess_shell(
        job.command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stdout_bytes, stderr_bytes = await proc.communicate()
    duration = time.time() - start_time

    stdout_text = stdout_bytes.decode().strip() if stdout_bytes else ""
    stderr_text = stderr_bytes.decode().strip() if stderr_bytes else ""

    if proc.returncode == 0:
        last_success[job.name] = True
        log_event(
            "INFO",
            "JOB_SUCCESS",
            f"Job '{job.name}' succeeded",
            job_name=job.name,
            extra={
                "exit_code": proc.returncode,
                "duration_sec": duration,
                "stdout": stdout_text,
                "stderr": stderr_text,
            },
        )
    else:
        last_success[job.name] = False
        log_event(
            "ERROR",
            "JOB_FAILED",
            f"Job '{job.name}' failed with exit code {proc.returncode}",
            job_name=job.name,
            extra={
                "exit_code": proc.returncode,
                "duration_sec": duration,
                "stdout": stdout_text,
                "stderr": stderr_text,
            },
        )

async def scheduler_loop(jobs: List[Job]) -> None:
    # State
    last_run_time: Dict[str, float] = {job.name: 0.0 for job in jobs}
    last_success: Dict[str, bool] = {job.name: False for job in jobs}

    log_event("INFO", "SCHEDULER_START", "Scheduler started")

    while True:
        now = time.time()
        tasks = []

        for job in jobs:
            time_since_last = now - last_run_time[job.name]

            # Check if job is due
            if time_since_last < job.interval:
                continue  # Not yet time to run

            # Check dependencies
            deps = job.depends_on
            if deps:
                all_ok = True
                for dep in deps:
                    if not last_success.get(dep, False):
                        all_ok = False
                        log_event(
                            "INFO",
                            "JOB_WAITING_DEPENDENCIES",
                            f"Job '{job.name}' waiting for dependency '{dep}' to succeed",
                            job_name=job.name,
                        )
                        break
                if not all_ok:
                    continue  # Skip this job this cycle

            # If we reach here, job is due and dependencies are satisfied (or none)
            last_run_time[job.name] = now  # mark run time
            tasks.append(asyncio.create_task(run_job(job, last_success)))

        # Allow all due jobs to run concurrently
        if tasks:
            await asyncio.gather(*tasks)

        # Sleep one second before next scheduling cycle
        await asyncio.sleep(1)
        
        
async def main():
    jobs = load_jobs_from_yaml("jobs.yml")
    await scheduler_loop(jobs)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log_event("INFO", "SCHEDULER_STOP", "Scheduler stopped by user")


