from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional
import json


class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TERMINATED = "terminated"


@dataclass
class JobMetrics:
    rows_per_second: int = 0
    rows_processed: int = 0
    target_path: Optional[str] = None
    target_type: Optional[str] = None
    source_type: Optional[str] = None
    source_detail: Optional[str] = None
    table_name: Optional[str] = None
    # bytes_processed: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    errors: List[str] = field(default_factory=list)
    status: str = "Pending"
    duration: Optional[str] = None

    # total_tables: int = 0

    def to_dict(self):
        return {
            "rows_processed": self.rows_processed,
            "rows_per_second": self.rows_per_second,
            "target_path": self.target_path,
            "target_type": self.target_type,
            "source_type": self.source_type,
            "source_detail": self.source_detail,
            "table_name": self.table_name,
            # "bytes_processed": self.bytes_processed,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "errors": self.errors,
            "duration": self.duration,
            "status": self.status,
        }


class JobMonitor:
    def __init__(self):
        self.jobs: Dict[str, Dict] = {}

    def register_job(self, job_id: str, dag_id: str, dag_details: dict, config_file: str):
        self.jobs[job_id] = {
            "status": JobStatus.PENDING,
            "metrics": JobMetrics(),
            "config_file": config_file,
            "logs": [],
            "dag_id": dag_id,
            **dag_details
        }

    def update_status(self, job_id: str, status: JobStatus):
        if job_id in self.jobs:
            self.jobs[job_id]["status"] = status
            if (
                status == JobStatus.RUNNING
                and not self.jobs[job_id]["metrics"].start_time
            ):
                self.jobs[job_id]["metrics"].start_time = datetime.now()
            elif status in (
                JobStatus.COMPLETED,
                JobStatus.FAILED,
                JobStatus.TERMINATED,
            ):
                self.jobs[job_id]["metrics"].end_time = datetime.now()

                time_format = "%I:%M%p"
                start = self.jobs[job_id]["metrics"].start_time
                end = self.jobs[job_id]["metrics"].end_time
                
                duration_seconds = (end - start).total_seconds()

                # # Format the duration into the highest possible unit
                # if duration_seconds >= 3600:
                #     # More than or equal to 1 hour
                #     duration = f"{int(duration_seconds // 3600)} hrs"
                # elif duration_seconds >= 60:
                #     # More than or equal to 1 minute
                #     duration = f"{int(duration_seconds // 60)} mins"
                # elif duration_seconds >= 1:
                #     # More than or equal to 1 second
                #     duration = f"{int(duration_seconds)} sec"
                # else:
                #     # Less than 1 second, display in milliseconds
                #     duration = f"{int(duration_seconds * 1000)} ms"

                hours = int(duration_seconds // 3600)
                remaining_seconds = duration_seconds % 3600
                minutes = int(remaining_seconds // 60)
                seconds = int(remaining_seconds % 60)
                milliseconds = int((remaining_seconds % 1) * 1000)

                # Format the duration into a composite format
                if hours > 0:
                    if minutes > 0:
                        duration = f"{hours} hr {minutes} mins"
                    else:
                        duration = f"{hours} hr"
                elif minutes > 0:
                    if seconds > 0:
                        duration = f"{minutes} mins {seconds} sec"
                    else:
                        duration = f"{minutes} mins"
                elif seconds > 0:
                    duration = f"{seconds} sec"
                else:
                    duration = f"{milliseconds} ms"

                self.jobs[job_id][
                    "metrics"
                ].duration = duration

    def add_log(self, job_id: str, log_entry: str, log_time: datetime):
        if job_id in self.jobs:
            self.jobs[job_id]["logs"].append(
                {"timestamp": log_time, "message": log_entry}
            )

    def update_metrics(self, job_id: str, metrics_update: dict):
        if job_id in self.jobs:
            current_metrics = self.jobs[job_id]["metrics"]
            for key, value in metrics_update.items():
                if hasattr(current_metrics, key):
                    setattr(current_metrics, key, value)

    def get_job_status(self, job_id: str) -> Optional[dict]:
        if job_id not in self.jobs:
            return None

        job = self.jobs[job_id]
        return {
            **job,
            "status": job["status"].value,
            "metrics": job["metrics"].to_dict(),
            "recent_logs": job["logs"][-100:] if job["logs"] else [],
            # "dag_id": job["dag_id"],
            # "execution_date": job["execution_date"],
        }

    def get_job_metrics(self, job_id: str) -> Optional[dict]:
        if job_id not in self.jobs:
            return None

        return self.jobs[job_id]["metrics"].to_dict()
