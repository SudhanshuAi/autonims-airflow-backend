import subprocess
import threading
from typing import Dict, Optional, List
import json
from datetime import datetime
from job_monitor import JobStatus
import re
from pymongo import MongoClient
import logging

logging.basicConfig(level=logging.INFO)

def clean_line(line: str) -> str:
    """Removes ANSI codes and whitespace from a log line."""
    line = re.sub(r'\u001B\[\d+m', '', line)
    line = re.sub(r'\u001B\[0m', '', line)
    return line.strip()

def store_in_mongodb(mongo_config: dict, collection_name: str, data: dict):
    """Utility to store a document in a specified MongoDB collection."""
    client = None
    try:
        client = MongoClient(mongo_config["connection_string"])
        # MODIFICATION: Automatically get the database from the connection string.
        db = client.get_database() 
        collection = db[collection_name]
        result = collection.insert_one(data)
        logging.info(f"SUCCESS: Inserted document into '{collection_name}' with ID: {result.inserted_id}")
    except Exception as e:
        logging.error(f"FAIL: Could not store data in MongoDB collection '{collection_name}': {e}")
    finally:
        if client:
            client.close()

class SlingExecutor:
    def __init__(self, job_monitor, log_manager):
        self.active_jobs: Dict[str, subprocess.Popen] = {}
        self.job_monitor = job_monitor
        self.log_manager = log_manager
        self.mongodb_config: Optional[Dict] = None
        self.stream_logs: Dict[str, List[Dict]] = {}

    def set_connection(self, conn_name: str, conn_details: dict) -> bool:
        try:
            command = ["sling", "conns", "set", conn_name]
            for key, value in conn_details.items():
                command.append(f"{key}={value}")
            subprocess.run(command, capture_output=True, text=True, check=True)
            return True
        except subprocess.CalledProcessError:
            return False

    def execute_sling(self, job_id: str, config_file: str, mongodb_config: dict) -> bool:
        try:
            self.mongodb_config = mongodb_config
            self.stream_logs[job_id] = []
            command = ["sling", "run", "-r", config_file]
            process = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True, bufsize=1
            )
            self.active_jobs[job_id] = process
            self.job_monitor.update_status(job_id, JobStatus.RUNNING)
            threading.Thread(target=self._monitor_output, args=(process.stdout, job_id), daemon=True).start()
            threading.Thread(target=self._monitor_output, args=(process.stderr, job_id), daemon=True).start()
            threading.Thread(target=self._monitor_process, args=(job_id,), daemon=True).start()
            return True
        except Exception as e:
            self.job_monitor.add_log(job_id, f"Error starting job: {e}", datetime.now())
            self.job_monitor.update_status(job_id, JobStatus.FAILED)
            return False

    def _monitor_output(self, pipe, job_id: str):
        for line in pipe:
            cleaned_line = clean_line(line)
            if not cleaned_line: continue
            log_time = datetime.now()
            self._parse_stream_details(job_id, cleaned_line, log_time)
            self.job_monitor.add_log(job_id, cleaned_line, log_time)
            self.log_manager.publish_log(job_id, f"{log_time.strftime('%H:%M:%S')} {cleaned_line}")
            self._parse_metrics(job_id, cleaned_line)

    def _monitor_process(self, job_id: str):
        process = self.active_jobs.get(job_id)
        if not process: return

        return_code = process.wait()
        final_status = JobStatus.COMPLETED if return_code == 0 else JobStatus.FAILED
        self.job_monitor.update_status(job_id, final_status)
        
        summary_log = self.job_monitor.get_job_status(job_id)

        if self.mongodb_config:
            # Write to the new collection for pipeline summaries
            store_in_mongodb(self.mongodb_config, "pipelineRunSummaries", summary_log)

            if self.stream_logs.get(job_id):
                detailed_logs = {
                    "job_id": job_id,
                    "dag_id": summary_log.get("dag_id"),
                    "dag_run_id": summary_log.get("dag_run_id"),
                    "finished_at": datetime.now().isoformat(),
                    "status": final_status.value,
                    "streams": self.stream_logs.get(job_id, [])
                }
                # Write to the new collection for pipeline details
                store_in_mongodb(self.mongodb_config, "pipelineRunDetails", detailed_logs)

        # Cleanup
        if job_id in self.active_jobs: del self.active_jobs[job_id]
        if job_id in self.stream_logs: del self.stream_logs[job_id]

    def _parse_stream_details(self, job_id: str, line: str, timestamp: datetime):
        stream_start_match = re.search(r"running stream (\S+)", line)
        if stream_start_match:
            stream_name = stream_start_match.group(1)
            new_stream_log = {
                "stream_name": stream_name, "status": "Running", "started_at": timestamp.isoformat(),
                "finished_at": None, "details": [line]
            }
            self.stream_logs.setdefault(job_id, []).append(new_stream_log)
            return
        if self.stream_logs.get(job_id):
            current_stream = self.stream_logs[job_id][-1]
            current_stream["details"].append(line)
            if "execution succeeded" in line.lower():
                current_stream["status"] = "Success"
                current_stream["finished_at"] = timestamp.isoformat()
            elif "execution failed" in line.lower():
                current_stream["status"] = "Failed"
                current_stream["finished_at"] = timestamp.isoformat()

    def _parse_metrics(self, job_id: str, line: str):
        metrics = {}
        if "wrote" in line or "inserted" in line:
            rows_match = re.search(r"(?:wrote|inserted) (\d+[\d,]*) rows", line)
            if rows_match:
                metrics["rows_processed"] = int(rows_match.group(1).replace(',', ''))
        if metrics:
            self.job_monitor.update_metrics(job_id, metrics)

    def terminate_job(self, job_id: str) -> bool:
        if job_id not in self.active_jobs: return False
        try:
            self.active_jobs[job_id].terminate()
            self.job_monitor.update_status(job_id, JobStatus.TERMINATED)
            del self.active_jobs[job_id]
            return True
        except Exception:
            return False