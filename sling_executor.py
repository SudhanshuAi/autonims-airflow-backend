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
    line = re.sub(r'\u001B\[\d+m', '', line)
    line = re.sub(r'\u001B\[0m', '', line)
    return line.strip()

def store_in_mongodb(mongo_config: dict, collection_name: str, data: dict):
    client = None
    try:
        client = MongoClient(mongo_config["connection_string"])
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
        self.output_threads: Dict[str, List[threading.Thread]] = {}
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
            self.stream_logs[job_id] = [] # Initialize as empty
            
            command = ["sling", "run", "--concurrency", "5", "-r", config_file]
            process = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True, bufsize=1
            )
            self.active_jobs[job_id] = process
            self.job_monitor.update_status(job_id, JobStatus.RUNNING)

            self.output_threads[job_id] = []
            stdout_thread = threading.Thread(target=self._monitor_output, args=(process.stdout, job_id), daemon=True)
            stderr_thread = threading.Thread(target=self._monitor_output, args=(process.stderr, job_id), daemon=True)
            self.output_threads[job_id].extend([stdout_thread, stderr_thread])
            stdout_thread.start()
            stderr_thread.start()

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

        process.wait()
        if job_id in self.output_threads:
            for thread in self.output_threads[job_id]:
                thread.join()
        
        return_code = process.poll()
        final_status = JobStatus.COMPLETED if return_code == 0 else JobStatus.FAILED
        self.job_monitor.update_status(job_id, final_status)
        
        if self.stream_logs.get(job_id):
            last_log_entry = self.stream_logs[job_id][-1]
            if not last_log_entry["finished_at"]:
                last_log_entry["finished_at"] = datetime.now().isoformat()
            last_log_entry["status"] = "Success" if final_status == JobStatus.COMPLETED else "Failed"

        summary_log = self.job_monitor.get_job_status(job_id)

        if self.mongodb_config:
            store_in_mongodb(self.mongodb_config, "pipelineRunSummaries", summary_log)
            if self.stream_logs.get(job_id):
                detailed_logs = {
                    "job_id": job_id, "dag_id": summary_log.get("dag_id"),
                    "dag_run_id": summary_log.get("dag_run_id"), "finished_at": datetime.now().isoformat(),
                    "status": final_status.value, "streams": self.stream_logs.get(job_id, [])
                }
                store_in_mongodb(self.mongodb_config, "pipelineRunDetails", detailed_logs)

        # Cleanup
        if job_id in self.active_jobs: del self.active_jobs[job_id]
        if job_id in self.stream_logs: del self.stream_logs[job_id]
        if job_id in self.output_threads: del self.output_threads[job_id]

    def _parse_stream_details(self, job_id: str, line: str, timestamp: datetime):
        log_list = self.stream_logs.setdefault(job_id, [])

        # If this is the very first log line for the job, create the default entry.
        if not log_list:
            log_list.append({
                "stream_name": "Overall Job Log", "status": "Running",
                "started_at": timestamp.isoformat(), "finished_at": None, "details": []
            })
        
        stream_start_match = re.search(r"running stream (\S+)", line)
        current_log_entry = log_list[-1]

        # ***** THE FIX IS HERE *****
        if stream_start_match:
            stream_name = stream_start_match.group(1)
            # If the current entry is the default placeholder, REPURPOSE it for the first real stream.
            if current_log_entry["stream_name"] == "Overall Job Log":
                current_log_entry["stream_name"] = stream_name
                current_log_entry["started_at"] = timestamp.isoformat() # Update start time to be more accurate
                current_log_entry["details"].append(line)
            else:
                # Otherwise, it's a subsequent stream, so create a new entry.
                new_stream_log = {
                    "stream_name": stream_name, "status": "Running", "started_at": timestamp.isoformat(),
                    "finished_at": None, "details": [line]
                }
                log_list.append(new_stream_log)
        else:
            # For all other lines, just append to the current log entry.
            current_log_entry["details"].append(line)

        # Update status on the *current* entry (which is always the last one)
        last_entry = log_list[-1]
        if "execution succeeded" in line.lower():
            last_entry["status"] = "Success"
            last_entry["finished_at"] = timestamp.isoformat()
        elif "execution failed" in line.lower():
            last_entry["status"] = "Failed"
            last_entry["finished_at"] = timestamp.isoformat()
                
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