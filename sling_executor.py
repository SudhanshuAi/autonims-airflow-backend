import subprocess
import threading
from typing import Dict, Optional
import json
from datetime import datetime
from job_monitor import JobStatus
import re
import boto3
from pymongo import MongoClient
import logging

logging.basicConfig(level=logging.INFO)


def clean_line(line: str) -> str:
    # Remove ANSI color codes
    line = re.sub(r"\u001B\[\d+m", "", line)
    # Remove ANSI reset sequence
    line = re.sub(r"\u001B\[0m", "", line)
    # Remove special arrow character if present
    line = line.replace("\u003E", ">")
    return line


import boto3


def upload_data_to_mongodb(MONGODB_CONFIG, data):
    """
    Upload data to a MongoDB collection.
    
    :param connection_string: MongoDB connection string
    :param database_name: Name of the database
    :param collection_name: Name of the collection
    :param data: List of dictionaries to upload
    """
    # Establish connection to MongoDB
    connection_string = MONGODB_CONFIG["connection_string"]
    database_name = MONGODB_CONFIG["database_name"]
    collection_name = MONGODB_CONFIG["collection_name"]

    logging.info(f"====================== Adding to DB ========================")

    try:
        client = MongoClient(connection_string)

        db = client[database_name]
        collection = db[collection_name]
        
        result = collection.insert_one(data)
        logging.info(f"============== Inserted document with ID: {result.inserted_id} ==================")
        
    except Exception as e:
        print(f"Error uploading data: {e}")
    finally:
        client.close()

class SlingExecutor:
    def __init__(self, job_monitor, log_manager):
        self.active_jobs: Dict[str, subprocess.Popen] = {}
        self.job_monitor = job_monitor
        self.log_manager = log_manager

        self.mongodb_config = None

    def set_connection(self, conn_name: str, conn_details: dict) -> bool:
        try:
            command = ["sling", "conns", "set", conn_name]
            for key, value in conn_details.items():
                command.append(f"{key}={value}")

            result = subprocess.run(command, capture_output=True, text=True, check=True)
            return True
        except subprocess.CalledProcessError:
            return False

    def execute_sling(self, job_id: str, config_file: str, mongodb_config: dict) -> bool:
        try:
            self.mongodb_config = mongodb_config

            command = ["sling", "run", "-r", config_file]

            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                bufsize=1,
            )

            self.active_jobs[job_id] = process
            self.job_monitor.update_status(job_id, JobStatus.RUNNING)

            # Start output monitoring threads
            threading.Thread(
                target=self._monitor_output,
                args=(process.stdout, job_id, "stdout"),
                daemon=True,
            ).start()

            threading.Thread(
                target=self._monitor_output,
                args=(process.stderr, job_id, "stderr"),
                daemon=True,
            ).start()

            # Start process monitoring thread
            threading.Thread(
                target=self._monitor_process, args=(job_id,), daemon=True
            ).start()

            return True

        except Exception as e:
            self.job_monitor.add_log(job_id, f"Error starting job: {str(e)}")
            self.job_monitor.update_status(job_id, JobStatus.FAILED)
            return False

    def _monitor_output(self, pipe, job_id: str, stream_type: str):
        for line in pipe:
            format_line = clean_line(line.strip())

            # --- THIS IS THE CRITICAL NEW LINE ---
            # Print the raw output from Sling directly to the flask-app console log.
            logging.info(f"[SLING_OUTPUT][{job_id}]: {format_line}")
            # --- END OF NEW LINE ---

            timestamp_pattern = r'\u001B\[90m\d+:\d+(?:AM|PM)\u001B\[0m\s+'
            line_without_timestamp = re.sub(timestamp_pattern, '', line)

            log_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.job_monitor.add_log(job_id, line_without_timestamp, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            self.log_manager.publish_log(job_id, f"{log_time} {format_line}")
            self._parse_metrics(job_id, format_line)

    def _monitor_process(self, job_id: str):
        process = self.active_jobs.get(job_id)
        if process:
            return_code = process.wait()

            if return_code == 0:
                self.job_monitor.update_status(job_id, JobStatus.COMPLETED)
            else:
                self.job_monitor.update_status(job_id, JobStatus.FAILED)

            del self.active_jobs[job_id]
        
            log_obj = self.job_monitor.get_job_status(job_id)

            upload_data_to_mongodb(self.mongodb_config, log_obj)
            
            # upload_log_to_s3(dag_id, job_id, log_obj)

    def _parse_metrics(self, job_id: str, line: str):
        metrics = {}

        # timestamp_match = re.match(r'(\d+:\d+(?:AM|PM))', line)
        # if timestamp_match:
        #     timestamp = timestamp_match.group(1)

        if "Sling Replication" in line:
            parts = line.split("|")
            if len(parts) > 2:
                source_dest = parts[1].split("->")
                if len(source_dest) > 1:
                    metrics["source_detail"] = source_dest[0].strip()
                metrics["table_name"] = parts[2].strip()
            # metrics["start_time"] = timestamp

        if "connecting to source" in line:
            type_match = re.search(r"connecting to source (\w+) \(([\w-]+)\)", line)
            if type_match:
                # Extract specific source type
                metrics["source_type"] = type_match.group(2)  # e.g., "postgres"

        if "writing to target" in line:
            # Extract system like 's3' from parentheses
            type_match = re.search(r"writing to target .*?\(([\w-]+)\)", line)
            if type_match:
                metrics["target_type"] = type_match.group(1)

        if "wrote" in line:
            rows_match = re.search(r"wrote (\d+) rows \[(\d+) r/s\]", line)
            if rows_match:
                metrics["rows_processed"] = int(rows_match.group(1))
                metrics["rows_per_second"] = int(rows_match.group(2))

            path_match = re.search(r"to\s+([^\s]+)$", line)
            if path_match:
                metrics["target_path"] = path_match.group(1)

        if "execution" in line.lower():
            metrics["status"] = "succeeded" if "succeeded" in line.lower() else "failed"
            # metrics["end_time"] = timestamp

            # if "start_time" in metrics and "end_time" in metrics:
            #     try:
            #         # Parse times and calculate duration
            #         time_format = "%I:%M%p"
            #         start = datetime.strptime(metrics.start_time, time_format)
            #         end = datetime.strptime(metrics.end_time, time_format)
            #         duration_minutes = (end - start).total_seconds() / 60
            #         metrics["duration"] = f"{int(duration_minutes)} minutes"
            #     except Exception as e:
            #         metrics["duration"] = "Could not calculate duration"

        if metrics:
            self.job_monitor.update_metrics(job_id, metrics)

    def terminate_job(self, job_id: str) -> bool:
        if job_id not in self.active_jobs:
            return False

        try:
            process = self.active_jobs[job_id]
            process.terminate()
            self.job_monitor.update_status(job_id, JobStatus.TERMINATED)
            del self.active_jobs[job_id]
            return True
        except Exception:
            return False
