from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import os
import re
import yaml
import threading
from datetime import datetime
from typing import Dict, Optional

from job_monitor import JobMonitor, JobStatus
from sling_executor import SlingExecutor
from log_manager import LogManager

import logging

app = Flask(__name__)
CORS(app)

# Directory to store replication configs
CONFIG_DIR = "./configs"
if not os.path.exists(CONFIG_DIR):
    os.makedirs(CONFIG_DIR)

# Initialize core services
job_monitor = JobMonitor()
log_manager = LogManager()
sling_executor = SlingExecutor(job_monitor, log_manager)

def extract_format(obj_name):
    """Extract the part after the last dot in the object name"""
    return obj_name.split(".")[-1] if "." in obj_name else obj_name


@app.route("/set_connection", methods=["POST"])
def set_connection():
    """
    Sets up a new connection configuration for Sling.
    Expects a JSON payload with:
    {
        "name": "connection_name",
        "details": {
            "key1": "value1",
            "key2": "value2"
        }
    }
    """
    data = request.json
    conn_name = data["name"]
    conn_details = data["details"]

    try:
        success = sling_executor.set_connection(conn_name, conn_details)
        if success:
            return (
                jsonify(
                    {"status": "success", "message": "Connection set successfully"}
                ),
                200,
            )
        return jsonify({"status": "error", "message": "Failed to set connection"}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/register", methods=["POST"])
def register():
    """
    Registers a new pipeline configuration. This is the final, unified function
    that correctly handles all modes (full-refresh, truncate, incremental, 
    snapshot, backfill) based on the variable structure from the frontend.
    """
    data = request.json
    config_file = os.path.join(
        CONFIG_DIR, f"{re.sub(r'[^a-z0-9]+', '_', data['dag_id'].lower())}.yaml"
    )

    try:
        # The frontend always sends a config_obj, so we will rely on it as the 
        # single source of truth. We no longer need a broken 'else' fallback.
        config_obj = data.get("config_obj", {})

        # 1. Build the 'defaults' block for the YAML file.
        defaults_config = {"mode": data.get("mode", "full-refresh")}
        
        # Correctly format primary_key as a comma-separated string if it exists.
        if config_obj.get("primary_key"):
            pk_list = config_obj.get("primary_key", [])
            if isinstance(pk_list, list):
                defaults_config["primary_key"] = ",".join(pk_list)

        # Correctly format update_key as a comma-separated string if it exists.
        if config_obj.get("update_key"):
            uk_list = config_obj.get("update_key", [])
            if isinstance(uk_list, list):
                defaults_config["update_key"] = ",".join(uk_list)

        # 2. Build the 'source_options' block ONLY for backfill mode.
        source_options = {}
        if data.get("mode") == "backfill":
            start_dt = config_obj.get("start_dt")
            end_dt = config_obj.get("end_dt")
            if start_dt and end_dt:
                source_options["range"] = f"{start_dt},{end_dt}"
            if config_obj.get("update_key"): 
                 uk_list = config_obj.get("update_key", [])
                 if isinstance(uk_list, list):
                    source_options["update_key"] = ",".join(uk_list)
        
        # 3. Process the streams object.
        streams_obj = config_obj.get("streams", {})
        for stream_name, stream_config in streams_obj.items():
            # Add the 'public.' schema prefix if it's missing.
            if "object" in stream_config and "." not in stream_config["object"]:
                stream_config["object"] = f"public.{stream_config['object']}"
            
            # Inject the source_options block if we created it.
            if source_options:
                stream_config["source_options"] = source_options

        # 4. Assemble the final, complete configuration for Sling.
        config = {
            "source": data["source"]["conn"],
            "target": data["target"]["conn"],
            "defaults": defaults_config,
            "streams": streams_obj
        }

        # 5. Write the generated configuration to a YAML file.
        with open(config_file, "w") as file:
            yaml.dump(config, file)

        return jsonify({"status": "success", "config_file": config_file}), 201

    except Exception as e:
        logging.error(f"Error in /register endpoint: {str(e)}", exc_info=True)
        return jsonify({"status": "error", "message": f"An unexpected error occurred: {str(e)}"}), 500

@app.route("/run", methods=["POST"])
def run():
    """
    Initiates a Sling data pipeline execution.
    Provides real-time monitoring and logging capabilities.
    """
    data = request.json
    dag_id = data["dag_id"]
    dag_details = data["dag_details"]
    dag_run_id = dag_details["dag_run_id"]
    mongodb_config = data["mongodb_config"]

    # to uniquely identify every running job
    job_id = f"{dag_id}{dag_run_id}"

    config_file = os.path.join(
        CONFIG_DIR, f"{re.sub(r'[^a-z0-9]+', '_', dag_id.lower())}.yaml"
    )

    if not os.path.exists(config_file):
        return jsonify({"status": "error", "message": "Config file not found"}), 404

    try:
        # Register and start the job
        job_monitor.register_job(job_id, dag_id, dag_details, config_file)
        success = sling_executor.execute_sling(job_id, config_file, mongodb_config)

        if success:
            return (
                jsonify(
                    {
                        "status": "accepted",
                        "job_id": job_id,
                        "message": "Job started successfully",
                        "logs_endpoint": f"/stream_logs/{job_id}",
                        "status_endpoint": f"/job_status/{job_id}",
                        "metrics_endpoint": f"/job_metrics/{job_id}",
                    }
                ),
                202,
            )

        return jsonify({"status": "error", "message": "Failed to start job"}), 500

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/stream_logs/<job_id>")
def stream_logs(job_id):
    """
    Provides server-sent events stream of real-time job logs.
    """

    def generate():
        log_queue = log_manager.subscribe(job_id)
        try:
            while True:
                log_entry = log_queue.get()
                if log_entry is None:  # Signal to stop streaming
                    break
                yield f"data: {log_entry}\n\n"
        except GeneratorExit:
            log_manager.unsubscribe(job_id, log_queue)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


@app.route("/job_status/<job_id>", methods=["GET"])
def get_job_status(job_id):
    """
    Retrieves current status and basic information about a job.
    """
    status = job_monitor.get_job_status(job_id)
    if status is None:
        return jsonify({"status": "error", "message": "Job not found"}), 404

    return jsonify(status)


@app.route("/job_metrics/<job_id>", methods=["GET"])
def get_job_metrics(job_id):
    """
    Retrieves detailed metrics about a job's execution.
    """
    metrics = job_monitor.get_job_metrics(job_id)
    if metrics is None:
        return jsonify({"status": "error", "message": "Job not found"}), 404

    return jsonify(metrics)


@app.route("/terminate_job/<job_id>", methods=["POST"])
def terminate_job(job_id):
    """
    Terminates a running job.
    """
    success = sling_executor.terminate_job(job_id)
    if success:
        return (
            jsonify({"status": "success", "message": "Job terminated successfully"}),
            200,
        )

    return (
        jsonify(
            {"status": "error", "message": "Failed to terminate job or job not found"}
        ),
        404,
    )


@app.route("/all_jobs", methods=["GET"])
def fetch_all_jobs():
    # Convert all JobStatus enums to their string values
    jobs_with_string_status = {
        job_id: {
            **job,
            "status": job["status"].value,  # Convert JobStatus to string
        }
        for job_id, job in job_monitor.jobs.items()
    }
    return {"jobs": jobs_with_string_status}


@app.route("/fetch_config/<string:dag_id>", methods=["GET"])
def fetch_config(dag_id):
    """
    Retrieves the YAML configuration for a specific pipeline.
    """
    config_file = os.path.join(
        CONFIG_DIR, f"{re.sub(r'[^a-z0-9]+', '_', dag_id.lower())}.yaml"
    )

    if not os.path.exists(config_file):
        return jsonify({"status": "error", "message": "Config file not found"}), 404

    try:
        with open(config_file, "r") as file:
            config_contents = yaml.safe_load(file)

        return jsonify({"status": "success", "config": config_contents}), 200
    except Exception as e:
        return (
            jsonify({"status": "error", "message": f"Error reading config: {str(e)}"}),
            500,
        )


@app.route("/update_config/<string:dag_id>", methods=["PUT"])
def update_config(dag_id):
    """
    Updates the YAML configuration for a specific pipeline.
    """
    config_file = os.path.join(
        CONFIG_DIR, f"{re.sub(r'[^a-z0-9]+', '_', dag_id.lower())}.yaml"
    )

    if not os.path.exists(config_file):
        return jsonify({"status": "error", "message": "Config file not found"}), 404

    try:
        data = request.json
        with open(config_file, "w") as file:
            yaml.dump(data, file)

        return (
            jsonify(
                {"status": "success", "message": "Config file updated successfully"}
            ),
            200,
        )
    except Exception as e:
        return (
            jsonify({"status": "error", "message": f"Error updating config: {str(e)}"}),
            500,
        )


@app.route("/health", methods=["GET"])
def health_check():
    """
    Health check endpoint for monitoring service status.
    Returns service status, version, and basic metrics.
    """
    try:
        # Get basic service information
        active_jobs = len([job for job in job_monitor.jobs.values() if job["status"].value in ["running", "pending"]])
        total_jobs = len(job_monitor.jobs)
        
        health_data = {
            "status": "healthy",
            "service": "Sling Data Pipeline Service",
            "version": "1.0.0",
            "timestamp": datetime.now().isoformat(),
            "uptime": "running",
            "metrics": {
                "active_jobs": active_jobs,
                "total_jobs": total_jobs,
                "config_files": len([f for f in os.listdir(CONFIG_DIR) if f.endswith('.yaml')]) if os.path.exists(CONFIG_DIR) else 0
            },
            "dependencies": {
                "sling_executor": "available",
                "job_monitor": "available",
                "log_manager": "available"
            }
        }
        
        return jsonify(health_data), 200
        
    except Exception as e:
        error_data = {
            "status": "unhealthy",
            "service": "Sling Data Pipeline Service",
            "version": "1.0.0",
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }
        return jsonify(error_data), 503

if __name__ == "__main__":
    app.run(debug=True, port=5001)
