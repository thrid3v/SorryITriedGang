"""
RetailNexus - Pipeline Orchestrator
===================================
Automates the full data lifecycle and manages the API:
Generator -> Cleaner -> SCD Type 2 -> Star Schema

Runs on a repeating schedule to simulate real-time data flow.
Also ensures the FastAPI is running.
"""

import time
import subprocess
import sys
import os
import signal
from datetime import datetime
from pathlib import Path

# Setup logging
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# Configuration
SLEEP_INTERVAL_SECONDS = 60 * 5  # Run every 5 minutes
PIPELINE_SCRIPTS = [
    "src/ingestion/generator.py",
    "src/transformation/cleaner.py",
    "src/transformation/scd_logic.py",
    "src/transformation/star_schema.py",
]
API_MODULE = "api.main:app"
API_PORT = 8000

class Orchestrator:
    def __init__(self):
        self.api_process = None
        self.project_root = Path(__file__).resolve().parents[1]

    def start_api(self):
        """Starts the FastAPI server in a background process."""
        if self.api_process and self.api_process.poll() is None:
            return  # Already running

        logger.info("Starting FastAPI server...")
        
        # Start FastAPI using uvicorn
        try:
            self.api_process = subprocess.Popen(
                [sys.executable, "-m", "uvicorn", API_MODULE, "--host", "0.0.0.0", "--port", str(API_PORT)],
                cwd=str(self.project_root),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0
            )
            logger.info(f"FastAPI server started on port {API_PORT} (PID: {self.api_process.pid})")
        except Exception as e:
            logger.error(f"Could not start API: {e}", exc_info=True)

    def stop_api(self):
        """Stops the FastAPI server process."""
        if self.api_process:
            logger.info("Stopping FastAPI server...")
            try:
                if os.name == 'nt':
                    # On Windows, we need to be aggressive with process groups sometimes
                    subprocess.run(['taskkill', '/F', '/T', '/PID', str(self.api_process.pid)], capture_output=True)
                else:
                    os.killpg(os.getpgid(self.api_process.pid), signal.SIGTERM)
            except Exception as e:
                logger.warning(f"Error stopping API process: {e}")
            finally:
                self.api_process = None

    def run_script(self, script_path):
        """Executes a python script and monitors its success."""
        full_path = self.project_root / script_path
        logger.info(f"Running script: {script_path}")
        
        try:
            # We use sys.executable to ensure we use the same python environment
            result = subprocess.run([sys.executable, str(full_path)], capture_output=True, text=True, check=True)
            # Log first few lines of output
            output_lines = result.stdout.strip().split('\n')
            for line in output_lines[:2]:
                if line.strip():
                    logger.debug(f"  > {line}")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Script {script_path} failed:")
            if e.stderr:
                logger.error(f"  STDERR: {e.stderr}")
            if e.stdout:
                logger.error(f"  STDOUT: {e.stdout}")
            return False

    def run(self):
        logger.info("="*50)
        logger.info("RetailNexus Real-Time Orchestrator Started")
        logger.info(f"Configuration: Interval = {SLEEP_INTERVAL_SECONDS}s")
        logger.info("="*50)

        # 1. Start API first
        self.start_api()

        pulse_count = 0
        try:
            while True:
                # Check if API is still alive
                if self.api_process.poll() is not None:
                    logger.warning("API process died. Restarting...")
                    self.start_api()

                pulse_count += 1
                logger.info(f"Pulse #{pulse_count}: Starting full pipeline run...")
                start_time = time.time()
                
                success = True
                for script in PIPELINE_SCRIPTS:
                    if not self.run_script(script):
                        success = False
                        break # Stop the pulse if a critical step fails
                
                duration = time.time() - start_time
                if success:
                    logger.info(f"Pulse #{pulse_count} completed successfully in {duration:.2f}s")
                else:
                    logger.error(f"Pulse #{pulse_count} failed. Retrying next cycle.")

                logger.debug(f"Sleeping for {SLEEP_INTERVAL_SECONDS}s...")
                time.sleep(SLEEP_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logger.info("Orchestrator stopped by user.")
        finally:
            self.stop_api()

if __name__ == "__main__":
    orchestrator = Orchestrator()
    orchestrator.run()
