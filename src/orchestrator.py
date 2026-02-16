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
SLEEP_INTERVAL_SECONDS = 60 * 5  # Run every 5 minutes (when enabled)

# We split generator from the rest of the pipeline so we can control whether
# synthetic data keeps being generated in the background.
GENERATOR_SCRIPT = "src/ingestion/generator.py"
PIPELINE_SCRIPTS = [
    "src/transformation/cleaner.py",
    "src/transformation/scd_logic.py",
    "src/transformation/star_schema.py",
]

# By default we run ONE full pipeline cycle on startup (including generator)
# so the app has data, but we DO NOT keep generating new data in the
# background. To re-enable periodic pulses, set ENABLE_PERIODIC_PIPELINE=true.
ENABLE_PERIODIC_PIPELINE = os.getenv("ENABLE_PERIODIC_PIPELINE", "false").lower() == "true"
API_MODULE = "api.main:app"
API_PORT = 8000

class Orchestrator:
    def __init__(self):
        self.api_process = None
        self.project_root = Path(__file__).resolve().parents[1]
        self._api_log_handle = None

    def start_api(self):
        """Starts the FastAPI server in a background process."""
        if self.api_process and self.api_process.poll() is None:
            return  # Already running

        logger.info("Starting FastAPI server...")
        
        # Start FastAPI using uvicorn
        try:
            # Ensure we capture uvicorn output somewhere useful; otherwise failures are silent.
            logs_dir = self.project_root / "logs"
            logs_dir.mkdir(parents=True, exist_ok=True)
            uvicorn_log_path = logs_dir / "uvicorn.log"

            # Close prior handle if any (e.g., after a restart)
            try:
                if self._api_log_handle:
                    self._api_log_handle.close()
            except Exception:
                pass
            self._api_log_handle = open(uvicorn_log_path, "a", encoding="utf-8")

            self.api_process = subprocess.Popen(
                [sys.executable, "-m", "uvicorn", API_MODULE, "--host", "0.0.0.0", "--port", str(API_PORT)],
                cwd=str(self.project_root),
                stdout=self._api_log_handle,
                stderr=self._api_log_handle,
                text=True,
                # On Unix, this makes uvicorn its own process group so we can terminate cleanly.
                start_new_session=False if os.name == "nt" else True,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0,
            )
            logger.info(
                f"FastAPI server started on port {API_PORT} (PID: {self.api_process.pid}). "
                f"Logs: {uvicorn_log_path}"
            )
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
                    # If started with start_new_session=True, PID is the process group leader.
                    os.killpg(os.getpgid(self.api_process.pid), signal.SIGTERM)
            except Exception as e:
                logger.warning(f"Error stopping API process: {e}")
            finally:
                self.api_process = None
                try:
                    if self._api_log_handle:
                        self._api_log_handle.close()
                except Exception:
                    pass
                self._api_log_handle = None

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
        
        # 2. Run a single bootstrap pipeline (generator + transformations)
        #    so the app has data immediately on first start.
        pulse_count = 1
        logger.info(f"Pulse #{pulse_count}: Starting initial full pipeline run (with generator)...")
        start_time = time.time()
        success = True
        
        # Run generator once to seed data
        if not self.run_script(GENERATOR_SCRIPT):
            success = False
        
        # Then run the transformation pipeline steps
        if success:
            for script in PIPELINE_SCRIPTS:
                if not self.run_script(script):
                    success = False
                    break
        
        duration = time.time() - start_time
        if success:
            logger.info(f"Pulse #{pulse_count} completed successfully in {duration:.2f}s")
        else:
            logger.error(f"Pulse #{pulse_count} failed during bootstrap run.")
        
        # 3. Optionally continue running periodic pulses if explicitly enabled.
        if not ENABLE_PERIODIC_PIPELINE:
            logger.info("Periodic pipeline is disabled (ENABLE_PERIODIC_PIPELINE!=true).")
            # Keep API alive and monitor it, but do not generate new data.
            try:
                while True:
                    if self.api_process and self.api_process.poll() is not None:
                        logger.warning("API process died. Restarting...")
                        self.start_api()
                    time.sleep(5)
            except KeyboardInterrupt:
                logger.info("Orchestrator stopped by user.")
            finally:
                self.stop_api()
            return
        
        # Legacy mode: periodic pulses enabled.
        try:
            while True:
                # Check if API is still alive
                if self.api_process and self.api_process.poll() is not None:
                    logger.warning("API process died. Restarting...")
                    self.start_api()
                
                pulse_count += 1
                logger.info(f"Pulse #{pulse_count}: Starting full pipeline run...")
                start_time = time.time()
                
                success = True
                # Optional: generate more synthetic data each pulse
                if not self.run_script(GENERATOR_SCRIPT):
                    success = False
                
                if success:
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
