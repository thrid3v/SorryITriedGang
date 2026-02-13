"""
RetailNexus - Pipeline Orchestrator
===================================
Automates the full data lifecycle and manages the API:
Generator -> Cleaner -> SCD Type 2 -> Star Schema

Runs on a repeating schedule to simulate real-time data flow.
Also ensures the Flask API is running.
"""

import time
import subprocess
import sys
import os
import signal
from datetime import datetime
from pathlib import Path

# Configuration
SLEEP_INTERVAL_SECONDS = 60 * 5  # Run every 5 minutes
PIPELINE_SCRIPTS = [
    "src/ingestion/generator.py",
    "src/transformation/cleaner.py",
    "src/transformation/scd_logic.py",
    "src/transformation/star_schema.py",
]
API_SCRIPT = "src/api/app.py"

class Orchestrator:
    def __init__(self):
        self.api_process = None
        self.project_root = Path(__file__).resolve().parents[1]

    def start_api(self):
        """Starts the Flask API in a background process."""
        if self.api_process and self.api_process.poll() is None:
            return  # Already running

        print(f"[{datetime.now().strftime('%H:%M:%S')}] [API] Starting Flask server...")
        api_path = self.project_root / API_SCRIPT
        
        # Start the API in a new process group so it doesn't die with us immediately if not careful
        # But we actually WANT it to die when we exit, so standard subprocess is fine.
        try:
            self.api_process = subprocess.Popen(
                [sys.executable, str(api_path)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0
            )
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [API] Server process started (PID: {self.api_process.pid})")
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] Could not start API: {e}")

    def stop_api(self):
        """Stops the Flask API process."""
        if self.api_process:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [API] Stopping Flask server...")
            if os.name == 'nt':
                # On Windows, we need to be aggressive with process groups sometimes
                subprocess.run(['taskkill', '/F', '/T', '/PID', str(self.api_process.pid)], capture_output=True)
            else:
                os.killpg(os.getpgid(self.api_process.pid), signal.SIGTERM)
            self.api_process = None

    def run_script(self, script_path):
        """Executes a python script and monitors its success."""
        full_path = self.project_root / script_path
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [SCRIPT] Running: {script_path}")
        
        try:
            # We use sys.executable to ensure we use the same python environment
            result = subprocess.run([sys.executable, str(full_path)], capture_output=True, text=True, check=True)
            # Print first few lines of output to keep console clean
            output_lines = result.stdout.strip().split('\n')
            for line in output_lines[:2]:
                print(f"  > {line}")
            return True
        except subprocess.CalledProcessError as e:
            print(f"  [ERROR] in {script_path}:")
            print(e.stderr if e.stderr else e.stdout)
            return False

    def run(self):
        print("="*50)
        print("[START] RetailNexus Real-Time Orchestrator Started")
        print(f"[CONFIG] Interval: {SLEEP_INTERVAL_SECONDS}s")
        print("="*50)

        # 1. Start API first
        self.start_api()

        pulse_count = 0
        try:
            while True:
                # Check if API is still alive
                if self.api_process.poll() is not None:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] [WARNING] API died. Restarting...")
                    self.start_api()

                pulse_count += 1
                print(f"\n[PULSE #{pulse_count}] Starting full pipeline run...")
                start_time = time.time()
                
                success = True
                for script in PIPELINE_SCRIPTS:
                    if not self.run_script(script):
                        success = False
                        break # Stop the pulse if a critical step fails
                
                duration = time.time() - start_time
                if success:
                    print(f"[SUCCESS] [Pulse #{pulse_count}] Completed successfully in {duration:.2f}s")
                else:
                    print(f"[FAILED] [Pulse #{pulse_count}] Failed. Retrying next cycle.")

                print(f"[WAIT] Sleeping for {SLEEP_INTERVAL_SECONDS}s...")
                time.sleep(SLEEP_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            print("\n[STOP] Orchestrator stopped by user.")
        finally:
            self.stop_api()

if __name__ == "__main__":
    orchestrator = Orchestrator()
    orchestrator.run()
