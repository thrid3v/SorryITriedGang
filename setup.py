
import os
import sys
import subprocess
import shutil
from pathlib import Path

def print_step(step):
    print(f"\n{'='*50}\n{step}\n{'='*50}")

def check_python_version():
    print_step("Checking Python Version")
    version = sys.version_info
    print(f"Current Python version: {sys.version}")
    if version.major < 3 or (version.major == 3 and version.minor < 9):
        print("Error: Python 3.9+ is required.")
        sys.exit(1)
    print("✅ Python version OK")

def install_dependencies():
    print_step("Installing Python Dependencies")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✅ Dependencies installed")
    except subprocess.CalledProcessError:
        print("❌ Failed to install dependencies")
        sys.exit(1)

def setup_env_file():
    print_step("Setting up Environment Variables")
    env_path = Path(".env")
    if env_path.exists():
        print("✅ .env file exists")
    else:
        print("⚠️ .env file missing. Creating from template...")
        with open(".env", "w") as f:
            f.write("OPENAI_API_KEY=your_key_here\n")
        print("✅ Created .env file. PLEASE EDIT IT with your API key.")

def run_data_generation():
    print_step("Generating Initial Data")
    try:
        subprocess.check_call([sys.executable, "src/ingestion/generator.py"])
        print("✅ Data generated")
    except subprocess.CalledProcessError:
        print("❌ Data generation failed")

def run_pipeline():
    print_step("Running Transformation Pipeline")
    try:
        subprocess.check_call([sys.executable, "src/transformation/pipeline.py"])
        print("✅ Pipeline finished")
    except subprocess.CalledProcessError:
        print("❌ Pipeline failed")

def main():
    print("🚀 Starting RetailNexus Setup...")
    check_python_version()
    install_dependencies()
    setup_env_file()
    run_data_generation()
    run_pipeline()
    
    print_step("Setup Complete!")
    print("To start the application:")
    print("1. Backend: python -m uvicorn api.main:app --reload")
    print("2. Frontend: cd frontend && npm install && npm run dev")

if __name__ == "__main__":
    main()
