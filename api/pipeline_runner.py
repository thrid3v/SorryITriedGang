"""
Pipeline Runner Module
======================
Runs the data generation and transformation pipeline.
Designed to be called from the API to avoid file locking issues.
"""
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

def run_generator(num_transactions: int = 200) -> dict:
    """
    Run the data generator.
    
    Args:
        num_transactions: Number of transactions to generate
        
    Returns:
        dict with status and message
    """
    try:
        generator_path = PROJECT_ROOT / "src" / "ingestion" / "generator.py"
        result = subprocess.run(
            [sys.executable, str(generator_path)],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode == 0:
            return {
                "status": "success",
                "message": "Data generation completed",
                "output": result.stdout
            }
        else:
            return {
                "status": "error",
                "message": "Data generation failed",
                "error": result.stderr
            }
    except subprocess.TimeoutExpired:
        return {
            "status": "error",
            "message": "Data generation timed out (>60s)"
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to run generator: {str(e)}"
        }


def run_pipeline() -> dict:
    """
    Run the transformation pipeline.
    
    Returns:
        dict with status and message
    """
    try:
        pipeline_path = PROJECT_ROOT / "src" / "transformation" / "pipeline.py"
        result = subprocess.run(
            [sys.executable, str(pipeline_path)],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=120
        )
        
        if result.returncode == 0:
            return {
                "status": "success",
                "message": "Pipeline completed successfully",
                "output": result.stdout
            }
        else:
            return {
                "status": "error",
                "message": "Pipeline failed",
                "error": result.stderr
            }
    except subprocess.TimeoutExpired:
        return {
            "status": "error",
            "message": "Pipeline timed out (>120s)"
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to run pipeline: {str(e)}"
        }


def run_full_pipeline(num_transactions: int = 200) -> dict:
    """
    Run both generator and pipeline in sequence.
    
    Args:
        num_transactions: Number of transactions to generate
        
    Returns:
        dict with status and message
    """
    # Step 1: Generate data
    gen_result = run_generator(num_transactions)
    if gen_result["status"] != "success":
        return gen_result
    
    # Step 2: Run pipeline
    pipeline_result = run_pipeline()
    
    return {
        "status": pipeline_result["status"],
        "message": f"Generator: {gen_result['message']}. Pipeline: {pipeline_result['message']}",
        "generator_output": gen_result.get("output", ""),
        "pipeline_output": pipeline_result.get("output", ""),
        "error": pipeline_result.get("error")
    }
