"""
Pipeline Runner Module
======================
Runs the data generation and transformation pipeline asynchronously.
Designed to be called from the API to avoid blocking.
"""
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

async def run_generator(num_transactions: int = 200) -> dict:
    """
    Run the data generator asynchronously.
    
    Args:
        num_transactions: Number of transactions to generate
        
    Returns:
        dict with status and message
    """
    try:
        generator_path = PROJECT_ROOT / "src" / "ingestion" / "generator.py"
        
        # Create async subprocess with num_transactions argument
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(generator_path),
            "--num", str(num_transactions),  # Pass num_transactions to generator
            cwd=str(PROJECT_ROOT),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        # Wait for completion with timeout
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=60.0
            )
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            return {
                "status": "error",
                "message": "Data generation timed out (>60s)"
            }
        
        if process.returncode == 0:
            return {
                "status": "success",
                "message": "Data generation completed",
                "output": stdout.decode('utf-8')
            }
        else:
            return {
                "status": "error",
                "message": "Data generation failed",
                "error": stderr.decode('utf-8')
            }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to run generator: {str(e)}"
        }


async def run_pipeline() -> dict:
    """
    Run the transformation pipeline asynchronously.
    
    Returns:
        dict with status and message
    """
    try:
        pipeline_path = PROJECT_ROOT / "src" / "transformation" / "pipeline.py"
        
        # Create async subprocess
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(pipeline_path),
            cwd=str(PROJECT_ROOT),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        # Wait for completion with timeout
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=120.0
            )
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            return {
                "status": "error",
                "message": "Pipeline timed out (>120s)"
            }
        
        if process.returncode == 0:
            return {
                "status": "success",
                "message": "Pipeline completed successfully",
                "output": stdout.decode('utf-8')
            }
        else:
            return {
                "status": "error",
                "message": "Pipeline failed",
                "error": stderr.decode('utf-8')
            }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to run pipeline: {str(e)}"
        }


async def run_full_pipeline(num_transactions: int = 200) -> dict:
    """
    Run both generator and pipeline in sequence asynchronously.
    
    Args:
        num_transactions: Number of transactions to generate
        
    Returns:
        dict with status and message
    """
    # Step 1: Generate data
    gen_result = await run_generator(num_transactions)
    if gen_result["status"] != "success":
        return gen_result
    
    # Step 2: Run pipeline
    pipeline_result = await run_pipeline()
    
    return {
        "status": pipeline_result["status"],
        "message": f"Generator: {gen_result['message']}. Pipeline: {pipeline_result['message']}",
        "generator_output": gen_result.get("output", ""),
        "pipeline_output": pipeline_result.get("output", ""),
        "error": pipeline_result.get("error")
    }
