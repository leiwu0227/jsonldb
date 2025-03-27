import sys
import os
import random
import time
import cProfile
import pstats
import io
from datetime import datetime
from line_profiler import LineProfiler
import pandas as pd
import numpy as np

# Add parent directory to path to import jsonlfile

from jsonldb.jsonlfile import save_jsonl, load_jsonl, select_jsonl, update_jsonl, delete_jsonl

def generate_test_data(num_records=100_000):
    """Generate test data with realistic structure."""
    return {
        f"key_{i:08d}": {
            "timestamp": datetime.now().isoformat(),
            "value": random.randint(1, 1000000),
            "temperature": round(random.uniform(-10.0, 40.0), 2),
            "pressure": round(random.uniform(980, 1020), 2),
            "humidity": random.randint(30, 90),
            "status": random.choice(["normal", "warning", "critical", "unknown"]),
            "location": random.choice(["north", "south", "east", "west", "center"]),
            "tags": random.sample(["sensor", "validated", "raw", "filtered", "anomaly", 
                                 "peak", "valley", "trend", "stable"], k=3)
        }
        for i in range(num_records)
    }

def profile_save_operation(data, filename="profile_test.jsonl"):
    """Profile the save operation."""
    profiler = cProfile.Profile()
    profiler.enable()
    
    save_jsonl(filename, data)
    
    profiler.disable()
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
    ps.print_stats()
    
    print("\nSave Operation Profile:")
    print(s.getvalue())
    return os.path.getsize(filename)

def profile_load_operation(filename="profile_test.jsonl"):
    """Profile the load operation."""
    profiler = cProfile.Profile()
    profiler.enable()
    
    data = load_jsonl(filename)
    
    profiler.disable()
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
    ps.print_stats()
    
    print("\nLoad Operation Profile:")
    print(s.getvalue())
    return len(data)

def profile_select_operation(filename="profile_test.jsonl", num_records=1000):
    """Profile the select operation."""
    profiler = cProfile.Profile()
    profiler.enable()
    
    start_key = f"key_{random.randint(0, num_records):08d}"
    end_key = f"key_{int(start_key[4:]) + num_records:08d}"
    selected = select_jsonl(filename, (start_key, end_key))
    
    profiler.disable()
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
    ps.print_stats()
    
    print("\nSelect Operation Profile:")
    print(s.getvalue())
    return len(selected)

def profile_update_operation(filename="profile_test.jsonl", num_updates=1000):
    """Profile the update operation."""
    profiler = cProfile.Profile()
    profiler.enable()
    
    updates = {
        f"key_{random.randint(0, num_updates):08d}": generate_test_data(1)[f"key_00000000"]
        for _ in range(num_updates)
    }
    update_jsonl(filename, updates)
    
    profiler.disable()
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
    ps.print_stats()
    
    print("\nUpdate Operation Profile:")
    print(s.getvalue())
    return len(updates)

def profile_delete_operation(filename="profile_test.jsonl", num_deletes=1000):
    """Profile the delete operation."""
    profiler = cProfile.Profile()
    profiler.enable()
    
    delete_keys = [f"key_{random.randint(0, num_deletes):08d}" 
                  for _ in range(num_deletes)]
    delete_jsonl(filename, delete_keys)
    
    profiler.disable()
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
    ps.print_stats()
    
    print("\nDelete Operation Profile:")
    print(s.getvalue())
    return len(delete_keys)

def run_memory_profile():
    """Run memory profiling on all operations."""
    import memory_profiler
    
    @memory_profiler.profile
    def memory_test():
        # Generate test data
        data = generate_test_data(100_000)
        
        # Save operation
        save_jsonl("memory_test.jsonl", data)
        
        # Load operation
        loaded_data = load_jsonl("memory_test.jsonl")
        
        # Select operation
        start_key = "key_00000000"
        end_key = "key_00001000"
        selected = select_jsonl("memory_test.jsonl", (start_key, end_key))
        
        # Update operation
        updates = {
            f"key_{i:08d}": generate_test_data(1)[f"key_00000000"]
            for i in range(1000)
        }
        update_jsonl("memory_test.jsonl", updates)
        
        # Delete operation
        delete_keys = [f"key_{i:08d}" for i in range(1000)]
        delete_jsonl("memory_test.jsonl", delete_keys)
        
        # Cleanup
        os.remove("memory_test.jsonl")
        os.remove("memory_test.jsonl.idx")
    
    memory_test()

def main():
    print("Starting JSONL File Profiling")
    print("=" * 50)
    
    # Test parameters
    num_records = 100_000
    num_operations = 1000
    
    # Generate test data
    print(f"\nGenerating {num_records:,} test records...")
    data = generate_test_data(num_records)
    
    # Run CPU profiling
    print("\nRunning CPU Profiling...")
    print("-" * 50)
    
    # Profile save operation
    print("\nProfiling save operation...")
    file_size = profile_save_operation(data)
    print(f"File size: {file_size/1024/1024:.2f} MB")
    
    # Profile load operation
    print("\nProfiling load operation...")
    loaded_count = profile_load_operation()
    print(f"Loaded {loaded_count:,} records")
    
    # Profile select operation
    print("\nProfiling select operation...")
    selected_count = profile_select_operation(num_records=num_operations)
    print(f"Selected {selected_count:,} records")
    
    # Profile update operation
    print("\nProfiling update operation...")
    updated_count = profile_update_operation(num_updates=num_operations)
    print(f"Updated {updated_count:,} records")
    
    # Profile delete operation
    print("\nProfiling delete operation...")
    deleted_count = profile_delete_operation(num_deletes=num_operations)
    print(f"Deleted {deleted_count:,} records")
    
    # Run memory profiling
    print("\nRunning Memory Profiling...")
    print("-" * 50)
    run_memory_profile()
    
    # Cleanup
    print("\nCleaning up...")
    if os.path.exists("profile_test.jsonl"):
        os.remove("profile_test.jsonl")
    if os.path.exists("profile_test.jsonl.idx"):
        os.remove("profile_test.jsonl.idx")
    print("Done!")

if __name__ == "__main__":
    main() 