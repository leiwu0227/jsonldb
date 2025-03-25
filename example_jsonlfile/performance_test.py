import sys
import os
import random
import time
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jsonlfile import save_jsonl, load_jsonl, select_jsonl, update_jsonl, delete_jsonl

def generate_random_record():
    """Generate a random record with consistent structure."""
    return {
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

def time_operation(name, func, *args, **kwargs):
    """Time an operation and print results."""
    start = time.time()
    result = func(*args, **kwargs)
    elapsed = time.time() - start
    print(f"{name}: {elapsed:.3f} seconds")
    return result

def main():
    print("Performance Test for JSONL Operations")
    print("=" * 50)
    
    # Test parameters
    num_records = 100_000  # 100K records
    select_range = 1000    # Select 1K records
    update_count = 1000    # Update 1K records
    delete_count = 1000    # Delete 1K records
    
    # Generate test data
    print(f"\nGenerating {num_records:,} records...")
    data = {
        f"key_{i:08d}": generate_random_record()
        for i in range(num_records)
    }
    
    # Test save operation
    print("\nTesting save operation...")
    time_operation("Save", save_jsonl, "perf_test.jsonl", data)
    
    # Test load operation
    print("\nTesting load operation...")
    loaded_data = time_operation("Load", load_jsonl, "perf_test.jsonl")
    print(f"Loaded {len(loaded_data):,} records")
    
    # Test select operation
    print("\nTesting select operation...")
    start_key = f"key_{random.randint(0, num_records-select_range):08d}"
    end_key = f"key_{int(start_key[4:]) + select_range:08d}"
    selected = time_operation("Select", select_jsonl, "perf_test.jsonl", (start_key, end_key))
    print(f"Selected {len(selected):,} records")
    
    # Test update operation
    print("\nTesting update operation...")
    updates = {
        f"key_{random.randint(0, num_records):08d}": generate_random_record()
        for _ in range(update_count)
    }
    time_operation("Update", update_jsonl, "perf_test.jsonl", updates)
    
    # Test delete operation
    print("\nTesting delete operation...")
    delete_keys = [f"key_{random.randint(0, num_records):08d}" 
                  for _ in range(delete_count)]
    time_operation("Delete", delete_jsonl, "perf_test.jsonl", delete_keys)
    
    # Calculate and print statistics
    print("\nPerformance Statistics:")
    print("-" * 50)
    final_data = load_jsonl("perf_test.jsonl")
    print(f"Final record count: {len(final_data):,}")
    print(f"Records per operation:")
    print(f"- Save: {num_records/1000:.1f}K records")
    print(f"- Select: {select_range} records")
    print(f"- Update: {update_count} records")
    print(f"- Delete: {delete_count} records")
    
    # Clean up
    print("\nCleaning up...")
    os.remove("perf_test.jsonl")
    os.remove("perf_test.jsonl.idx")
    print("Done!")

if __name__ == "__main__":
    main() 