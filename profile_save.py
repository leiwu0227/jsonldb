import cProfile
import pstats
import os
import time
from datetime import datetime
from jsonlfile import save_jsonl

def generate_test_data(size):
    """Generate test data with consistent record sizes"""
    data = {}
    for i in range(size):
        key = f"key_{i:010d}"  # Fixed width key
        data[key] = {
            "timestamp": datetime.now().isoformat(),
            "value": i,
            "description": "Test data " * 5  # ~50 bytes of text
        }
    return data

def profile_save(size):
    """Profile save_jsonl for a specific size"""
    test_file = f"test_{size}.jsonl"
    
    # Generate test data
    data = generate_test_data(size)
    
    # Time the save operation
    start_time = time.time()
    save_jsonl(test_file, data)
    elapsed = time.time() - start_time
    
    # Calculate metrics
    records_per_sec = size / elapsed
    avg_time_per_record_ms = (elapsed / size) * 1000
    
    # Cleanup
    if os.path.exists(test_file):
        os.remove(test_file)
    if os.path.exists(test_file + '.idx'):
        os.remove(test_file + '.idx')
        
    return elapsed, records_per_sec, avg_time_per_record_ms

def main():
    # Run detailed profiling on 1M records
    print("\n=== Detailed Function Analysis ===")
    profiler = cProfile.Profile()
    test_file = "test_1m.jsonl"
    data = generate_test_data(1_000_000)
    
    profiler.enable()
    save_jsonl(test_file, data)
    profiler.disable()
    
    # Clean up files
    if os.path.exists(test_file):
        os.remove(test_file)
    if os.path.exists(test_file + '.idx'):
        os.remove(test_file + '.idx')
    
    # Print detailed stats
    stats = pstats.Stats(profiler)
    stats.sort_stats('cumulative').print_stats(20)
    
    # Test different sizes
    print("\n=== Testing Size Scaling ===\n")
    sizes = [1_000, 10_000, 100_000, 1_000_000]
    
    print("Size (records) | Time (s) | Records/s | Avg Time/Record (ms)")
    print("-" * 65)
    
    for size in sizes:
        elapsed, rps, avg_time = profile_save(size)
        print(f"{size:>12,d} | {elapsed:>7.4f} | {rps:>9.2f} | {avg_time:>18.4f}")

if __name__ == "__main__":
    main() 