import cProfile
import pstats
import os
import time
import datetime
from typing import Dict, Any
from jsonlfile import save_jsonl, select_jsonl

def generate_test_data(size: int) -> Dict[str, Any]:
    """Generate test data with consistent record sizes."""
    data = {}
    for i in range(size):
        key = f"key_{i:010d}"  # Fixed width keys for consistent sorting
        data[key] = {
            "timestamp": datetime.datetime.now().isoformat(),
            "value": i,
            "description": f"Test record {i} with some additional data for consistent size"
        }
    return data

def profile_select(size: int, range_size: float = 0.1) -> None:
    """
    Profile select_jsonl for a specific size and range.
    
    Args:
        size: Number of total records
        range_size: Fraction of total records to select (0.0 to 1.0)
    """
    # Generate and save test data
    test_file = "test_select.jsonl"
    data = generate_test_data(size)
    save_jsonl(test_file, data)
    
    # Calculate range bounds
    keys = sorted(data.keys())
    range_records = int(size * range_size)
    start_idx = (size - range_records) // 2  # Select from middle
    end_idx = start_idx + range_records
    range_bounds = (keys[start_idx], keys[end_idx])
    
    # Time the selection
    start_time = time.time()
    selected = select_jsonl(test_file, range_bounds)
    elapsed = time.time() - start_time
    
    # Calculate metrics
    records_per_sec = size / elapsed
    avg_time_ms = (elapsed * 1000.0) / size
    
    print(f"\nSize: {size:,} records, Range: {range_size:.1%} of data")
    print(f"Selected records: {len(selected):,}")
    print(f"Total time: {elapsed:.4f} seconds")
    print(f"Records/second: {records_per_sec:.2f}")
    print(f"Average time per record: {avg_time_ms:.4f} ms")

def main():
    """Run detailed profiling on 1M records with different range sizes."""
    test_file = "test_select.jsonl"
    
    # Test different data sizes
    sizes = [1_000, 10_000, 100_000, 1_000_000]
    range_sizes = [0.01, 0.1, 0.5]  # 1%, 10%, 50% of data
    
    print("\nProfiling select_jsonl with different data sizes and range queries")
    print("=" * 60)
    
    # Profile 1M records in detail
    size = 1_000_000
    range_size = 0.1  # 10% of data
    
    # Generate test data once for detailed profiling
    data = generate_test_data(size)
    save_jsonl(test_file, data)
    
    # Calculate range bounds
    keys = sorted(data.keys())
    range_records = int(size * range_size)
    start_idx = (size - range_records) // 2
    end_idx = start_idx + range_records
    range_bounds = (keys[start_idx], keys[end_idx])
    
    # Run detailed profiling
    profiler = cProfile.Profile()
    profiler.enable()
    selected = select_jsonl(test_file, range_bounds)
    profiler.disable()
    
    print(f"\nDetailed profiling for {size:,} records (selecting {range_size:.1%} of data):")
    stats = pstats.Stats(profiler)
    stats.sort_stats('cumulative').print_stats(20)
    
    # Test scaling with different sizes and ranges
    print("\nScaling Analysis:")
    print("-" * 60)
    print("Size (records) | Range % | Time (s) | Records/s | Selected | Avg Time/Record (ms)")
    print("-" * 60)
    
    for size in sizes:
        data = generate_test_data(size)
        save_jsonl(test_file, data)
        
        for range_size in range_sizes:
            keys = sorted(data.keys())
            range_records = int(size * range_size)
            start_idx = (size - range_records) // 2
            end_idx = start_idx + range_records
            range_bounds = (keys[start_idx], keys[end_idx])
            
            start_time = time.time()
            selected = select_jsonl(test_file, range_bounds)
            elapsed = time.time() - start_time
            
            records_per_sec = size / elapsed
            avg_time_ms = (elapsed * 1000.0) / size
            
            print(f"{size:12,} | {range_size:7.1%} | {elapsed:8.4f} | {records_per_sec:9.2f} | {len(selected):8,} | {avg_time_ms:8.4f}")
    
    # Cleanup
    if os.path.exists(test_file):
        os.remove(test_file)
    if os.path.exists(f"{test_file}.idx"):
        os.remove(f"{test_file}.idx")

if __name__ == "__main__":
    main() 