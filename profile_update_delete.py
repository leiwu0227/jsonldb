import cProfile
import pstats
import os
import time
from datetime import datetime
from jsonlfile import save_jsonl, update_jsonl, delete_jsonl, load_jsonl

def generate_test_data(size):
    """Generate test data with consistent record sizes"""
    data = {}
    for i in range(size):
        key = f"key_{i:010d}"
        data[key] = {
            "timestamp": datetime.now().isoformat(),
            "value": i,
            "description": "Test data " * 5  # ~50 bytes of text
        }
    return data

def profile_update(size, update_ratio=0.1):
    """Profile update_jsonl with different data sizes"""
    test_file = f"test_update_{size}.jsonl"
    
    # Initial data
    data = generate_test_data(size)
    save_jsonl(test_file, data)
    
    # Generate update data (update_ratio % of records)
    update_size = int(size * update_ratio)
    update_data = {}
    for i in range(update_size):
        key = f"key_{i:010d}"  # Update first update_ratio% of records
        update_data[key] = {
            "timestamp": datetime.now().isoformat(),
            "value": i * 100,  # Changed value
            "description": "Updated data " * 5
        }
    
    # Profile update operation
    start_time = time.time()
    update_jsonl(test_file, update_data)
    elapsed = time.time() - start_time
    
    # Calculate metrics
    updates_per_sec = update_size / elapsed
    avg_time_per_update_ms = (elapsed / update_size) * 1000
    
    # Cleanup
    if os.path.exists(test_file):
        os.remove(test_file)
    if os.path.exists(test_file + '.idx'):
        os.remove(test_file + '.idx')
        
    return elapsed, updates_per_sec, avg_time_per_update_ms

def profile_delete(size, delete_ratio=0.1):
    """Profile delete_jsonl with different data sizes"""
    test_file = f"test_delete_{size}.jsonl"
    
    # Initial data
    data = generate_test_data(size)
    save_jsonl(test_file, data)
    
    # Generate delete keys (delete_ratio % of records)
    delete_size = int(size * delete_ratio)
    delete_keys = [f"key_{i:010d}" for i in range(delete_size)]  # Delete first delete_ratio% of records
    
    # Profile delete operation
    start_time = time.time()
    delete_jsonl(test_file, delete_keys)
    elapsed = time.time() - start_time
    
    # Verify deletion
    loaded_data = load_jsonl(test_file)
    actual_deleted = size - len(loaded_data)
    
    # Calculate metrics
    deletes_per_sec = delete_size / elapsed
    avg_time_per_delete_ms = (elapsed / delete_size) * 1000
    
    # Cleanup
    if os.path.exists(test_file):
        os.remove(test_file)
    if os.path.exists(test_file + '.idx'):
        os.remove(test_file + '.idx')
        
    return elapsed, deletes_per_sec, avg_time_per_delete_ms, actual_deleted == delete_size

def main():
    sizes = [1_000, 10_000, 100_000, 1_000_000]
    
    # Profile update operations
    print("\n=== Update Performance Analysis ===")
    print("\nDetailed profiling for 1M records with 10% updates...")
    
    profiler = cProfile.Profile()
    test_file = "test_update_1m.jsonl"
    data = generate_test_data(1_000_000)
    save_jsonl(test_file, data)
    
    update_size = 100_000  # 10% of records
    update_data = {f"key_{i:010d}": {"value": i * 100, "timestamp": datetime.now().isoformat(), "description": "Updated " * 5} for i in range(update_size)}
    
    profiler.enable()
    update_jsonl(test_file, update_data)
    profiler.disable()
    
    # Cleanup
    if os.path.exists(test_file):
        os.remove(test_file)
    if os.path.exists(test_file + '.idx'):
        os.remove(test_file + '.idx')
    
    stats = pstats.Stats(profiler)
    stats.sort_stats('cumulative').print_stats(20)
    
    print("\nUpdate Scaling Analysis:")
    print("Size (records) | Updates | Time (s) | Updates/s | Avg Time/Update (ms)")
    print("-" * 75)
    
    for size in sizes:
        elapsed, ups, avg_time = profile_update(size)
        update_size = int(size * 0.1)
        print(f"{size:>12,d} | {update_size:>7,d} | {elapsed:>8.4f} | {ups:>9.2f} | {avg_time:>19.4f}")
    
    # Profile delete operations
    print("\n=== Delete Performance Analysis ===")
    print("\nDetailed profiling for 1M records with 10% deletes...")
    
    profiler = cProfile.Profile()
    test_file = "test_delete_1m.jsonl"
    data = generate_test_data(1_000_000)
    save_jsonl(test_file, data)
    
    delete_size = 100_000  # 10% of records
    delete_keys = [f"key_{i:010d}" for i in range(delete_size)]
    
    profiler.enable()
    delete_jsonl(test_file, delete_keys)
    profiler.disable()
    
    # Cleanup
    if os.path.exists(test_file):
        os.remove(test_file)
    if os.path.exists(test_file + '.idx'):
        os.remove(test_file + '.idx')
    
    stats = pstats.Stats(profiler)
    stats.sort_stats('cumulative').print_stats(20)
    
    print("\nDelete Scaling Analysis:")
    print("Size (records) | Deletes | Time (s) | Deletes/s | Avg Time/Delete (ms) | Verified")
    print("-" * 85)
    
    for size in sizes:
        elapsed, dps, avg_time, verified = profile_delete(size)
        delete_size = int(size * 0.1)
        status = "✓" if verified else "✗"
        print(f"{size:>12,d} | {delete_size:>7,d} | {elapsed:>8.4f} | {dps:>9.2f} | {avg_time:>19.4f} | {status:>8}")

if __name__ == "__main__":
    main() 