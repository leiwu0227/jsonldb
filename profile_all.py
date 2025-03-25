import cProfile
import pstats
import os
import time
from datetime import datetime
from jsonlfile import save_jsonl, load_jsonl, update_jsonl, delete_jsonl, lint_jsonl

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

def profile_operation(operation, size, **kwargs):
    """Profile any operation with timing and metrics"""
    test_file = f"test_{size}.jsonl"
    metrics = {}
    
    # Setup test data
    data = generate_test_data(size)
    save_jsonl(test_file, data)  # Create initial file
    
    # Prepare operation-specific data
    if operation == update_jsonl:
        update_data = {k: v for k, v in list(data.items())[:size//10]}  # Update 10%
        kwargs['update_dict'] = update_data
    elif operation == delete_jsonl:
        delete_keys = list(data.keys())[:size//10]  # Delete 10%
        kwargs['linekeys'] = delete_keys
    elif operation == save_jsonl:
        kwargs['db_dict'] = data
    elif operation == load_jsonl:
        pass  # No extra args needed
    elif operation == lint_jsonl:
        pass  # No extra args needed
    
    # Profile the operation
    profiler = cProfile.Profile()
    start_time = time.time()
    
    profiler.enable()
    if operation in [save_jsonl, update_jsonl, delete_jsonl, lint_jsonl]:
        operation(test_file, **kwargs)
    else:  # load_jsonl
        result = operation(test_file)
        metrics['records_loaded'] = len(result)
    profiler.disable()
    
    elapsed = time.time() - start_time
    
    # Calculate metrics
    metrics['elapsed'] = elapsed
    metrics['records_per_sec'] = size / elapsed
    metrics['avg_time_per_record_ms'] = (elapsed / size) * 1000
    
    # Cleanup
    if os.path.exists(test_file):
        os.remove(test_file)
    if os.path.exists(test_file + '.idx'):
        os.remove(test_file + '.idx')
        
    return profiler, metrics

def format_number(num):
    """Format number with comma separators"""
    return f"{num:,.2f}"

def print_operation_header(operation_name, size):
    """Print a formatted header for operation results"""
    print(f"\n=== {operation_name} Performance Analysis ({size:,d} records) ===")
    print("\nDetailed profiling results:")

def main():
    operations = [
        ('Save', save_jsonl),
        ('Load', load_jsonl),
        ('Update', update_jsonl),
        ('Delete', delete_jsonl),
        ('Lint', lint_jsonl)
    ]
    
    sizes = [1_000, 10_000, 100_000, 1_000_000]
    
    for op_name, operation in operations:
        print(f"\n{'='*20} {op_name} Operation {'='*20}")
        print("\nSize (records) | Time (s) | Records/s | Avg Time/Record (ms)")
        print("-" * 65)
        
        for size in sizes:
            profiler, metrics = profile_operation(operation, size)
            
            # Print scaling metrics
            print(f"{size:>12,d} | {metrics['elapsed']:>7.4f} | "
                  f"{format_number(metrics['records_per_sec']):>9} | "
                  f"{metrics['avg_time_per_record_ms']:>18.4f}")
            
            # Print detailed profiling for largest size
            if size == max(sizes):
                print(f"\nDetailed profiling for {size:,d} records:")
                stats = pstats.Stats(profiler)
                stats.sort_stats('cumulative').print_stats(20)

if __name__ == "__main__":
    main() 