import time
import datetime
import random
import string
import os
from typing import Dict, Any
import cProfile
import pstats
from jsonlfile import (
    save_jsonl,
    load_jsonl,
    select_jsonl,
    update_jsonl,
    delete_jsonl,
    build_jsonl_index
)

class JsonlFileProfiler:
    def __init__(self, file_path: str = "profile_test.jsonl"):
        self.file_path = file_path
        self.cleanup()

    def cleanup(self):
        """Remove test files if they exist."""
        for ext in ['', '.idx']:
            if os.path.exists(self.file_path + ext):
                os.remove(self.file_path + ext)

    def generate_test_data(self, size: int, key_type: str = "string") -> Dict[Any, Dict]:
        """Generate test data with specified size and key type."""
        data = {}
        for i in range(size):
            if key_type == "string":
                key = f"key_{i:010d}"  # Fixed-length string keys
            elif key_type == "datetime":
                key = datetime.datetime(2024, 1, 1) + datetime.timedelta(minutes=i)
            else:
                raise ValueError(f"Unknown key_type: {key_type}")
            
            # Generate random value dict with consistent size
            value = {
                "id": i,
                "name": ''.join(random.choices(string.ascii_letters, k=10)),
                "value": random.random(),
                "tags": [random.randint(1, 100) for _ in range(5)]
            }
            data[key] = value
        return data

    def profile_operation(self, operation_name: str, func, *args, **kwargs):
        """Profile a single operation and return execution time."""
        start_time = time.time()
        func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        print(f"{operation_name}: {duration:.4f} seconds")
        return duration

    def run_size_benchmark(self, sizes=[100, 1000, 10000, 1000000]):
        """Run benchmarks with different data sizes."""
        print("\n=== Size Benchmark ===")
        print("Note: Testing with 1 million records may take several minutes.")
        print("Progress will be shown for each operation.")
        results = []
        
        for size in sizes:
            print(f"\nTesting with {size:,} records:")
            print("Generating test data...")
            data = self.generate_test_data(size)
            
            # Profile save operation
            save_time = self.profile_operation(
                f"Save {size:,} records",
                save_jsonl,
                self.file_path,
                data
            )
            
            # Profile load operation
            load_time = self.profile_operation(
                f"Load {size:,} records",
                load_jsonl,
                self.file_path
            )
            
            # Profile select operation (middle range)
            mid_key = f"key_{size//2:010d}"
            end_key = f"key_{(size//2 + size//10):010d}"
            select_time = self.profile_operation(
                f"Select {size//10:,} records from {size:,}",
                select_jsonl,
                self.file_path,
                (mid_key, end_key)
            )
            
            # Profile update operation (10% of records)
            print(f"Preparing to update {size//10:,} records...")
            update_data = {k: v for k, v in list(data.items())[:size//10]}
            update_time = self.profile_operation(
                f"Update {size//10:,} records in {size:,}",
                update_jsonl,
                self.file_path,
                update_data
            )
            
            # Profile delete operation (10% of records)
            print(f"Preparing to delete {size//10:,} records...")
            delete_keys = list(data.keys())[:size//10]
            delete_time = self.profile_operation(
                f"Delete {size//10:,} records from {size:,}",
                delete_jsonl,
                self.file_path,
                delete_keys
            )
            
            results.append({
                'size': size,
                'save': save_time,
                'load': load_time,
                'select': select_time,
                'update': update_time,
                'delete': delete_time
            })
            
            print(f"Cleaning up {size:,} records test...")
            self.cleanup()
        
        return results

    def run_key_type_benchmark(self, size=1000):
        """Compare performance with different key types."""
        print("\n=== Key Type Benchmark ===")
        results = {}
        
        for key_type in ["string", "datetime"]:
            print(f"\nTesting with {key_type} keys:")
            data = self.generate_test_data(size, key_type)
            
            results[key_type] = {
                'save': self.profile_operation(
                    f"Save ({key_type} keys)",
                    save_jsonl,
                    self.file_path,
                    data
                ),
                'load': self.profile_operation(
                    f"Load ({key_type} keys)",
                    load_jsonl,
                    self.file_path
                )
            }
            
            self.cleanup()
        
        return results

    def run_index_benchmark(self, size=1000):
        """Profile index building and usage."""
        print("\n=== Index Benchmark ===")
        data = self.generate_test_data(size)
        
        # Save without index
        with open(self.file_path, 'w') as f:
            for key, value in data.items():
                f.write(f'{{"key_{key}": {value}}}\n')
        
        # Profile index building
        index_time = self.profile_operation(
            "Build index",
            build_jsonl_index,
            self.file_path
        )
        
        self.cleanup()
        return index_time

def main():
    profiler = JsonlFileProfiler()
    
    # Run detailed profiling with cProfile
    print("Running detailed profiling...")
    pr = cProfile.Profile()
    pr.enable()
    
    # Run benchmarks
    size_results = profiler.run_size_benchmark()
    key_type_results = profiler.run_key_type_benchmark()
    index_time = profiler.run_index_benchmark()
    
    pr.disable()
    
    # Print detailed profiling results
    print("\n=== Detailed Profiling Results ===")
    stats = pstats.Stats(pr)
    stats.sort_stats('cumulative')
    stats.print_stats(20)  # Show top 20 time-consuming functions
    
    # Print summary
    print("\n=== Performance Summary ===")
    print("\nSize Scaling:")
    for result in size_results:
        print(f"\nSize: {result['size']} records")
        print(f"Save: {result['save']:.4f}s")
        print(f"Load: {result['load']:.4f}s")
        print(f"Select: {result['select']:.4f}s")
        print(f"Update: {result['update']:.4f}s")
        print(f"Delete: {result['delete']:.4f}s")
    
    print("\nKey Type Comparison:")
    for key_type, times in key_type_results.items():
        print(f"\n{key_type} keys:")
        print(f"Save: {times['save']:.4f}s")
        print(f"Load: {times['load']:.4f}s")
    
    print(f"\nIndex Building Time: {index_time:.4f}s")

if __name__ == "__main__":
    main() 