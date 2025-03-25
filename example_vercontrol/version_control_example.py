import os
import sys
import json
from datetime import datetime
import shutil

# Add parent directory to path to import vercontrol
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from vercontrol import init_folder, commit, list_version, revert

def create_sample_data(folder_path: str, version: int) -> None:
    """Create sample JSONL data for testing.
    
    Args:
        folder_path (str): Path to create the data
        version (int): Version number for the data
    """
    # Create sample data
    data = {
        "version": version,
        "timestamp": datetime.now().isoformat(),
        "records": [
            {"id": 1, "value": f"test_value_{version}_1"},
            {"id": 2, "value": f"test_value_{version}_2"}
        ]
    }
    
    # Save to JSONL file
    with open(os.path.join(folder_path, "data.jsonl"), "w") as f:
        for record in data["records"]:
            f.write(json.dumps(record) + "\n")

def main():
    # Create a test folder
    test_folder = "test_version_control"
    if os.path.exists(test_folder):
        shutil.rmtree(test_folder)
    os.makedirs(test_folder)
    
    print("\n=== Version Control Example ===")
    
    # Initialize git repository
    print("\n1. Initializing git repository...")
    init_folder(test_folder)
    
    # Create and commit initial data
    print("\n2. Creating initial data...")
    create_sample_data(test_folder, 1)
    commit(test_folder)
    
    # Create and commit updated data
    print("\n3. Creating updated data...")
    create_sample_data(test_folder, 2)
    commit(test_folder, "Updated test values")
    
    # Create and commit more data
    print("\n4. Creating more data...")
    create_sample_data(test_folder, 3)
    commit(test_folder)
    
    # List all versions
    print("\n5. Listing all versions:")
    versions = list_version(test_folder)
    for hash_value, msg in versions.items():
        print(f"Hash: {hash_value[:8]}... Message: {msg}")
    
    # Revert to first version
    print("\n6. Reverting to first version...")
    first_commit = list(versions.keys())[-1]  # Get the first commit hash
    revert(test_folder, first_commit)
    
    # List versions again
    print("\n7. Listing versions after revert:")
    versions = list_version(test_folder)
    for hash_value, msg in versions.items():
        print(f"Hash: {hash_value[:8]}... Message: {msg}")
    
    # Clean up
    print("\n8. Cleaning up...")
    shutil.rmtree(test_folder)
    print("\nExample completed successfully!")

if __name__ == "__main__":
    main() 