# Version Control Example

This example demonstrates how to use the version control functions to manage JSONL files using Git.

## Prerequisites

1. Python 3.6 or higher
2. Git installed on your system
3. Required Python packages:
   ```bash
   pip install gitpython
   ```

## Example Structure

The example consists of:
- `version_control_example.py`: Main example script demonstrating all version control functions
- `README.md`: This file with usage instructions

## What the Example Does

The example script demonstrates:
1. Initializing a Git repository
2. Creating and committing sample JSONL data
3. Making multiple commits with different data
4. Listing all versions
5. Reverting to a previous version
6. Cleaning up the test environment

## Running the Example

1. Navigate to the example directory:
   ```bash
   cd example_vercontrol
   ```

2. Run the example script:
   ```bash
   python version_control_example.py
   ```

## Expected Output

The script will:
1. Create a test folder
2. Initialize it as a Git repository
3. Create and commit sample data three times
4. Show all commit versions
5. Revert to the first version
6. Show the versions again
7. Clean up the test folder

## Understanding the Output

- Each commit will have a unique hash
- Commit messages will follow the format:
  - Auto commits: "Auto Commit: YYYY-MM-DD@HH-MM"
  - Manual commits: "Manual Commit: YYYY-MM-DD@HH-MM message"
- The revert operation will restore the data to the state of the first commit

## Customization

You can modify the example by:
1. Changing the sample data structure in `create_sample_data()`
2. Adding more commits with different messages
3. Reverting to different versions
4. Modifying the cleanup behavior

## Error Handling

The example includes error handling for:
- Git repository initialization
- Commit operations
- Version listing
- Revert operations
- File system operations 