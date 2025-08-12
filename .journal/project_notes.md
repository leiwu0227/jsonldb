# JsonlDB Project Overview

## Core Concept
JsonlDB is a simple file-based database using JSONL format where:
- **Folders** = Databases
- **JSONL files** = Tables  
- **Lines** = Rows (key → dict mapping)

## Architecture (3-Layer)

### 1. jsonlfile.py (Foundation)
- Core JSONL operations with dict storage
- Index files (*.idx) for fast row access without loading full file
- Optimized with orjson, numba, and memory mapping

### 2. jsonldf.py (DataFrame Layer)  
- Pandas DataFrame ↔ JSONL conversion
- Uses DataFrame index as JSONL keys

### 3. folderdb.py (Database Layer)
- Multi-table database operations
- **Hierarchy support**: Splits large folders into subdirectories for performance
- File organization: `level1.level2.jsonl` → nested folder structure

## Additional Features
- **Version Control** (vercontrol.py): Git integration for database versioning
- **Visualization** (visual.py): Bokeh-based plotting from JSONL data
- **Maintenance**: `lint()` function syncs index files with JSONL data

## Performance
- Index-based row access avoids full file loading
- Numba optimization for sorting operations
- Configurable buffer sizes and time specifications 