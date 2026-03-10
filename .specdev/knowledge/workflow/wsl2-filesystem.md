# WSL2 Filesystem Constraints

The project runs on WSL2 with source files on `/mnt/h/` (Windows host filesystem). Key constraints:

- **mtime resolution is ~1 second** — tests that check file modification time after writes may see unchanged mtime if the operation completes within 1 second. Use file content comparison instead of mtime for test assertions.
- **CRLF/LF conversion** — Git may warn about CRLF being replaced by LF. This is normal for Windows-hosted repos edited from WSL2.
