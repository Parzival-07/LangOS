[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/0ek2UV58)

# Distributed File Collaboration System — User Manual

This project implements a distributed file collaboration system with three components:
- **Name Server (NM)**: Central registry, file routing, user tracking, and command execution helper
- **Storage Server (SS)**: File storage with metadata, ACL enforcement, sentence-level locking, checkpoints, undo history, and trash bin
- **Client CLI**: Interactive REPL with colorized logs for all user operations

**Defaults**: Name Server listens on `127.0.0.1:8000`. Storage data root is `ss_data/`.

**Key Design Decisions**:
- Binary protocol with size-prefixed messages (no sentinel collisions)
- O(1) file lookup using FNV-1a hash map (4096 buckets)
- Sentence-level write locking for concurrent editing
- Persistent metadata and undo history
- Trash bin for recoverable deletions

## Build and Run

1) Build everything
	- make

2) Start the Name Server in one terminal
	- ./name_server

3) Start one or more Storage Servers (each needs a client port), e.g.
	- ./storage_server 9001
	- Or with custom Name Server IP: `./storage_server 9001 192.168.1.100`

4) Start a Client in a separate terminal and enter a username when prompted
	- ./client
	- Or with custom Name Server IP: `./client 192.168.1.100`

Order matters: start NM → SS → client. Each Storage Server registers itself with the Name Server.

### Network Configuration

By default, all components connect to Name Server at `127.0.0.1:8000` (localhost). To run across multiple machines:

**Scenario: Friend hosts Name Server on IP `192.168.1.100`**
```bash
# On friend's laptop (192.168.1.100)
./name_server

# On your laptop - Storage Server
./storage_server 9001 192.168.1.100

# On your laptop - Client
./client 192.168.1.100
```

**Requirements**:
- Name Server must be reachable on the network (check firewall rules)
- All machines must be on same network or have proper routing
- Port 8000 must be open for Name Server connections
- Storage Server client ports (e.g., 9001) must be accessible to clients

## Architecture & Data Model

### File Metadata
Each file has a corresponding `.meta` file in `ss_data/` containing:
- **owner**: Username who created the file (cannot be changed)
- **created**: Unix timestamp of file creation
- **updated**: Unix timestamp of last content modification (WRITE/CLEAR/UNDO/REVERT)
- **accessed**: Unix timestamp of last read/stream operation (updated on READ/STREAM)
- **readers**: Comma-separated list of usernames with read access
- **writers**: Comma-separated list of usernames with write access (writer implies reader)

### Access Control Rules
- **Owner**: Full control (read, write, delete, modify ACL)
- **Writer**: Can read, write, and delete; cannot modify ACL
- **Reader**: Can read only
- **Implicit access**: Writers automatically have read access

### Locking Mechanism
- **Sentence-level locks**: Files are parsed into sentences by delimiters (`.`, `!`, `?`)
- **Lock granularity**: Each sentence can be locked independently (2048 lock slots)
- **Lock scope**: WRITE acquires lock on specific sentence; CLEAR/DELETE/REVERT blocked if ANY lock exists
- **Auto-release**: Locks released on WRITE_DONE or client disconnect
- **Conflict handling**: Returns 423 (Locked) if lock acquisition fails

### Undo System
- **Snapshots**: Taken before WRITE, CLEAR, REVERT operations
- **Storage**: `ss_data/.undo/<filename>/<N>.bak` (numbered sequentially)
- **Single-level**: UNDO restores the most recent snapshot only
- **Preservation**: Undo history persists across server restarts

### Trash Bin
- **Location**: `ss_data/.trash/<owner>/` per-owner subdirectories
- **Behavior**: DELETE moves files instead of removing them
- **Recovery**: RECOVER can restore with original or new name
- **Visibility**: Trashed files excluded from VIEW/INFO listings

## Commands Reference

### CREATE - File Creation

**Syntax**: `CREATE <filename>`

**Implementation**:
1. Client sends CREATE request to Name Server with authenticated username
2. Name Server selects a Storage Server (round-robin/first-available)
3. Storage Server validates filename (alphanumeric, `-`, `_`, `.` only; max 256 chars)
4. Creates empty content file: `ss_data/<filename>`
5. Creates metadata file: `ss_data/<filename>.meta` with:
   - `owner:<username>`
   - `created:<timestamp>`
   - `updated:<timestamp>`
   - `readers:` (empty)
   - `writers:` (empty)
6. Name Server updates registry cache with new file mapping

**Access Requirements**: Any authenticated user (becomes owner)

**Error Cases**:
- 409 Conflict: File already exists
- 400 Bad Request: Invalid filename (special chars, path traversal, empty)
- 503 Service Unavailable: No Storage Server available

**Assumptions**:
- Filenames are case-sensitive
- No nested directories (flat namespace)
- File ownership is permanent

---

### DELETE - Move to Trash

**Syntax**: `DELETE <filename>`

**Implementation**:
1. Client sends DELETE request through Name Server
2. Name Server routes to owning Storage Server
3. Storage Server checks for active sentence locks (fails with 423 if any exist)
4. Validates requester is owner (only owner can delete)
5. Reads owner from `.meta` file
6. Creates trash directory: `ss_data/.trash/<owner>/` (if not exists)
7. Atomically moves both files:
   - `ss_data/<filename>` → `ss_data/.trash/<owner>/<filename>`
   - `ss_data/<filename>.meta` → `ss_data/.trash/<owner>/<filename>.meta`
8. Name Server removes file from registry cache

**Access Requirements**: Owner only

**Error Cases**:
- 403 Forbidden: Non-owner attempting delete
- 404 Not Found: File doesn't exist
- 423 Locked: Active write lock on any sentence

**Assumptions**:
- DELETE is recoverable via RECOVER command
- Trash is per-owner (users cannot access others' trash)
- If file already exists in trash, it's overwritten

---

### CLEAR - Truncate File Content

**Syntax**: `CLEAR <filename>`

**Implementation**:
1. Client sends CLEAR request through Name Server
2. Storage Server validates write access (owner or writer)
3. Checks for active sentence locks (fails with 423 if any exist)
4. Takes undo snapshot: copies current content to `ss_data/.undo/<filename>/<N>.bak`
5. Truncates file to zero bytes using `fopen(path, "w")`
6. Updates metadata: `updated:<new_timestamp>`
7. Preserves `accessed:` timestamp and ACL entries

**Access Requirements**: Owner or writer

**Error Cases**:
- 403 Forbidden: No write access
- 404 Not Found: File doesn't exist
- 423 Locked: Active write lock exists

**Assumptions**:
- CLEAR is undoable (use UNDO to restore)
- Metadata (owner, ACL) preserved
- Empty file still exists after CLEAR

---

### READ - Display File Content

**Syntax**: `READ <filename>`

**Implementation**:
1. Client queries Name Server for file location
2. Name Server returns Storage Server IP and port (from registry cache)
3. Client connects directly to Storage Server
4. Storage Server validates read access:
   - Owner → allowed
   - Username in readers list → allowed
   - Username in writers list → allowed (writer implies reader)
   - Otherwise → 403 Forbidden
5. Opens file and sends entire content as binary payload
6. Updates `accessed:<timestamp>` in metadata
7. Client displays content to stdout

**Access Requirements**: Owner, reader, or writer

**Error Cases**:
- 403 Forbidden: No read access
- 404 Not Found: File doesn't exist or not found in registry

**Assumptions**:
- File content sent as-is (preserves encoding, whitespace)
- No size limit enforced (entire file loaded into memory)
- READ doesn't require locking (concurrent reads allowed)

---

### WRITE - Interactive Sentence Editing

**Syntax**: `WRITE <filename> <sentence_index>`

**Implementation**:

**Phase 1 - Lock Acquisition (WRITE_BEGIN)**:
1. Client sends WRITE_BEGIN with filename and 0-based sentence index
2. Storage Server validates write access (owner or writer)
3. Parses file into sentences using delimiters (`.`, `!`, `?`)
4. Checks if requested sentence exists (or allows append if index = sentence_count)
5. Attempts to acquire lock on that sentence index:
   - Checks global lock table (2048 slots) for conflicts
   - If already locked by another client → 423 Locked
   - If available → records lock (filename, sentence_index, client_socket)
6. Returns success with current sentence content

**Phase 2 - Interactive Editing**:
7. Client displays current sentence split into words (1-based indexing)
8. User enters replacements: `<word_number> <new_text>` (one per line)
9. User types `ETIRW` when done editing

**Phase 3 - Apply Changes (WRITE_FILE)**:
10. Takes undo snapshot before modification
11. Reconstructs sentence with user replacements:
    - Maintains original word positions
    - Insertions allowed (appends new words at specified indices)
    - Sentence delimiters (`.`, `!`, `?`) split words if inserted mid-word
12. Rebuilds entire file with modified sentence
13. Writes updated content atomically
14. Updates metadata: `updated:<timestamp>`
15. Sends success response

**Phase 4 - Lock Release (WRITE_DONE)**:
16. Client sends WRITE_DONE
17. Storage Server removes lock from table
18. Lock also auto-released if client disconnects

**Access Requirements**: Owner or writer

**Error Cases**:
- 403 Forbidden: No write access
- 404 Not Found: File doesn't exist
- 423 Locked: Sentence already locked by another client
- 400 Bad Request: Invalid sentence index (beyond file length + 1)

**Assumptions**:
- Sentences parsed by `.`, `!`, `?` followed by whitespace or EOF
- Word numbering is 1-based for user convenience (internally 0-based)
- Appending allowed: sentence_index == total_sentences creates new sentence
- Multiple delimiters in replacement text create multiple sentences
- Locks held until explicit WRITE_DONE or disconnect
- Concurrent writes to different sentences allowed

---

### VIEW - List Files

**Syntax**: `VIEW [-a] [-l] [-al]`

**Flags**:
- (no flags): List files you can access (own, reader, writer)
- `-a`: List all files in the system
- `-l`: Long format (includes owner, size, word count, char count, timestamps)
- `-al`: All files with long format

**Implementation**:
1. Client sends VIEW request to Name Server with flags
2. Name Server checks registry cache:
   - If empty or stale → refreshes from all Storage Servers via CMD_SS_LIST_FILES
   - Otherwise → serves from cache (O(1) lookup via hash index)
3. For each file, filters by access:
   - Without `-a`: Include only if user is owner/reader/writer
   - With `-a`: Include all files
4. For long listing (`-l`):
   - Retrieves cached metadata (owner, size, timestamps, ACL)
   - Only refreshes from SS if cache entry is completely empty
   - Formats: `filename\towner:X\tsize:Y\twords:Z\tchars:W\tlast_access:...\tlast_modified:...`
5. Returns newline-separated file list

**Access Requirements**: Any authenticated user

**Performance Optimizations**:
- **Smart caching**: Only refreshes metadata on cache miss, not on every VIEW
- **Hash-based registry**: O(1) file lookup using FNV-1a hash (4096 buckets)
- **Lazy refresh**: VIEW without flags doesn't trigger SS queries if cache populated
- **Parallel SS queries**: When refresh needed, queries all SS concurrently

**Error Cases**: None (returns empty list if no accessible files)

**Assumptions**:
- Cache updated on CREATE/DELETE/INFO operations
- Trashed files excluded from VIEW results
- Timestamps shown in both human-readable and Unix epoch formats

---

### INFO - Detailed File Metadata

**Syntax**: `INFO <filename>`

**Implementation**:
1. Client sends INFO request to Name Server
2. Name Server looks up file in registry (O(1) hash lookup)
3. If not in cache → refreshes from all Storage Servers
4. Routes request to owning Storage Server
5. Storage Server:
   - Validates access (owner, reader, or writer)
   - Reads `.meta` file for: owner, created, updated, accessed, readers, writers
   - Stats file using `stat()` for: size_bytes
   - Reads content and computes:
     - `words_cnt`: Tokens split by whitespace
     - `chars_cnt`: Total characters
   - Formats response with all fields
6. Name Server caches result and forwards to client

**Access Requirements**: Owner, reader, or writer

**Output Format**:
```
filename: <name>
owner: <username>
created: YYYY-MM-DD HH:MM:SS (<unix_timestamp>)
updated: YYYY-MM-DD HH:MM:SS (<unix_timestamp>)
size: <bytes>
words: <count>
chars: <count>
last_access: YYYY-MM-DD HH:MM:SS (<unix_timestamp>)
last_modified: YYYY-MM-DD HH:MM:SS (<unix_timestamp>)
readers: <comma_separated_usernames>
writers: <comma_separated_usernames>
```

**Error Cases**:
- 403 Forbidden: No access to file
- 404 Not Found: File doesn't exist

**Assumptions**:
- `last_access` = most recent `accessed:` timestamp from metadata
- `last_modified` = `updated:` timestamp from metadata
- Words counted by whitespace tokenization
- Empty ACL lists displayed as empty line

---

### ADDACCESS - Grant Access

**Syntax**: `ADDACCESS -R|-W <filename> <username>`

**Flags**:
- `-R`: Grant read access only
- `-W`: Grant write access (automatically includes read access)

**Implementation**:
1. Client sends ADDACCESS through Name Server
2. Name Server validates target user exists:
   - Checks `historical_users` array (tracks all ever-registered users)
   - Returns 404 if target user never registered
3. Routes to owning Storage Server
4. Storage Server:
   - Validates requester is owner (403 if not)
   - Reads current `.meta` file
   - For `-R`: Adds username to `readers:` list (if not already present)
   - For `-W`: Adds username to both `writers:` and `readers:` lists
   - Checks for duplicates (returns 409 if already has that access)
   - Rewrites `.meta` file with updated ACL
5. Name Server updates cached metadata

**Access Requirements**: Owner only

**Error Cases**:
- 403 Forbidden: Non-owner attempting to modify ACL
- 404 Not Found: File doesn't exist OR target user never registered
- 409 Conflict: User already has requested access level

**Assumptions**:
- Writer access implies reader access (enforced at grant time)
- ACL stored as comma-separated lists in metadata
- No limit on number of readers/writers
- Cannot grant access to non-existent users (validated by Name Server)

---

### REMACCESS - Remove Access

**Syntax**: `REMACCESS <filename> <username>` or `REMACCESS -R|-W <filename> <username>`

**Implementation**:
1. Client sends REMACCESS through Name Server (flag optional, ignored)
2. Routes to owning Storage Server
3. Storage Server:
   - Validates requester is owner (403 if not)
   - Reads current `.meta` file
   - Removes username from BOTH `readers:` and `writers:` lists
   - Returns 404 if username not found in either list
   - Rewrites `.meta` file
4. Name Server updates cached metadata

**Access Requirements**: Owner only

**Error Cases**:
- 403 Forbidden: Non-owner attempting removal
- 404 Not Found: File doesn't exist OR user not in ACL
- 404 Not Found: Attempting to remove owner (owner cannot be removed)

**Assumptions**:
- Removal is role-agnostic (removes from both lists regardless of flag)
- Owner cannot be removed from their own file
- Client accepts optional `-R|-W` flag for consistency but server ignores it

---

### CHECKPOINT - Create Named Snapshot

**Syntax**: `CHECKPOINT <filename> <tag>`

**Implementation**:
1. Client sends CHECKPOINT request through Name Server
2. Storage Server:
   - Validates write access (owner or writer)
   - Checks for active sentence locks (fails with 423 if any exist)
   - Validates tag format: 1-64 chars, [A-Za-z0-9_-] only
   - Creates checkpoint directory: `ss_data/.checkpoints/<filename>/` (if needed)
   - Checks if tag already exists (409 if duplicate)
   - Copies entire file content to: `ss_data/.checkpoints/<filename>/<tag>`
   - Does NOT update metadata timestamps

**Access Requirements**: Owner or writer

**Error Cases**:
- 403 Forbidden: No write access
- 404 Not Found: File doesn't exist
- 409 Conflict: Tag already exists
- 423 Locked: Active write lock exists
- 400 Bad Request: Invalid tag format

**Assumptions**:
- Checkpoints are immutable (cannot overwrite existing tag)
- No limit on number of checkpoints per file
- Checkpoints persist across server restarts
- Checkpoint content is full file snapshot (not differential)

---

### LISTCHECKPOINTS - List All Tags

**Syntax**: `LISTCHECKPOINTS <filename>`

**Implementation**:
1. Storage Server validates read access
2. Reads directory: `ss_data/.checkpoints/<filename>/`
3. Returns newline-separated list of tag names
4. Empty response if no checkpoints exist

**Access Requirements**: Owner, reader, or writer

**Error Cases**:
- 403 Forbidden: No read access
- 404 Not Found: File doesn't exist

---

### VIEWCHECKPOINT - Display Checkpoint Content

**Syntax**: `VIEWCHECKPOINT <filename> <tag>`

**Implementation**:
1. Storage Server validates read access
2. Reads file: `ss_data/.checkpoints/<filename>/<tag>`
3. Sends raw content as binary payload
4. Client displays to stdout

**Access Requirements**: Owner, reader, or writer

**Error Cases**:
- 403 Forbidden: No read access
- 404 Not Found: File or checkpoint doesn't exist

---

### REVERT - Restore from Checkpoint

**Syntax**: `REVERT <filename> <tag>`

**Implementation**:
1. Storage Server validates write access (owner or writer)
2. Checks for active sentence locks (fails with 423 if any exist)
3. Takes undo snapshot of current content (before overwriting)
4. Reads checkpoint content from: `ss_data/.checkpoints/<filename>/<tag>`
5. Overwrites current file content: `ss_data/<filename>`
6. Updates metadata: `updated:<timestamp>`
7. Preserves: owner, ACL, accessed timestamp

**Access Requirements**: Owner or writer

**Error Cases**:
- 403 Forbidden: No write access
- 404 Not Found: File or checkpoint doesn't exist
- 423 Locked: Active write lock exists

**Assumptions**:
- REVERT is undoable (undo snapshot taken before revert)
- Does NOT delete the checkpoint after reverting
- Checkpoint content replaces entire file (not merged)

---

### TRASH - List Trashed Files

**Syntax**: `TRASH`

**Implementation**:
1. Name Server broadcasts TRASH_LIST to all Storage Servers
2. Each Storage Server:
   - Reads directory: `ss_data/.trash/<authenticated_user>/`
   - Returns newline-separated list of filenames
3. Name Server aggregates results from all servers
4. Client displays combined list

**Access Requirements**: Any authenticated user (sees only their own trash)

**Error Cases**: None (returns empty if trash is empty)

**Assumptions**:
- Trash is per-owner (isolated by username)
- Trashed files retain original filename
- If same filename deleted multiple times, latest delete overwrites previous

---

### RECOVER - Restore from Trash

**Syntax**: `RECOVER <filename>` or `RECOVER <filename> <newname>`

**Implementation**:
1. Name Server queries all Storage Servers for trashed file
2. First Storage Server with matching file in `ss_data/.trash/<user>/` handles recovery:
   - Validates target name not in use (409 if exists)
   - Uses `newname` if provided, otherwise uses original filename
   - Atomically moves both files:
     - `ss_data/.trash/<user>/<filename>` → `ss_data/<newname>`
     - `ss_data/.trash/<user>/<filename>.meta` → `ss_data/<newname>.meta`
3. Name Server adds recovered file to registry cache
4. Storage Server refreshes metadata for new file

**Access Requirements**: Owner only (implicitly enforced by trash directory structure)

**Error Cases**:
- 404 Not Found: File not in any trash bin
- 409 Conflict: Target filename already exists

**Assumptions**:
- Recovered files retain original owner and ACL
- Can rename during recovery to avoid conflicts
- Recovery is atomic (both content and metadata moved together)

---

### EMPTYTRASH - Permanently Delete

**Syntax**: `EMPTYTRASH -all` or `EMPTYTRASH <filename>`

**Implementation**:

**For `-all` flag**:
1. Name Server broadcasts EMPTYTRASH to all Storage Servers
2. Each Storage Server:
   - Recursively removes: `ss_data/.trash/<authenticated_user>/`
   - Recreates empty directory for future use
3. Returns success if any server had items to remove

**For specific filename**:
1. Name Server queries all Storage Servers
2. First server with matching file permanently removes:
   - `ss_data/.trash/<user>/<filename>`
   - `ss_data/.trash/<user>/<filename>.meta`

**Access Requirements**: Owner only (trash directory enforces isolation)

**Error Cases**:
- 404 Not Found: Specified file not in trash (for single-file mode)

**Assumptions**:
- EMPTYTRASH is irreversible (no undo)
- `-all` removes everything, regardless of when deleted
- Single-file mode useful for selective cleanup

---

### STREAM - Live File Streaming

**Syntax**: `STREAM <filename>`

**Implementation**:
1. Client queries Name Server for file location (like READ)
2. Client connects directly to Storage Server
3. Storage Server validates read access (owner/reader/writer)
4. Gets file size via `stat()`
5. Sends `CMD_ACK` header with `payload_size = file_size_bytes`
6. Streams file word-by-word with delays:
   - Reads character-by-character
   - Accumulates word until whitespace encountered
   - Sends word + trailing whitespace
   - Sleeps 100ms between words (simulates streaming delay)
7. Updates `accessed:<timestamp>` in metadata
8. Client reads exactly `payload_size` bytes and displays in real-time

**Access Requirements**: Owner, reader, or writer

**Protocol Design**:
- **Size-prefixed protocol**: Client knows exact byte count to expect
- **No sentinel collision**: No special "STOP" marker needed (avoids issues if "STOP" appears in content)
- **Unbuffered stdout**: Client uses `setvbuf(stdout, NULL, _IONBF, 0)` for live display
- **Word-level granularity**: Entire words sent atomically with whitespace preserved

**Error Cases**:
- 403 Forbidden: No read access
- 404 Not Found: File doesn't exist
- Connection closed: If client disconnects mid-stream, server releases resources immediately

**Assumptions**:
- Stream is read-only (no locking required)
- Whitespace preserved exactly as in file
- 100ms delay per word is configurable (hardcoded for demo)
- Concurrent streams to same file allowed

---

### EXEC - Execute File as Script

**Syntax**: `EXEC <filename>`

**Implementation**:
1. Client sends EXEC request to Name Server (NOT Storage Server)
2. Name Server:
   - Resolves file location from registry
   - Fetches file content from Storage Server using internal READ
   - Validates requester has read access (owner/reader/writer)
3. Name Server writes content to temporary file: `/tmp/nm_exec_XXXXXX`
4. Executes via shell: `/bin/sh -s < /tmp/nm_exec_XXXXXX 2>&1`
5. Captures stdout and stderr combined
6. Sends output back to client as ACK payload
7. Removes temporary file after execution

**Access Requirements**: Owner, reader, or writer (same as READ)

**Error Cases**:
- 403 Forbidden: No read access
- 404 Not Found: File doesn't exist
- 500 Internal Error: mkstemp or popen failure

**Assumptions**:
- Script executed with `/bin/sh` (POSIX shell)
- Output captured after full execution (not streamed)
- Return code not reported (only stdout/stderr)
- No timeout mechanism (long-running scripts block)

---

### LIST - Show All Registered Users

**Syntax**: `LIST`

**Implementation**:
1. Name Server maintains `historical_users` array (tracks all ever-connected users)
2. When client registers (CMD_REGISTER_CLIENT):
   - Username added to array if not present
   - Array persists for lifetime of Name Server process
3. LIST returns newline-separated list of all usernames ever registered

**Access Requirements**: Any authenticated user

**Error Cases**: None

**Assumptions**:
- Historical tracking (includes disconnected users)
- No user removal mechanism
- Array limited to 1024 users (MAX_HISTORICAL_USERS)
- Order not guaranteed

---

### UNDO - Restore Previous Version

**Syntax**: `UNDO <filename>`

**Implementation**:
1. Client sends UNDO through Name Server
2. Storage Server:
   - Validates write access (owner or writer)
   - Reads undo directory: `ss_data/.undo/<filename>/`
   - Finds highest numbered snapshot: `<N>.bak`
   - Returns 404 if no undo history exists
3. Copies snapshot content to current file: `ss_data/<filename>`
4. Updates metadata: `updated:<timestamp>`
5. Preserves: owner, ACL, accessed timestamp
6. Deletes used snapshot file (single-level undo)

**Access Requirements**: Owner or writer

**Error Cases**:
- 403 Forbidden: No write access
- 404 Not Found: File doesn't exist or no undo history

**Assumptions**:
- Single-level undo only (most recent snapshot)
- Snapshot consumed after UNDO (cannot undo same change twice)
- UNDO itself does NOT create new snapshot
- Undo history persists across server restarts

---

## Error Codes

| Code | Name | Usage |
|------|------|-------|
| 400 | Bad Request | Invalid filename, bad payload, malformed request |
| 403 | Forbidden | Insufficient permissions (not owner/reader/writer) |
| 404 | Not Found | File, checkpoint, user, or trash item doesn't exist |
| 409 | Conflict | Duplicate creation (file/tag already exists) |
| 423 | Locked | Operation blocked by active sentence lock |
| 500 | Internal Error | Server-side failure (I/O error, malloc failure) |
| 502 | Bad Gateway | Name Server cannot contact Storage Server |
| 503 | Service Unavailable | No Storage Server registered |

---

## Technical Implementation Details

### Protocol Design
- **Binary framing**: Every message has `MsgHeader {CommandCode, int payload_size}`
- **Size-prefixed payloads**: Eliminates need for sentinels or escape sequences
- **No sentinel collision**: STREAM uses exact byte count, not "STOP" marker
- **Direct SS connections**: Client connects to Storage Server directly for READ/WRITE/STREAM (reduces Name Server load)

### Performance Optimizations
- **O(1) file lookup**: FNV-1a hash map with 4096 buckets in Name Server registry
- **Smart caching**: Metadata refreshed only on cache miss, not on every VIEW
- **Lazy evaluation**: VIEW doesn't query Storage Servers if cache is warm
- **Concurrent operations**: Multiple readers allowed; writes to different sentences can proceed in parallel

### Concurrency Control
- **Sentence-level locking**: 2048 lock slots allow fine-grained parallelism
- **Per-SS serialization**: `ss_io_mutex` prevents interleaved Name Server ↔ Storage Server messages
- **Auto-cleanup**: Locks released on client disconnect via connection tracking
- **Lock-free reads**: No locking required for READ/STREAM/INFO operations

### File Storage Layout
```
ss_data/
├── <filename>                    # Content files (flat namespace)
├── <filename>.meta               # Metadata (owner, ACL, timestamps)
├── .undo/
│   └── <filename>/
│       └── <N>.bak               # Undo snapshots (numbered sequentially)
├── .trash/
│   └── <owner>/
│       ├── <filename>            # Deleted content
│       └── <filename>.meta       # Deleted metadata
└── .checkpoints/
    └── <filename>/
        └── <tag>                 # Tagged snapshots
```

### Assumptions and Limitations
1. **Flat namespace**: No nested directories, all files at root level
2. **Filename restrictions**: Alphanumeric, `-`, `_`, `.` only; max 256 chars
3. **Single Name Server**: No NM replication or failover
4. **Single-level undo**: Only most recent change can be undone
5. **In-memory registry**: File mappings lost on NM restart (rebuilt from SS)
6. **No authentication**: Username is self-declared, no password verification
7. **No encryption**: All data transmitted in plaintext
8. **No quota limits**: Users can create unlimited files
9. **Fixed buffer sizes**: 16KB for file lists, 2048 bytes for sentence replacements
10. **POSIX dependency**: Uses Linux-specific system calls (not fully portable to Windows)
11. **No file replication**: Each file resides on exactly one Storage Server (the one where it was created). Files are NOT replicated or migrated between Storage Servers. If a Storage Server goes offline, its files become inaccessible until the server returns. This is a deliberate design choice for simplicity and is common in distributed systems without redundancy.

---

## Usage Examples

### Basic Workflow
```bash
# Alice creates and edits a file
alice> CREATE report.txt
[Client] File 'report.txt' created.

alice> WRITE report.txt 0
[Client] Sentence 0 has 0 words. Enter replacements or append (e.g., "1 text"), then type ETIRW:
1 This
2 is
3 my
4 report.
ETIRW
[Client] WRITE applied.

alice> READ report.txt
This is my report.

# Grant access to Bob
alice> ADDACCESS -R report.txt bob
[Client] Access added.

# Bob can now read
bob> READ report.txt
This is my report.
```

### Collaborative Editing
```bash
# Alice writes sentence 0, Bob writes sentence 1 (concurrent)
alice> WRITE report.txt 0
1 Introduction:
2 This
3 document
4 covers.
ETIRW

bob> WRITE report.txt 1
1 We
2 will
3 discuss
4 the
5 findings.
ETIRW

alice> READ report.txt
Introduction: This document covers. We will discuss the findings.
```

### Checkpoint and Recovery
```bash
alice> CHECKPOINT report.txt v1
[Client] Checkpoint created.

alice> WRITE report.txt 0
[... makes changes ...]

alice> LISTCHECKPOINTS report.txt
v1

alice> REVERT report.txt v1
[Client] File reverted to checkpoint 'v1'.
```

### Trash and Recovery
```bash
alice> DELETE report.txt
[Client] File 'report.txt' deleted.

alice> TRASH
report.txt

alice> RECOVER report.txt
[Client] File recovered.

alice> EMPTYTRASH -all
[Client] Trash emptied.
```

---

## Building and Testing

### Build Commands
```bash
make              # Build all binaries
make clean        # Remove binaries and object files
```

### Running Tests
```bash
./test.sh         # Automated test suite (requires WSL/bash)
```

The test suite covers:
- File creation and deletion
- Read/write operations with access control
- Concurrent operations and locking
- Checkpoints and undo
- Trash bin operations
- EXEC and STREAM functionality
- Error handling and edge cases
