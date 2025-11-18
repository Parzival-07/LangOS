[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/0ek2UV58)

# Distributed File Collaboration System — User Manual

This project implements a simple distributed file collaboration system with three components:
- Name Server (NM): registry, routing, user list, access request workflow, and exec helper.
- Storage Server (SS): stores files and metadata, enforces ACLs/locks, trash, checkpoints, undo, stream.
- Client CLI: interactive REPL with colorized logs and commands for all features.

Defaults: Name Server listens on 127.0.0.1:8000. Storage data root is `ss_data/`.

## Build and Run

1) Build everything
	- make

2) Start the Name Server in one terminal
	- ./name_server

3) Start one or more Storage Servers (each needs a client port), e.g.
	- ./storage_server 9001

4) Start a Client in a separate terminal and enter a username when prompted
	- ./client

Order matters: start NM → SS → client. Each Storage Server registers itself with the Name Server.

## File and Access Model (at a glance)

- Each file has: owner, readers, writers; writer implies reader.
- Metadata lives in `ss_data/<file>.meta` with fields: owner, created, updated, accessed, readers, writers.
- INFO shows size (bytes/words/chars), last_access, last_modified, owner/ACL.
- Sentence-level write locks: WRITE, CLEAR, REVERT, DELETE are blocked while any write lock is active (423).
- Undo snapshots are taken before content-changing operations, enabling UNDO to restore previous content.

## Core Commands (Client)

### Create / Delete / Clear

```
CREATE <filename>           # Owner-only initially; creates empty file and .meta
DELETE <filename>           # Owner or writer; moves file to owner’s Trash (recoverable)
CLEAR  <filename>           # Owner or writer; truncate content to empty (undoable)
```

Notes
- DELETE and CLEAR are blocked by active write locks (423).
- CLEAR takes an undo snapshot first; UNDO restores the pre-clear content. `updated:` is refreshed.

### Read / Write

```
READ  <filename>            # Print file content (requires read access)
WRITE <filename> <sentence> # Interactive replace/append at 0-based sentence index
```

Notes
- WRITE acquires a sentence lock, fetches content, shows words for that sentence, and lets you edit until you type `ETIRW`.
- On success, metadata `updated:` is refreshed; undo snapshot is stored.

### Listing and Info

```
VIEW [-a|-l|-al]            # List files; -a all, -l long (details)
INFO <filename>             # Show detailed metadata and computed stats
```

Notes
- VIEW/INFO values are refreshed from the Storage Server on demand.
- INFO shows owner, readers, writers, size/words/chars, last_access, last_modified.

### Access Control (direct)

```
ADDACCESS -R|-W <filename> <user>  # Grant read or write (writer implies reader)
REMACCESS        <filename> <user>  # Remove any access for the user
```

Rules
- Only the owner can modify ACLs. Granting -W automatically adds the user to readers.

### Access Requests (workflow)

```
REQUESTACCESS -R|-W <filename>     # Ask owner for access
LISTREQUESTS <filename>            # Owner: list pending requests
APPROVEREQUEST -R|-W <filename> <user>
DENYREQUEST        <filename> <user>
```

Notes
- Anyone can request on existing files; owner approves/denies. On approval, ACL updates accordingly.

### Checkpoints (snapshots)

The system supports persistent, tagged checkpoints of the entire file.

```
CHECKPOINT <filename> <tag>        # Owner or writer
LISTCHECKPOINTS <filename>         # Any reader
VIEWCHECKPOINT <filename> <tag>    # Any reader
REVERT <filename> <tag>            # Owner or writer; blocked by active locks
```

Rules and Errors
- Tags: 1–64 chars [A-Za-z0-9_-]. Existing tag → 409. Missing/unauthorized → 404/403. Active lock → 423.
- Reverting updates `updated:` in metadata. Tags are stored under `ss_data/.checkpoints/<file>/<tag>`.

### Trash Bin (recoverable deletes)

```
TRASH                          # List your trashed files (owner only)
RECOVER <filename> [newname]   # Restore from your trash; optionally rename
EMPTYTRASH [-all | <filename>] # Permanently remove all or one item from trash
```

Notes
- DELETE moves content and .meta to `ss_data/.trash/<owner>/`. VIEW/INFO ignore `.trash`.
- RECOVER fails if the target name exists; pass `newname` to restore under a different name.

### Streaming and Exec

```
STREAM <filename>             # Live stream raw file bytes; server first ACKs with total size
EXEC   <filename>             # Run file content via /bin/sh on the Name Server; returns output
```

Notes
- STREAM prints content as it streams; when the announced number of bytes are received, the prompt continues on a new line.
- EXEC requires read access; use with caution—runs on the Name Server host.

### Users

```
LIST                          # Show currently registered users
```

### Undo

```
UNDO <filename>               # Restore last snapshot (from WRITE, CLEAR, REVERT, etc.)
```

Notes
- UNDO updates `updated:` in metadata while preserving `accessed`, readers, and writers.

## Error Codes (representative)

| Scenario                                  | Code | Message (typical)                |
|-------------------------------------------|------|----------------------------------|
| Unauthorized (no required ACL)            | 403  | Read/Write access denied         |
| Missing file/checkpoint                   | 404  | File not found / Checkpoint missing |
| Name conflict / Already exists            | 409  | Tag exists / File exists         |
| Operation blocked by active write locks   | 423  | Active write lock                |

## Implementation Notes

- Storage Root: `ss_data/` (override by defining STORAGE_ROOT at compile time if desired).
- Metadata policy: `last_access` and `last_modified` are computed using meta fields; `updated:` changes on content edits, UNDO, and REVERT; `accessed:` is updated on reads/streams.
- Writer implies reader: granting write automatically grants read; INFO and VIEW reflect this.
- Sentence locking: WRITE operates on 0-based sentence indices parsed by [.!?] separators; whitespace after punctuation belongs to the sentence.
- STREAM protocol: client sends a `CMD_STREAM` request; SS responds with `CMD_ACK` whose `payload_size` equals the total byte length of the file, then streams exactly that many bytes. This avoids any sentinel-collision issues.

## Examples

```
alice> CREATE report.txt
alice> WRITE report.txt 0
[Client] Current sentence words (1-based):
...
alice> CHECKPOINT report.txt draft1
alice> VIEW -l
...
alice> CLEAR report.txt
alice> UNDO report.txt
alice> DELETE report.txt
alice> TRASH
report.txt
alice> RECOVER report.txt
```

---

## Checkpoint Feature

The system now supports persistent, tagged checkpoints for file contents. A checkpoint is an immutable snapshot of the full file content captured at a point in time. You can create multiple checkpoints for a file, list them, inspect their content, and revert the file back to any named checkpoint.

### Commands (Client CLI)

```
CHECKPOINT <filename> <tag>        # Create a checkpoint (owner or writer).
LISTCHECKPOINTS <filename>         # List all checkpoint tags (any user with read access).
VIEWCHECKPOINT <filename> <tag>    # Show the content stored in a specific checkpoint (read access).
REVERT <filename> <tag>            # Replace current file content with that checkpoint (owner or writer, no active write locks).
```

### Notes & Rules

* Tags must be 1–64 chars using only: letters, digits, '-' or '_'.
* Creating or reverting is blocked if any sentence write lock is active on that file.
* Reverting updates the file's `updated:` timestamp in metadata.
* Listing returns newline-separated tags; viewing returns raw file bytes.
* Attempting to create a checkpoint with an existing tag returns an error (409).

### Error Cases (Representative)

| Scenario | Error Code | Message |
|----------|------------|---------|
| Tag already exists | 409 | Tag exists |
| Unauthorized action | 403 | Write access denied / Unauthorized |
| File or checkpoint missing | 404 | File not found / Checkpoint not found |
| Active sentence locks block create/revert | 423 | Active write lock |

### Internals

Checkpoints are stored under: `ss_data/.checkpoints/<filename>/<tag>` on the Storage Server.

### Examples

```
alice> CHECKPOINT report.txt draft1
alice> LISTCHECKPOINTS report.txt
draft1
alice> VIEWCHECKPOINT report.txt draft1
<contents printed>
alice> REVERT report.txt draft1
```

## Access Requests

Users can request access to files they don’t own. Owners can list and approve or deny requests.

### Commands (Client CLI)

```
REQUESTACCESS -R|-W <filename>         # Request read (-R) or write (-W) access for yourself.
LISTREQUESTS <filename>                # List pending requests for a file you own.
APPROVEREQUEST -R|-W <filename> <user> # Approve a user's request (choose access level to grant).
DENYREQUEST <filename> <user>          # Deny and remove a user's request.
```

### Notes & Rules

* Anyone may submit a request on an existing file; it fails if you already have that access or a pending request exists.
* Only the owner can list or respond to requests.
* On approval, the user's name is added to readers (for -R) or writers (for -W). On denial, nothing changes.
* Pending requests are removed after approval or denial.
* Storage layout: requests live under `ss_data/.requests/<filename>/<requester>.req` with content `READ` or `WRITE`.

### Examples

```
alice> LISTREQUESTS report.txt
(no output when empty)
bob> REQUESTACCESS -R report.txt
alice> LISTREQUESTS report.txt
bob	READ
alice> APPROVEREQUEST -R report.txt bob
bob> READ report.txt   # now allowed
```

## Trash Bin (Recoverable Deletions)

Deleted files are now moved to a per-owner Trash on the Storage Server instead of being permanently removed. You can list trashed files, recover them, or empty the trash.

### Commands (Client CLI)

```
TRASH                          # List your trashed files across all Storage Servers
RECOVER <filename> [newname]   # Recover a file from your trash; optionally rename on restore
EMPTYTRASH [-all | <filename>] # Permanently remove all trashed files, or a specific file
```

### Notes & Rules

* Only the owner can view or manage their own trash; the Name Server enforces this.
* On DELETE, the Storage Server moves both the content and its .meta into `ss_data/.trash/<owner>/`.
* RECOVER fails if a file of the same name already exists; supply `newname` to restore under a different name.
* VIEW/INFO listings exclude `.trash` entirely.
* EMPTYTRASH -all broadcasts to all Storage Servers and removes all your trashed items.

### Examples

```
alice> DELETE report.txt          # moves to trash
alice> TRASH
report.txt
alice> RECOVER report.txt         # restores as report.txt (if free)
alice> RECOVER notes.txt new.txt  # restore under a new name
alice> EMPTYTRASH -all            # purge any remaining trashed items
```
