#include "protocol.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <errno.h>
#include <dirent.h>
#include <ctype.h>

// --- Helpers for parsing and updating access lists ---
static int list_contains(const char* list, const char* user) {
    if (!user || !*user) return 0;
    char tmp[1024];
    strncpy(tmp, list ? list : "", sizeof(tmp)-1);
    tmp[sizeof(tmp)-1] = 0;
    for (char* p = tmp; *p; ++p) { if (*p==','||*p=='\n'||*p=='\r'||*p=='\t') *p=' '; }
    char* ctx = NULL; char* tok = strtok_r(tmp, " ", &ctx);
    while (tok) { if (strcmp(tok, user) == 0) return 1; tok = strtok_r(NULL, " ", &ctx); }
    return 0;
}

static void list_append(char* list, size_t cap, const char* user) {
    if (!user || !*user) return;
    size_t len = strlen(list);
    if (len == 0) {
        strncat(list, user, cap - strlen(list) - 1);
    } else {
        strncat(list, ",", cap - strlen(list) - 1);
        strncat(list, user, cap - strlen(list) - 1);
    }
}

static int list_remove(char* list, const char* user) {
    char tmp[1024]; strncpy(tmp, list ? list : "", sizeof(tmp)-1); tmp[sizeof(tmp)-1] = 0;
    char out[1024] = "";
    char* ctx = NULL; char* tok = strtok_r(tmp, ", \t\r\n", &ctx);
    int first = 1;
    int removed = 0;
    while (tok) {
        if (strcmp(tok, user) != 0) {
            if (!first) strncat(out, ",", sizeof(out)-1);
            strncat(out, tok, sizeof(out)-1);
            first = 0;
        } else {
            removed++;
        }
        tok = strtok_r(NULL, ", \t\r\n", &ctx);
    }
    strncpy(list, out, 1023);
    list[1023] = 0;
    return removed;
}

static void trim_both(char* s) {
    if (!s) return;
    // left trim
    char* p = s;
    while (*p && isspace((unsigned char)*p)) p++;
    if (p != s) {
        size_t len = strlen(p);
        memmove(s, p, len + 1);
    }
    // right trim
    size_t n = strlen(s);
    while (n > 0 && isspace((unsigned char)s[n-1])) { s[n-1] = 0; n--; }
}

int client_listen_port;
int nm_socket;

#ifndef STORAGE_ROOT
#define STORAGE_ROOT "ss_data"
#endif

// --- WRITE Sentence Lock State ---
// Simple lock table for (filename, sentence_index) protected by a mutex.
// Start simple: immediate fail if already locked; no waiting/queuing.

#define MAX_SENTENCE_LOCKS 2048

typedef struct {
    int in_use;
    char filename[MAX_FILENAME_LEN];
    int sentence_index; // 0-based
    int owner_fd;       // owning client socket
} SentenceLock;

static SentenceLock g_sentence_locks[MAX_SENTENCE_LOCKS];
static pthread_mutex_t g_sentence_locks_mutex = PTHREAD_MUTEX_INITIALIZER;

static int sentence_lock_find(const char* filename, int idx) {
    for (int i = 0; i < MAX_SENTENCE_LOCKS; i++) {
        if (g_sentence_locks[i].in_use &&
            g_sentence_locks[i].sentence_index == idx &&
            strncmp(g_sentence_locks[i].filename, filename, MAX_FILENAME_LEN) == 0) {
            return i;
        }
    }
    return -1;
}

static int sentence_lock_acquire_nowait_owner(const char* filename, int idx, int owner_fd) {
    if (!filename || idx < 0) return -1;
    pthread_mutex_lock(&g_sentence_locks_mutex);
    // Already locked?
    if (sentence_lock_find(filename, idx) >= 0) {
        pthread_mutex_unlock(&g_sentence_locks_mutex);
        return -1;
    }
    // Find free slot
    for (int i = 0; i < MAX_SENTENCE_LOCKS; i++) {
        if (!g_sentence_locks[i].in_use) {
            g_sentence_locks[i].in_use = 1;
            strncpy(g_sentence_locks[i].filename, filename, MAX_FILENAME_LEN - 1);
            g_sentence_locks[i].filename[MAX_FILENAME_LEN - 1] = '\0';
            g_sentence_locks[i].sentence_index = idx;
            g_sentence_locks[i].owner_fd = owner_fd;
            pthread_mutex_unlock(&g_sentence_locks_mutex);
            return 0;
        }
    }
    pthread_mutex_unlock(&g_sentence_locks_mutex);
    return -1; // table full
}

static int sentence_lock_owned_by(const char* filename, int idx, int owner_fd) {
    int pos = sentence_lock_find(filename, idx);
    if (pos < 0) return 0;
    return g_sentence_locks[pos].owner_fd == owner_fd;
}

static void sentence_lock_release(const char* filename, int idx) {
    pthread_mutex_lock(&g_sentence_locks_mutex);
    int pos = sentence_lock_find(filename, idx);
    if (pos >= 0) {
        g_sentence_locks[pos].in_use = 0;
        g_sentence_locks[pos].filename[0] = '\0';
        g_sentence_locks[pos].sentence_index = 0;
        g_sentence_locks[pos].owner_fd = -1;
    }
    pthread_mutex_unlock(&g_sentence_locks_mutex);
}

static void sentence_lock_release_all_for_owner(int owner_fd) {
    pthread_mutex_lock(&g_sentence_locks_mutex);
    for (int i = 0; i < MAX_SENTENCE_LOCKS; i++) {
        if (g_sentence_locks[i].in_use && g_sentence_locks[i].owner_fd == owner_fd) {
            g_sentence_locks[i].in_use = 0;
            g_sentence_locks[i].filename[0] = '\0';
            g_sentence_locks[i].sentence_index = 0;
            g_sentence_locks[i].owner_fd = -1;
        }
    }
    pthread_mutex_unlock(&g_sentence_locks_mutex);
}

static int ensure_dir(const char* path) {
    struct stat st;
    if (stat(path, &st) == 0) {
        return S_ISDIR(st.st_mode) ? 0 : -1;
    }
    #ifdef _WIN32
    int rc = _mkdir(path);
    #else
    int rc = mkdir(path, 0755);
    #endif
    return (rc == 0) ? 0 : -1;
}

static int is_valid_filename(const char* s) {
    if (!s || !*s) return 0;
    for (const char* p = s; *p; ++p) {
        char c = *p;
        if (!( (c>='a'&&c<='z') || (c>='A'&&c<='Z') || (c>='0'&&c<='9') || c=='_' || c=='-' || c=='.')) {
            return 0;
        }
    }
    return 1;
}

static int file_exists(const char* path) {
    struct stat st;
    return stat(path, &st) == 0;
}

static int ss_create_file_do(const MsgCreateFile* m, char* errbuf, size_t errlen) {
    if (ensure_dir(STORAGE_ROOT) != 0) {
        snprintf(errbuf, errlen, "Failed to create storage dir");
        return -1;
    }
    if (!is_valid_filename(m->filename)) {
        snprintf(errbuf, errlen, "Invalid filename");
        return -1;
    }
    char txt[512], meta[512];

    // Store content exactly as provided (no extra .txt)
    snprintf(txt, sizeof(txt),  STORAGE_ROOT "/%s",       m->filename);
    snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", m->filename);

    if (file_exists(txt) || file_exists(meta)) {
        snprintf(errbuf, errlen, "File already exists");
        return -1;
    }

    FILE* f = fopen(txt, "w");
    if (!f) {
        snprintf(errbuf, errlen, "Cannot create content file");
        return -1;
    }
    fclose(f);

    FILE* mf = fopen(meta, "w");
    if (!mf) {
        snprintf(errbuf, errlen, "Cannot create metadata file");
        return -1;
    }
    time_t now = time(NULL);
    fprintf(mf, "owner:%s\n", m->owner);
    fprintf(mf, "created:%lld\n", (long long)now);
    fprintf(mf, "updated:%lld\n", (long long)now);
    // Write empty lists with no trailing space after colon for stable formatting
    fprintf(mf, "readers:\n");
    fprintf(mf, "writers:\n");
    fclose(mf);

    return 0;
}

// Delete the content file and its metadata. Returns 0 on success, -1 on failure.
static int ss_delete_file_do(const char* filename) {
    if (!is_valid_filename(filename)) {
        return -1;
    }
    if (ensure_dir(STORAGE_ROOT) != 0) {
        return -1;
    }
    char txt[512], meta[512];
    snprintf(txt, sizeof(txt),  STORAGE_ROOT "/%s",       filename);
    snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", filename);

    // Attempt to remove both files; both must succeed per spec
    if (remove(txt) != 0) {
        return -1;
    }
    if (remove(meta) != 0) {
        return -1;
    }
    return 0;
}

// Replace the sentence at index 'sidx' with 'repl' text in STORAGE_ROOT/filename.
// Returns 0 on success, -1 on error with errbuf filled.
static int parse_is_delim(char c) {
    return (c == '.' || c == '!' || c == '?');
}

static int ss_write_replace_sentence_impl(const char* filepath, int sidx, const char* repl, char* errbuf, size_t errlen) {
    // Read entire file
    FILE* f = fopen(filepath, "rb");
    if (!f) { snprintf(errbuf, errlen, "Open failed"); return -1; }
    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); snprintf(errbuf, errlen, "Seek failed"); return -1; }
    long sz = ftell(f);
    if (sz < 0) { fclose(f); snprintf(errbuf, errlen, "Tell failed"); return -1; }
    rewind(f);
    char* data = (char*)malloc((size_t)sz + 1);
    if (!data) { fclose(f); snprintf(errbuf, errlen, "OOM"); return -1; }
    size_t rd = fread(data, 1, (size_t)sz, f);
    fclose(f);
    data[rd] = '\0';

    // Create tmp path
    char tmppath[600];
    snprintf(tmppath, sizeof(tmppath), "%s.tmp", filepath);
    FILE* tf = fopen(tmppath, "wb");
    if (!tf) { free(data); snprintf(errbuf, errlen, "Tmp open failed"); return -1; }

    // Walk sentences
    size_t i = 0, n = rd; int idx = 0; int replaced = 0;
    while (i < n) {
        size_t start = i;
        // advance until delimiter or EOF
        while (i < n && !parse_is_delim(data[i])) i++;
        if (i < n && parse_is_delim(data[i])) {
            i++; // include delimiter
            // include trailing whitespace
            while (i < n && (data[i] == ' ' || data[i] == '\n' || data[i] == '\t' || data[i] == '\r')) i++;
        }
        size_t end = i; // [start,end)
        if (idx == sidx) {
            size_t rlen = strnlen(repl, 2048);
            if (rlen > 0) fwrite(repl, 1, rlen, tf);
            replaced = 1;
        } else {
            if (end > start) fwrite(data + start, 1, end - start, tf);
        }
        idx++;
    }

    // Handle empty file case or missing last sentence (no delim)
    if (idx == 0) {
        // empty file — index must be 0 to replace
        if (sidx == 0) {
            size_t rlen = strnlen(repl, 2048);
            if (rlen > 0) fwrite(repl, 1, rlen, tf);
            replaced = 1;
            idx = 1;
        }
    }

    fclose(tf);

    // If target is exactly the next sentence (append), write it now
    if (!replaced && sidx == idx) {
        FILE* tf2 = fopen(tmppath, "ab");
        if (!tf2) {
            remove(tmppath);
            snprintf(errbuf, errlen, "Tmp reopen failed");
            free(data);
            return -1;
        }
        // Auto-insert a single space between sentences if the existing content
        // does not end with whitespace (so "One." + "Two." becomes "One. Two.")
        if (n > 0) {
            char last = data[n - 1];
            if (!(last == ' ' || last == '\n' || last == '\t' || last == '\r')) {
                fputc(' ', tf2);
            }
        }
        size_t rlen = strnlen(repl, 2048);
        if (rlen > 0) fwrite(repl, 1, rlen, tf2);
        fclose(tf2);
        replaced = 1;
        idx++;
    }

    free(data);

    if (!replaced || sidx < 0 || sidx > idx) {
        // cleanup tmp
        remove(tmppath);
        snprintf(errbuf, errlen, "Sentence index out of range");
        return -1;
    }

    // Atomically replace
    if (rename(tmppath, filepath) != 0) {
        remove(tmppath);
        snprintf(errbuf, errlen, "Rename failed");
        return -1;
    }
    return 0;
}

int ss_write_replace_sentence(const char* filename, int sidx, const char* repl, char* errbuf, size_t errlen) {
    if (!is_valid_filename(filename)) { snprintf(errbuf, errlen, "Invalid filename"); return -1; }
    if (ensure_dir(STORAGE_ROOT) != 0) { snprintf(errbuf, errlen, "Storage dir error"); return -1; }
    char path[512]; snprintf(path, sizeof(path), STORAGE_ROOT "/%s", filename);
    struct stat st;
    if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) {
        snprintf(errbuf, errlen, "Not found");
        return -1;
    }
    int rc = ss_write_replace_sentence_impl(path, sidx, repl, errbuf, errlen);
    if (rc == 0) {
        // touch metadata 'updated'
        char meta[512]; snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", filename);
        FILE* mf = fopen(meta, "a");
        if (mf) { time_t now = time(NULL); fprintf(mf, "updated:%lld\n", (long long)now); fclose(mf); }
    }
    return rc;
}

// Insert a new sentence at index 'insert_idx' (0-based) with 'repl' text
// shifting existing sentences at and after that index to the right.
static int ss_write_insert_sentence_impl(const char* filepath, int insert_idx, const char* repl, char* errbuf, size_t errlen) {
    if (insert_idx < 0) { snprintf(errbuf, errlen, "Bad index"); return -1; }
    FILE* f = fopen(filepath, "rb");
    if (!f) { snprintf(errbuf, errlen, "Open failed"); return -1; }
    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); snprintf(errbuf, errlen, "Seek failed"); return -1; }
    long sz = ftell(f);
    if (sz < 0) { fclose(f); snprintf(errbuf, errlen, "Tell failed"); return -1; }
    rewind(f);
    char* data = (char*)malloc((size_t)sz + 1);
    if (!data) { fclose(f); snprintf(errbuf, errlen, "OOM"); return -1; }
    size_t rd = fread(data, 1, (size_t)sz, f);
    fclose(f);
    data[rd] = '\0';

    char tmppath[600]; snprintf(tmppath, sizeof(tmppath), "%s.tmp", filepath);
    FILE* tf = fopen(tmppath, "wb");
    if (!tf) { free(data); snprintf(errbuf, errlen, "Tmp open failed"); return -1; }

    size_t i = 0, n = rd; int idx = 0; int inserted = 0;
    while (i < n) {
        size_t start = i;
        while (i < n && !parse_is_delim(data[i])) i++;
        if (i < n && parse_is_delim(data[i])) {
            i++;
            while (i < n && (data[i] == ' ' || data[i] == '\n' || data[i] == '\t' || data[i] == '\r')) i++;
        }
        size_t end = i;

        if (!inserted && idx == insert_idx - 1) {
            // write previous sentence without its trailing whitespace
            size_t ws_start = end;
            while (ws_start > start && (data[ws_start-1] == ' ' || data[ws_start-1] == '\n' || data[ws_start-1] == '\t' || data[ws_start-1] == '\r')) ws_start--;
            if (ws_start > start) fwrite(data + start, 1, ws_start - start, tf);
            // one space between previous and inserted
            fputc(' ', tf);
            // inserted sentence
            size_t rlen = strnlen(repl, 2048); if (rlen > 0) fwrite(repl, 1, rlen, tf);
            // original whitespace (between prev and next) now separates inserted and next
            if (end > ws_start) fwrite(data + ws_start, 1, end - ws_start, tf);
            inserted = 1;
        } else {
            // normal write-through
            if (end > start) fwrite(data + start, 1, end - start, tf);
        }
        idx++;
    }

    if (!inserted) {
        // If inserting at 0 into empty file or at end, just append/precede
        if (insert_idx == 0) {
            size_t rlen = strnlen(repl, 2048); if (rlen > 0) fwrite(repl, 1, rlen, tf);
            if (n > 0) fputc(' ', tf);
            if (n > 0) fwrite(data, 1, n, tf);
            inserted = 1;
        } else {
            // Or append to end if index equals sentence count (handled here too)
            // Count sentences to see if insert_idx equals idx
            // idx currently equals sentence count
            if (insert_idx == idx) {
                // ensure separation
                if (n > 0) {
                    char last = data[n-1];
                    if (!(last == ' ' || last == '\n' || last == '\t' || last == '\r')) fputc(' ', tf);
                }
                size_t rlen = strnlen(repl, 2048); if (rlen > 0) fwrite(repl, 1, rlen, tf);
                inserted = 1;
            }
        }
    }

    free(data);
    fclose(tf);

    if (!inserted) { remove(tmppath); snprintf(errbuf, errlen, "Insert index out of range"); return -1; }
    if (rename(tmppath, filepath) != 0) { remove(tmppath); snprintf(errbuf, errlen, "Rename failed"); return -1; }
    return 0;
}

static int ss_write_insert_sentence(const char* filename, int insert_idx, const char* repl, char* errbuf, size_t errlen) {
    if (!is_valid_filename(filename)) { snprintf(errbuf, errlen, "Invalid filename"); return -1; }
    if (ensure_dir(STORAGE_ROOT) != 0) { snprintf(errbuf, errlen, "Storage dir error"); return -1; }
    char path[512]; snprintf(path, sizeof(path), STORAGE_ROOT "/%s", filename);
    struct stat st; if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) { snprintf(errbuf, errlen, "Not found"); return -1; }
    int rc = ss_write_insert_sentence_impl(path, insert_idx, repl, errbuf, errlen);
    if (rc == 0) {
        char meta[512]; snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", filename);
        FILE* mf = fopen(meta, "a"); if (mf) { time_t now = time(NULL); fprintf(mf, "updated:%lld\n", (long long)now); fclose(mf); }
    }
    return rc;
}

// --- Lock helpers for index shifts / owner lookup ---
static int sentence_lock_find_for_owner(const char* filename, int owner_fd) {
    int res = -1;
    pthread_mutex_lock(&g_sentence_locks_mutex);
    for (int i = 0; i < MAX_SENTENCE_LOCKS; i++) {
        if (g_sentence_locks[i].in_use && g_sentence_locks[i].owner_fd == owner_fd && strncmp(g_sentence_locks[i].filename, filename, MAX_FILENAME_LEN) == 0) {
            res = i; break;
        }
    }
    pthread_mutex_unlock(&g_sentence_locks_mutex);
    return res;
}

static void sentence_lock_shift_from(const char* filename, int from_index, int delta) {
    pthread_mutex_lock(&g_sentence_locks_mutex);
    for (int i = 0; i < MAX_SENTENCE_LOCKS; i++) {
        if (g_sentence_locks[i].in_use && strncmp(g_sentence_locks[i].filename, filename, MAX_FILENAME_LEN) == 0 && g_sentence_locks[i].sentence_index >= from_index) {
            g_sentence_locks[i].sentence_index += delta;
        }
    }
    pthread_mutex_unlock(&g_sentence_locks_mutex);
}

/**
 * @brief Thread to listen for direct Client connections (for READ, WRITE, etc.)
 */
typedef struct {
    int client_socket;
} ClientSessionArgs;

static void send_error_to(int sock, int code, const char* msg) {
    MsgHeader h = {0};
    MsgError e = {0};
    h.command = CMD_ERROR;
    h.payload_size = sizeof(MsgError);
    e.code = code;
    strncpy(e.message, msg ? msg : "error", sizeof(e.message)-1);
    send_all(sock, &h, sizeof(h));
    send_all(sock, &e, sizeof(e));
}

static void* handle_one_client(void* arg) {
    ClientSessionArgs* a = (ClientSessionArgs*)arg;
    int client_socket = a->client_socket;
    free(a);

    // Try to detect whether the client is using header-based protocol (robustly)
    MsgHeader peek;
    ssize_t p = recv(client_socket, &peek, sizeof(peek), MSG_PEEK);
    bool use_header = false;
    if (p >= (ssize_t)sizeof(peek)) {
        // Validate that the peeked bytes look like a real header
        int cmd = (int)peek.command;
        int ps  = (int)peek.payload_size;
        if ((cmd == CMD_READ_FILE      && ps == (int)sizeof(MsgReadFile)) ||
            (cmd == CMD_WRITE_FILE     && ps == (int)sizeof(MsgWriteFile)) ||
            (cmd == CMD_WRITE_BEGIN    && ps == (int)sizeof(MsgWriteBegin)) ||
            (cmd == CMD_WRITE_DONE     && ps == (int)sizeof(MsgWriteDone))) {
            use_header = true;
        }
    }
    if (use_header) {
        // Header-based loop
        while (true) {
            MsgHeader h;
            if (!recv_all(client_socket, &h, sizeof(h))) {
                break; // disconnected
            }

            switch (h.command) {
                case CMD_READ_FILE: {
                    if (h.payload_size != (int)sizeof(MsgReadFile)) {
                        send_error_to(client_socket, 400, "Bad payload size");
                        // Drain any unexpected payload
                        size_t rem = h.payload_size; char drain[512];
                        while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(client_socket, drain, chunk)) break; rem -= chunk; }
                        continue;
                    }
                    MsgReadFile req;
                    if (!recv_all(client_socket, &req, sizeof(req))) {
                        break;
                    }
                    if (!is_valid_filename(req.filename)) {
                        send_error_to(client_socket, 400, "Invalid filename");
                        continue;
                    }
                    char path[512];
                    snprintf(path, sizeof(path), STORAGE_ROOT "/%s", req.filename);
                    struct stat st;
                    if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) {
                        send_error_to(client_socket, 404, "Not found");
                        continue;
                    }
                    FILE* f = fopen(path, "rb");
                    if (!f) { send_error_to(client_socket, 500, "Open failed"); continue; }
                    // Announce payload size as file size
                    MsgHeader rh = { .command = CMD_ACK, .payload_size = (int)st.st_size };
                    if (!send_all(client_socket, &rh, sizeof(rh))) { fclose(f); break; }
                    // Stream exact st_size bytes
                    char buf[4096]; size_t remaining = (size_t)st.st_size;
                    while (remaining > 0) {
                        size_t to_read = remaining > sizeof(buf) ? sizeof(buf) : remaining;
                        size_t nr = fread(buf, 1, to_read, f);
                        if (nr == 0) break;
                        if (!send_all(client_socket, buf, nr)) { remaining = 0; break; }
                        remaining -= nr;
                    }
                    fclose(f);
                    continue;
                }
                case CMD_WRITE_FILE: {
                    if (h.payload_size != (int)sizeof(MsgWriteFile)) {
                        send_error_to(client_socket, 400, "Bad payload size");
                        // Drain
                        size_t rem = h.payload_size; char drain[512];
                        while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(client_socket, drain, chunk)) break; rem -= chunk; }
                        continue;
                    }
                    MsgWriteFile req;
                    if (!recv_all(client_socket, &req, sizeof(req))) {
                        break;
                    }
                    if (!is_valid_filename(req.filename) || req.sentence_index < 0) {
                        send_error_to(client_socket, 400, "Invalid arguments");
                        continue;
                    }
                    // Determine operation type: replacement of owned index OR insertion after owned index.
                    char err[256] = {0};
                    int target = req.sentence_index;
                    int handled = 0;

                    // Case 1: client owns the target index -> replacement
                    if (sentence_lock_owned_by(req.filename, target, client_socket)) {
                        int wrc = ss_write_replace_sentence(req.filename, target, req.replacement, err, sizeof(err));
                        if (wrc == 0) {
                            MsgHeader rh = { .command = CMD_ACK, .payload_size = 0 };
                            send_all(client_socket, &rh, sizeof(rh));
                        } else {
                            send_error_to(client_socket, 500, err[0] ? err : "WRITE failed");
                        }
                        handled = 1;
                    }

                    if (handled) { continue; }

                    // Case 2: client owns the previous index -> treat as insertion at 'target'
                    if (target > 0 && sentence_lock_owned_by(req.filename, target - 1, client_socket)) {
                        int wrc = ss_write_insert_sentence(req.filename, target, req.replacement, err, sizeof(err));
                        if (wrc == 0) {
                            // shift all locks at and after target by +1 (indices changed)
                            sentence_lock_shift_from(req.filename, target, +1);
                            MsgHeader rh = { .command = CMD_ACK, .payload_size = 0 };
                            send_all(client_socket, &rh, sizeof(rh));
                        } else {
                            send_error_to(client_socket, 500, err[0] ? err : "INSERT failed");
                        }
                        continue;
                    }

                    // Case 3: tolerate index drift after insertion by locating lock for this owner
                    int slot = sentence_lock_find_for_owner(req.filename, client_socket);
                    if (slot >= 0) {
                        int owned_idx = -1;
                        pthread_mutex_lock(&g_sentence_locks_mutex);
                        owned_idx = g_sentence_locks[slot].sentence_index;
                        pthread_mutex_unlock(&g_sentence_locks_mutex);

                        if (target == owned_idx) {
                            int wrc = ss_write_replace_sentence(req.filename, owned_idx, req.replacement, err, sizeof(err));
                            if (wrc == 0) { MsgHeader rh = (MsgHeader){ .command = CMD_ACK, .payload_size = 0 }; send_all(client_socket, &rh, sizeof(rh)); }
                            else { send_error_to(client_socket, 500, err[0]?err:"WRITE failed"); }
                            continue;
                        }
                        if (target == owned_idx + 1) {
                            int wrc = ss_write_insert_sentence(req.filename, target, req.replacement, err, sizeof(err));
                            if (wrc == 0) { sentence_lock_shift_from(req.filename, target, +1); MsgHeader rh = (MsgHeader){ .command = CMD_ACK, .payload_size = 0 }; send_all(client_socket, &rh, sizeof(rh)); }
                            else { send_error_to(client_socket, 500, err[0]?err:"INSERT failed"); }
                            continue;
                        }
                        // Enforce bound per requirement: only current (1) or next (2) relative to lock
                        send_error_to(client_socket, 400, "Sentence index out of bounds for this lock");
                        continue;
                    }

                    // Otherwise, follow legacy: try to acquire lock for target if free
                    int locked = (sentence_lock_find(req.filename, target) >= 0);
                    if (!locked) {
                        if (sentence_lock_acquire_nowait_owner(req.filename, target, client_socket) != 0) { send_error_to(client_socket, 423, "Sentence is locked"); continue; }
                        int wrc = ss_write_replace_sentence(req.filename, target, req.replacement, err, sizeof(err));
                        if (wrc == 0) { MsgHeader rh = { .command = CMD_ACK, .payload_size = 0 }; send_all(client_socket, &rh, sizeof(rh)); }
                        else { sentence_lock_release(req.filename, target); send_error_to(client_socket, 500, err[0]?err:"WRITE failed"); }
                        continue;
                    }
                    send_error_to(client_socket, 423, "Sentence is locked by another client");
                    continue;
                }
                case CMD_WRITE_BEGIN: {
                    if (h.payload_size != (int)sizeof(MsgWriteBegin)) {
                        send_error_to(client_socket, 400, "Bad payload size");
                        // Drain
                        size_t rem = h.payload_size; char drain[512];
                        while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(client_socket, drain, chunk)) break; rem -= chunk; }
                        continue;
                    }
                    MsgWriteBegin req;
                    if (!recv_all(client_socket, &req, sizeof(req))) { break; }
                    if (!is_valid_filename(req.filename) || req.sentence_index < 0) {
                        send_error_to(client_socket, 400, "Invalid arguments");
                        continue;
                    }
                    if (sentence_lock_acquire_nowait_owner(req.filename, req.sentence_index, client_socket) != 0) {
                        send_error_to(client_socket, 423, "Sentence is locked");
                        continue;
                    }
                    MsgHeader rh = { .command = CMD_ACK, .payload_size = 0 };
                    send_all(client_socket, &rh, sizeof(rh));
                    continue;
                }
                case CMD_WRITE_DONE: {
                    if (h.payload_size != (int)sizeof(MsgWriteDone)) {
                        send_error_to(client_socket, 400, "Bad payload size");
                        // Drain
                        size_t rem = h.payload_size; char drain[512];
                        while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(client_socket, drain, chunk)) break; rem -= chunk; }
                        continue;
                    }
                    MsgWriteDone done;
                    if (!recv_all(client_socket, &done, sizeof(done))) {
                        break;
                    }
                    // Only owner can release, but indices may have shifted; try owner-based resolution
                    if (!sentence_lock_owned_by(done.filename, done.sentence_index, client_socket)) {
                        int slot = sentence_lock_find_for_owner(done.filename, client_socket);
                        if (slot < 0) { send_error_to(client_socket, 409, "Not lock owner"); continue; }
                        int idx_to_release;
                        pthread_mutex_lock(&g_sentence_locks_mutex);
                        idx_to_release = g_sentence_locks[slot].sentence_index;
                        pthread_mutex_unlock(&g_sentence_locks_mutex);
                        sentence_lock_release(done.filename, idx_to_release);
                    } else {
                        sentence_lock_release(done.filename, done.sentence_index);
                    }
                    MsgHeader rh = { .command = CMD_ACK, .payload_size = 0 };
                    send_all(client_socket, &rh, sizeof(rh));
                    continue;
                }
                default: {
                    // Drain unknown payload
                    size_t rem = h.payload_size; char drain[512];
                    while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(client_socket, drain, chunk)) break; rem -= chunk; }
                    send_error_to(client_socket, 400, "Unknown command");
                    continue;
                }
            }
        }
    } else {
        // Legacy simple protocol: read a filename line, send content, then close
        char fname[MAX_FILENAME_LEN+2];
        size_t fpos = 0;
        while (fpos < sizeof(fname)-1) {
            char ch;
            ssize_t r = recv(client_socket, &ch, 1, 0);
            if (r <= 0) break; // disconnect or error
            if (ch == '\n') break;
            fname[fpos++] = ch;
        }
        fname[fpos] = '\0';

        if (fpos == 0 || !is_valid_filename(fname)) {
            const char* msg = "ERROR: invalid filename\n";
            send_all(client_socket, msg, strlen(msg));
            close(client_socket);
            return NULL;
        }

        char path[512];
        snprintf(path, sizeof(path), STORAGE_ROOT "/%s", fname);
        FILE* f = fopen(path, "rb");
        if (!f) {
            const char* msg = "ERROR: not found\n";
            send_all(client_socket, msg, strlen(msg));
            close(client_socket);
            return NULL;
        }
        char buf[4096];
        size_t nread;
        while ((nread = fread(buf, 1, sizeof(buf), f)) > 0) {
            if (!send_all(client_socket, buf, nread)) {
                break;
            }
        }
        fclose(f);
    }

    close(client_socket);
    // Release any locks held by this client
    sentence_lock_release_all_for_owner(client_socket);
    return NULL;
}

void* handle_client_connections(void* arg) {
    int server_fd;
    struct sockaddr_in address;
    int opt = 1;

    // Create listening socket for Clients
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("[SS] Client socket creation failed");
        return NULL;
    }
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        perror("[SS] setsockopt");
        return NULL;
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(client_listen_port);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("[SS] Client socket bind failed");
        return NULL;
    }
    if (listen(server_fd, 5) < 0) {
        perror("[SS] Client socket listen failed");
        return NULL;
    }

    printf("[SS] Storage Server now listening for CLIENTS on port %d...\n",
           client_listen_port);

    // --- Accept Loop for Clients ---
    while (true) {
        int client_socket;
        if ((client_socket = accept(server_fd, NULL, NULL)) < 0) {
            perror("[SS] Client accept failed");
            continue;
        }
        printf("[SS] Received a direct client connection!\n");

        // Spawn a detached thread per client to handle header-based loop or legacy read
        pthread_t tid;
        ClientSessionArgs* a = (ClientSessionArgs*)malloc(sizeof(ClientSessionArgs));
        if (!a) { close(client_socket); continue; }
        a->client_socket = client_socket;
        if (pthread_create(&tid, NULL, handle_one_client, a) != 0) {
            perror("[SS] pthread_create (client session)");
            close(client_socket);
            free(a);
            continue;
        }
        pthread_detach(tid);
    }
    return NULL;
}

static void nm_send_error(int code, const char* msg) {
    MsgHeader h = {0};
    MsgError  e = {0};
    h.command = CMD_ERROR;
    h.payload_size = sizeof(MsgError);
    e.code = code;
    strncpy(e.message, msg ? msg : "error", sizeof(e.message)-1);
    send_all(nm_socket, &h, sizeof(h));
    send_all(nm_socket, &e, sizeof(e));
}

/**
 * @brief Thread to listen for commands from the Name Server
 */
void* listen_to_nm(void* arg) {
    MsgHeader header;
    
    // Loop forever, waiting for commands from the NM
    while (true) {
        if (!recv_all(nm_socket, &header, sizeof(MsgHeader))) {
            fprintf(stderr, "[SS] Connection to Name Server lost! Exiting.\n");
            exit(EXIT_FAILURE); // If NM is down, SS can't function
        }

        printf("[SS] Received command %d from Name Server.\n", header.command);
        
        // Handle commands from NM
    switch (header.command) {
            case CMD_CREATE_FILE: {
                if (header.payload_size != sizeof(MsgCreateFile)) {
                    nm_send_error(400, "Bad payload size");
                    break;
                }
                MsgCreateFile msg;
                if (!recv_all(nm_socket, &msg, sizeof(msg))) {
                    nm_send_error(400, "Payload read failed");
                    break;
                }
                char err[256] = {0};
                int rc = ss_create_file_do(&msg, err, sizeof(err));
                if (rc == 0) {
                    printf("[SS] Created file '%s' for owner '%s'\n", msg.filename, msg.owner);
                    MsgHeader ack = { .command = CMD_ACK, .payload_size = 0 };
                    send_all(nm_socket, &ack, sizeof(ack));
                } else {
                    printf("[SS] CREATE failed for '%s': %s\n", msg.filename, err);
                    nm_send_error(409, err[0] ? err : "CREATE failed");
                }
                break;
            }
            case CMD_SS_LIST_FILES: {
                // Drain any unexpected payload
                if (header.payload_size > 0) {
                    char drain[512];
                    size_t remaining = header.payload_size;
                    while (remaining > 0) {
                        size_t chunk = remaining > sizeof(drain) ? sizeof(drain) : remaining;
                        if (!recv_all(nm_socket, drain, chunk)) break;
                        remaining -= chunk;
                    }
                }

                MsgSSFileListResponse resp = {0};
                size_t pos = 0, cap = sizeof(resp.files);

                DIR* d = opendir(STORAGE_ROOT);
                if (d) {
                    struct dirent* ent;
                    while ((ent = readdir(d)) != NULL) {
                        const char* name = ent->d_name;
                        // Skip . and ..
                        if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) continue;
                        // Skip metadata files
                        size_t nlen = strlen(name);
                        if (nlen > 5 && strcmp(name + nlen - 5, ".meta") == 0) continue;

                        int n = snprintf(resp.files + pos, (pos < cap ? cap - pos : 0), "%s\n", name);
                        if (n <= 0) break;
                        if ((size_t)n >= (cap - pos)) { // truncated; stop
                            pos = cap - 1;
                            break;
                        }
                        pos += (size_t)n;
                    }
                    closedir(d);
                }

                MsgHeader h = {0};
                h.command = CMD_SS_LIST_FILES_RESP;
                h.payload_size = sizeof(resp);
                send_all(nm_socket, &h, sizeof(h));
                send_all(nm_socket, &resp, sizeof(resp));
                break;
            }
            case CMD_DELETE_FILE: {
                if (header.payload_size != sizeof(MsgDeleteFile)) {
                    nm_send_error(400, "Bad payload size");
                    break;
                }
                MsgDeleteFile msg;
                if (!recv_all(nm_socket, &msg, sizeof(msg))) {
                    nm_send_error(400, "Payload read failed");
                    break;
                }
                int rc = ss_delete_file_do(msg.filename);
                if (rc == 0) {
                    printf("[SS] Deleted file '%s'\n", msg.filename);
                    MsgHeader ack = { .command = CMD_ACK, .payload_size = 0 };
                    send_all(nm_socket, &ack, sizeof(ack));
                } else {
                    printf("[SS] DELETE failed for '%s'\n", msg.filename);
                    nm_send_error(404, "DELETE failed");
                }
                break;
            }
            case CMD_ADD_ACCESS:
            case CMD_REM_ACCESS: {
                if (header.payload_size != sizeof(MsgAccessChange)) {
                    nm_send_error(400, "Bad payload size");
                    break;
                }
                MsgAccessChange msg;
                if (!recv_all(nm_socket, &msg, sizeof(msg))) { nm_send_error(400, "Payload read failed"); break; }
                if (!is_valid_filename(msg.filename)) { nm_send_error(400, "Invalid filename"); break; }

                // Load metadata
                char meta[512]; snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", msg.filename);
                FILE* mf = fopen(meta, "r");
                if (!mf) { nm_send_error(404, "File not found"); break; }
                char owner[MAX_USERNAME_LEN] = {0};
                long long created=0, updated=0;
                char readers[1024] = {0}, writers[1024] = {0};
                char line[2048];
                while (fgets(line, sizeof(line), mf)) {
                    if (strncmp(line, "owner:", 6) == 0) { sscanf(line+6, "%63s", owner); }
                    else if (strncmp(line, "created:", 8) == 0) { sscanf(line+8, "%lld", &created); }
                    else if (strncmp(line, "updated:", 8) == 0) { sscanf(line+8, "%lld", &updated); }
                    else if (strncmp(line, "readers:", 8) == 0) { strncpy(readers, line+8, sizeof(readers)-1); }
                    else if (strncmp(line, "writers:", 8) == 0) { strncpy(writers, line+8, sizeof(writers)-1); }
                }
                fclose(mf);
                // Trim leading and trailing whitespace so formatting is stable
                trim_both(readers);
                trim_both(writers);

                // Owner check
                if (owner[0]==0 || strncmp(owner, msg.requester, MAX_USERNAME_LEN) != 0) {
                    nm_send_error(403, "Only owner may change access");
                    break;
                }

                if (header.command == CMD_ADD_ACCESS) {
                    if (msg.is_writer) {
                        if (list_contains(writers, msg.target)) { nm_send_error(409, "User already has writer access"); break; }
                        list_append(writers, sizeof(writers), msg.target);
                    } else {
                        if (list_contains(readers, msg.target)) { nm_send_error(409, "User already has reader access"); break; }
                        list_append(readers, sizeof(readers), msg.target);
                    }
                } else { // REM_ACCESS: remove from BOTH readers and writers (role-agnostic)
                    int removed_w = list_remove(writers, msg.target);
                    int removed_r = list_remove(readers, msg.target);
                    if (removed_w == 0 && removed_r == 0) {
                        nm_send_error(404, "User not found");
                        break;
                    }
                }

                // Save metadata back (only on success)
                time_t now = time(NULL); updated = (long long)now;
                mf = fopen(meta, "w");
                if (!mf) { nm_send_error(500, "Failed to write meta"); break; }
                fprintf(mf, "owner:%s\n", owner);
                fprintf(mf, "created:%lld\n", created);
                fprintf(mf, "updated:%lld\n", updated);
                fprintf(mf, "readers:%s%s\n", (readers[0]?" ":""), readers);
                fprintf(mf, "writers:%s%s\n", (writers[0]?" ":""), writers);
                fclose(mf);

                MsgHeader ack = { .command = CMD_ACK, .payload_size = 0 };
                send_all(nm_socket, &ack, sizeof(ack));
                break;
            }
            case CMD_INFO: {
                if (header.payload_size != sizeof(MsgInfoRequest)) { nm_send_error(400, "Bad payload size"); break; }
                MsgInfoRequest req; if (!recv_all(nm_socket, &req, sizeof(req))) { nm_send_error(400, "Payload read failed"); break; }
                if (!is_valid_filename(req.filename)) { nm_send_error(400, "Invalid filename"); break; }
                char meta[512]; snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", req.filename);
                FILE* mf = fopen(meta, "r"); if (!mf) { nm_send_error(404, "File not found"); break; }
                char owner[MAX_USERNAME_LEN] = {0};
                long long created=0, updated=0;
                char readers[1024] = {0}, writers[1024] = {0};
                char line[2048];
                while (fgets(line, sizeof(line), mf)) {
                    if (strncmp(line, "owner:", 6) == 0) { sscanf(line+6, "%63s", owner); }
                    else if (strncmp(line, "created:", 8) == 0) { sscanf(line+8, "%lld", &created); }
                    else if (strncmp(line, "updated:", 8) == 0) { sscanf(line+8, "%lld", &updated); }
                    else if (strncmp(line, "readers:", 8) == 0) { strncpy(readers, line+8, sizeof(readers)-1); }
                    else if (strncmp(line, "writers:", 8) == 0) { strncpy(writers, line+8, sizeof(writers)-1); }
                }
                fclose(mf);
                // Trim leading and trailing whitespace
                trim_both(readers);
                trim_both(writers);
                // Build info string
                MsgInfoResponse resp = {0};
                char created_buf[64]={0}, updated_buf[64]={0};
                time_t cr=(time_t)created, up=(time_t)updated;
                struct tm tmv;
                if (localtime_r(&cr, &tmv)) strftime(created_buf, sizeof(created_buf), "%Y-%m-%d %H:%M:%S", &tmv);
                if (localtime_r(&up, &tmv)) strftime(updated_buf, sizeof(updated_buf), "%Y-%m-%d %H:%M:%S", &tmv);
                snprintf(resp.info, sizeof(resp.info),
                         "filename: %s\nowner: %s\ncreated: %s (%lld)\nupdated: %s (%lld)\nreaders: %s\nwriters: %s\n",
                         req.filename,
                         owner[0]?owner:"",
                         created_buf[0]?created_buf:"", created,
                         updated_buf[0]?updated_buf:"", updated,
                         readers[0]?readers:"",
                         writers[0]?writers:"");
                MsgHeader h = { .command = CMD_INFO_RESP, .payload_size = sizeof(resp) };
                send_all(nm_socket, &h, sizeof(h));
                send_all(nm_socket, &resp, sizeof(resp));
                break;
            }
            default:
                // TODO: other commands in Phase 2
                break;
        }
    }
    return NULL;
}

int main(int argc, char const *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <client_listen_port>\n", argv[0]);
        return 1;
    }

    client_listen_port = atoi(argv[1]);
    if (client_listen_port <= 0 || client_listen_port > 65535) {
        fprintf(stderr, "Invalid port number.\n");
        return 1;
    }

    // --- Start Thread to Listen for Clients ---
    pthread_t client_thread_id;
    if (pthread_create(&client_thread_id, NULL, handle_client_connections, NULL) != 0) {
        perror("[SS] Failed to create client listener thread");
        return 1;
    }

    // --- Connect to Name Server ---
    struct sockaddr_in nm_address;
    if ((nm_socket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("[SS] Socket creation error");
        return 1;
    }

    nm_address.sin_family = AF_INET;
    nm_address.sin_port = htons(NAME_SERVER_PORT);

    // Convert IPv4 address from text to binary form
    if (inet_pton(AF_INET, "127.0.0.1", &nm_address.sin_addr) <= 0) {
        perror("[SS] Invalid address/ Address not supported");
        return 1;
    }

    printf("[SS] Connecting to Name Server at 127.0.0.1:%d...\n", NAME_SERVER_PORT);

    if (connect(nm_socket, (struct sockaddr *)&nm_address, sizeof(nm_address)) < 0) {
        perror("[SS] Connection Failed");
        return 1;
    }

    printf("[SS] Connected to Name Server!\n");

    // Ensure storage root exists
    if (ensure_dir(STORAGE_ROOT) != 0) {
        fprintf(stderr, "[SS] Failed to ensure storage root '%s'\n", STORAGE_ROOT);
        return 1;
    }

    // --- Register with Name Server ---
    MsgHeader header;
    MsgRegisterSS reg_msg;

    header.command = CMD_REGISTER_SS;
    header.payload_size = sizeof(MsgRegisterSS);
    reg_msg.client_listen_port = client_listen_port;

    printf("[SS] Registering with NM (Client Port: %d)...\n", client_listen_port);

    // Send header, then payload
    if (!send_all(nm_socket, &header, sizeof(MsgHeader))) return 1;
    if (!send_all(nm_socket, &reg_msg, sizeof(MsgRegisterSS))) return 1;


    // --- Wait for ACK ---
    if (!recv_all(nm_socket, &header, sizeof(MsgHeader))) {
        fprintf(stderr, "[SS] Failed to receive ACK from NM.\n");
        return 1;
    }

    if (header.command == CMD_ACK) {
        printf("[SS] Registration successful!\n");
    } else {
        fprintf(stderr, "[SS] Registration failed (Received command %d).\n", header.command);
        return 1;
    }
    
    // --- Start Thread to Listen for NM Commands ---
    pthread_t nm_thread_id;
    if(pthread_create(&nm_thread_id, NULL, listen_to_nm, NULL) != 0) {
        perror("[SS] Failed to create NM listener thread");
        return 1;
    }

    // Wait for threads to finish (which they won't)
    pthread_join(client_thread_id, NULL);
    pthread_join(nm_thread_id, NULL);
    
    close(nm_socket);
    return 0;
}
