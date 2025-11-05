#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <string.h> // for memset, strcpy, etc.
#include <unistd.h> // for read, write
#include <sys/socket.h> // for socket operations
#include <stdbool.h> // for bool, true, false
#include <stdio.h> // for perror

// --- CONFIGURATION ---

#define NAME_SERVER_PORT 8000
#define MAX_USERNAME_LEN 64
#define MAX_FILENAME_LEN 256
#define MAX_ERROR_MSG_LEN 256
#define MAX_IP_LEN 64

// --- COMMAND DEFINITIONS ---
// This is the "language" our servers speak.
typedef enum {
    // Registration commands
    CMD_REGISTER_SS = 100,
    CMD_REGISTER_CLIENT = 101,

    // Acknowledgement / Error
    CMD_ACK = 200,    // General success
    CMD_ERROR = 201,  // General failure

    // File operations
    CMD_CREATE_FILE = 300,
    CMD_DELETE_FILE = 301,

    // View/listing operations
    // Client <-> NM
    CMD_VIEW_FILES = 310,
    CMD_VIEW_FILES_RESP = 311,

    // NM <-> SS (internal listing used by NM to refresh registry)
    CMD_SS_LIST_FILES = 320,
    CMD_SS_LIST_FILES_RESP = 321,

    // READ routing and response
    // Client <-> NM
    CMD_READ_FILE = 330,
    CMD_READ_FILE_RESP = 331,
    
    // Access control operations
    CMD_ADD_ACCESS = 340,
    CMD_REM_ACCESS = 341,

    // File info query
    CMD_INFO = 350,
    CMD_INFO_RESP = 351,

    // Direct client -> SS write (session begin)
    CMD_WRITE_BEGIN = 360,
    // Direct client -> SS write (apply and finalize)
    CMD_WRITE_FILE = 361,
    CMD_WRITE_DONE = 362,

    // Undo last change (Client -> NM -> SS)
    CMD_UNDO = 370,

    // User listing
    CMD_LIST_USERS = 380,
    CMD_LIST_USERS_RESP = 381
} CommandCode;

// --- DATA STRUCTURES ---
// We use fixed-size structs for simple network communication.

// Header sent before EVERY message
typedef struct {
    CommandCode command;
    int payload_size; // Size of the struct *following* this header
} MsgHeader;

// Data for CMD_REGISTER_SS
// SS -> NM
typedef struct {
    int client_listen_port; // The port this SS will open for clients
} MsgRegisterSS;

// Data for CMD_REGISTER_CLIENT
// Client -> NM
typedef struct {
    char username[MAX_USERNAME_LEN];
} MsgRegisterClient;

// Data for CMD_ERROR
// NM -> Client or SS
typedef struct {
    int  code;                       // error code (e.g., HTTP-ish or errno-ish)
    char message[MAX_ERROR_MSG_LEN]; // human readable message
} MsgError;

// Data for CMD_CREATE_FILE
// Client -> NM
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char owner[MAX_USERNAME_LEN];
} MsgCreateFile;

// Data for CMD_DELETE_FILE
// Client -> NM -> SS
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char requester[MAX_USERNAME_LEN]; // NM will overwrite with authenticated username
} MsgDeleteFile;

// Data for CMD_VIEW_FILES_RESP
// NM -> Client: newline-separated list of filenames
typedef struct {
    char file_list[16384];
} MsgViewFilesResponse;

// Data for CMD_VIEW_FILES (request with flags)
// Client -> NM
typedef struct {
    int show_all;                 // 0: only accessible files, 1: all files
    int long_list;                // 0: names only, 1: include details
    char requester[MAX_USERNAME_LEN]; // NM may ignore and use authenticated username
} MsgViewFilesRequest;

// Data for CMD_SS_LIST_FILES_RESP
// SS -> NM: newline-separated list of filenames on that SS
typedef struct {
    char files[16384];
} MsgSSFileListResponse;

// Data for CMD_READ_FILE
// Client -> NM
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char requester[MAX_USERNAME_LEN]; // who is requesting (for ACL checks on SS)
} MsgReadFile;

// Data for CMD_READ_FILE_RESP
// NM -> Client: IP/port of SS to connect to, and found flag
typedef struct {
    int found;                 // 1 if found, 0 otherwise
    char ss_ip[MAX_IP_LEN];    // IPv4 string (e.g., "127.0.0.1")
    int ss_port;               // SS client listen port
} MsgReadFileResponse;

// Data for CMD_ADD_ACCESS / CMD_REM_ACCESS
// Client -> NM -> SS
// is_writer: 0 means reader, 1 means writer
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char target[MAX_USERNAME_LEN];
    int  is_writer;
    char requester[MAX_USERNAME_LEN]; // NM will overwrite with authenticated username
} MsgAccessChange;

// Data for CMD_INFO
// Client -> NM -> SS
typedef struct {
    char filename[MAX_FILENAME_LEN];
} MsgInfoRequest;

// Data for CMD_INFO_RESP
// SS -> NM -> Client
typedef struct {
    char info[2048];
} MsgInfoResponse;
 
// Data for CMD_WRITE_BEGIN (direct client -> SS): acquire a sentence lock only
typedef struct {
    char filename[MAX_FILENAME_LEN];
    int  sentence_index;       // 0-based
    char requester[MAX_USERNAME_LEN]; // who is requesting (for ACL checks on SS)
} MsgWriteBegin;

// Data for CMD_WRITE_FILE (direct client -> SS)
// Replace a specific sentence (0-based index) in the file with replacement text
// NOTE: Sentence-parsing/locking is handled on SS side. For MVP we enforce a max replacement size.
typedef struct {
    char filename[MAX_FILENAME_LEN];
    int  sentence_index;       // 0-based
    char replacement[2048];    // new sentence text (UTF-8)
    char requester[MAX_USERNAME_LEN]; // who is requesting (for ACL checks on SS)
} MsgWriteFile;

// Data for CMD_WRITE_DONE (client -> SS): release a previously acquired sentence lock
typedef struct {
    char filename[MAX_FILENAME_LEN];
    int sentence_index;        // 0-based
    char requester[MAX_USERNAME_LEN]; // who is requesting
} MsgWriteDone;

// Data for CMD_UNDO (Client -> NM -> SS)
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char requester[MAX_USERNAME_LEN];
} MsgUndoRequest;

// Data for CMD_LIST_USERS_RESP
typedef struct {
    char users[16384]; // newline-separated usernames
} MsgUsersListResponse;


// --- HELPER FUNCTIONS ---
// These ensure all data is sent/received reliably,
// handling cases where TCP sends/receives data in chunks.

/**
 * @brief Reliably sends a buffer of a specific size over a socket.
 * @param socket_fd The socket file descriptor.
 * @param buffer The data to send.
 * @param size The number of bytes to send.
 * @return true on success, false on failure.
 */
static inline bool send_all(int socket_fd, const void* buffer, size_t size) {
    const char* ptr = (const char*)buffer;
    size_t total_sent = 0;
    while (total_sent < size) {
        ssize_t sent = send(socket_fd, ptr + total_sent, size - total_sent, 0);
        if (sent <= 0) {
            // 0 means connection closed, -1 is an error
            perror("send_all");
            return false;
        }
        total_sent += (size_t)sent;
    }
    return true;
}

/**
 * @brief Reliably receives a specific number of bytes from a socket.
 * @param socket_fd The socket file descriptor.
 * @param buffer The buffer to fill.
 * @param size The number of bytes to receive.
 * @return true on success, false on failure (e.g., connection closed).
 */
static inline bool recv_all(int socket_fd, void* buffer, size_t size) {
    char* ptr = (char*)buffer;
    size_t total_received = 0;
    while (total_received < size) {
        ssize_t received = recv(socket_fd, ptr + total_received, size - total_received, 0);
        if (received <= 0) {
            // 0 means connection closed, -1 is an error
            if (received == 0) {
                // Connection closed gracefully
                return false;
            }
            perror("recv_all");
            return false;
        }
        total_received += (size_t)received;
    }
    return true;
}

#endif // PROTOCOL_H

