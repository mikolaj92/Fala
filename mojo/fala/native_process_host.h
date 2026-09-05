#ifndef FALA_NATIVE_PROCESS_HOST_H
#define FALA_NATIVE_PROCESS_HOST_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct fala_process_host fala_process_host;

typedef enum fala_process_result {
    FALA_PROCESS_OK = 0,
    FALA_PROCESS_INVALID_ARGUMENT = 1,
    FALA_PROCESS_SYSTEM_ERROR = 2,
    FALA_PROCESS_TIMED_OUT = 3,
    FALA_PROCESS_CANCELLED = 4
} fala_process_result;

typedef enum fala_process_status {
    FALA_PROCESS_RUNNING = 0,
    FALA_PROCESS_EXITED = 1,
    FALA_PROCESS_SIGNALED = 2,
    FALA_PROCESS_STATUS_TIMED_OUT = 3,
    FALA_PROCESS_STATUS_CANCELLED = 4,
    FALA_PROCESS_STATUS_ERROR = 5
} fala_process_status;

/*
 * All strings and arrays are borrowed for the duration of
 * fala_process_start. argv must be NULL terminated and argv[0] is the
 * executable path. envp is also NULL terminated; when NULL, the host's
 * current environment is used. A negative timeout disables the deadline.
 *
 * A descriptor (>= 0) takes precedence over its path. Descriptors are
 * duplicated into the child and are never closed by the host in the parent;
 * this makes caller-created pipes suitable for streaming. Paths are opened
 * by the host and closed after spawn. A NULL descriptor path leaves that
 * stream inherited from the host.
 */
typedef struct fala_process_options {
    const char *const *argv;
    const char *const *envp;
    const char *cwd;
    const char *stdin_path;
    const char *stdout_path;
    const char *stderr_path;
    int stdin_fd;
    int stdout_fd;
    int stderr_fd;
    int64_t timeout_ms;
    int64_t terminate_grace_ms;
} fala_process_options;

void fala_process_options_init(fala_process_options *options);

/*
 * Host-process environment lookup for subprocess inherit_env resolution.
 * Returns NULL when *name is unset; otherwise a pointer into the process
 * environment (do not free). Empty string means the variable is set blank.
 */
const char *fala_host_getenv(const char *name);

/* Starts one direct-argv child in its own process group. */
fala_process_result fala_process_start(const fala_process_options *options,
                                       fala_process_host **out_process);

/* Mojo-friendly NUL-separated argv/envp entry point. Blobs contain exactly
 * argc/envc strings, each terminated by NUL; no shell parsing is performed. */
fala_process_result fala_process_start_blob(const char *argv_blob, int argc,
                                            const char *env_blob, int envc,
                                            const char *cwd,
                                            const char *stdin_path,
                                            const char *stdout_path,
                                            const char *stderr_path,
                                            int64_t timeout_ms,
                                            int64_t terminate_grace_ms,
                                            fala_process_host **out_process);

/* Nonblocking completion probe; returns OK and leaves status RUNNING. */
fala_process_result fala_process_poll(fala_process_host *process);

/* Waits for completion, polling with a monotonic clock. */
fala_process_result fala_process_wait(fala_process_host *process);

/* Cancels and reaps the child, using process-group TERM, grace, then KILL. */
fala_process_result fala_process_cancel(fala_process_host *process);

/* Destruction is NULL-safe and always reaps an active child. The handle is freed. */
void fala_process_destroy(fala_process_host *process);

fala_process_status fala_process_get_status(const fala_process_host *process);
int fala_process_get_pid(const fala_process_host *process);
int fala_process_get_exit_code(const fala_process_host *process);
int fala_process_get_term_signal(const fala_process_host *process);
int fala_process_was_timed_out(const fala_process_host *process);
int fala_process_was_cancelled(const fala_process_host *process);
int fala_process_get_error_code(const fala_process_host *process);
const char *fala_process_get_error_message(const fala_process_host *process);

#ifdef __cplusplus
}
#endif

#endif /* FALA_NATIVE_PROCESS_HOST_H */
