#include "native_process_host.h"

#define _DARWIN_C_SOURCE 1
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <spawn.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

extern char **environ;

struct fala_process_host {
    pid_t pid;
    fala_process_status status;
    int exit_code;
    int term_signal;
    int timed_out;
    int cancelled;
    int error_code;
    char error_message[256];
    int64_t timeout_ms;
    int64_t terminate_grace_ms;
    int reaped;
};


static void set_errno_error(fala_process_host *process, int code, const char *prefix, int value) {
    if (process == NULL) return;
    process->error_code = code;
    (void)snprintf(process->error_message, sizeof(process->error_message), "%s: %s", prefix, strerror(value));
}

static int64_t monotonic_ms(void) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) return -1;
    if (now.tv_sec > INT64_MAX / 1000) return INT64_MAX;
    return (int64_t)now.tv_sec * 1000 + (int64_t)now.tv_nsec / 1000000;
}

static void sleep_ms(int64_t milliseconds) {
    struct timespec delay;
    if (milliseconds <= 0) return;
    delay.tv_sec = (time_t)(milliseconds / 1000);
    delay.tv_nsec = (long)((milliseconds % 1000) * 1000000);
    while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {}
}

static int record_wait(fala_process_host *process, int wait_status) {
    process->reaped = 1;
    if (WIFEXITED(wait_status)) {
        process->status = FALA_PROCESS_EXITED;
        process->exit_code = WEXITSTATUS(wait_status);
        process->term_signal = 0;
    } else if (WIFSIGNALED(wait_status)) {
        process->status = process->timed_out ? FALA_PROCESS_STATUS_TIMED_OUT :
            (process->cancelled ? FALA_PROCESS_STATUS_CANCELLED : FALA_PROCESS_SIGNALED);
        process->exit_code = -1;
        process->term_signal = WTERMSIG(wait_status);
    }
    return 0;
}

static int reap_blocking(fala_process_host *process) {
    int wait_status;
    pid_t result;
    if (process == NULL || process->reaped) return 0;
    do {
        result = waitpid(process->pid, &wait_status, 0);
    } while (result < 0 && errno == EINTR);
    if (result < 0) {
        set_errno_error(process, FALA_PROCESS_SYSTEM_ERROR, "waitpid", errno);
        process->status = FALA_PROCESS_STATUS_ERROR;
        return -1;
    }
    return record_wait(process, wait_status);
}

static void terminate_group(fala_process_host *process) {
    int64_t grace;
    int64_t deadline;
    int wait_status;
    pid_t result;
    if (process == NULL || process->reaped) return;

    /* The process is the leader of a private group (pgid == pid). */
    if (killpg(process->pid, SIGTERM) != 0 && errno != ESRCH) {
        set_errno_error(process, FALA_PROCESS_SYSTEM_ERROR, "killpg(SIGTERM)", errno);
    }
    grace = process->terminate_grace_ms;
    if (grace < 0) grace = 100;
    deadline = monotonic_ms();
    if (deadline >= 0 && grace <= INT64_MAX - deadline) deadline += grace;
    else deadline = INT64_MAX;
    for (;;) {
        do {
            result = waitpid(process->pid, &wait_status, WNOHANG);
        } while (result < 0 && errno == EINTR);
        if (result == process->pid) {
            (void)record_wait(process, wait_status);
            return;
        }
        if (result < 0 && errno != EINTR && errno != ECHILD) {
            set_errno_error(process, FALA_PROCESS_SYSTEM_ERROR, "waitpid", errno);
            process->status = FALA_PROCESS_STATUS_ERROR;
            return;
        }
        if (deadline != INT64_MAX) {
            int64_t now = monotonic_ms();
            if (now >= 0 && now >= deadline) break;
        }
        sleep_ms(1);
    }
    if (killpg(process->pid, SIGKILL) != 0 && errno != ESRCH) {
        set_errno_error(process, FALA_PROCESS_SYSTEM_ERROR, "killpg(SIGKILL)", errno);
    }
    /* Always reap, even if the group disappeared between signals. */
    (void)reap_blocking(process);
}

void fala_process_options_init(fala_process_options *options) {
    if (options == NULL) return;
    (void)memset(options, 0, sizeof(*options));
    options->stdin_fd = -1;
    options->stdout_fd = -1;
    options->stderr_fd = -1;
    options->timeout_ms = -1;
    options->terminate_grace_ms = 100;
}

static int add_redirect(posix_spawn_file_actions_t *actions, int target, int source,
                        const char *path, int flags, mode_t mode) {
    if (source >= 0) {
        if (source == target) return 0;
        return posix_spawn_file_actions_adddup2(actions, source, target);
    }
    if (path == NULL || path[0] == '\0') return 0;
    return posix_spawn_file_actions_addopen(actions, target, path, flags, mode);
}
 
static char *join_paths(const char *base, const char *suffix) {
    size_t base_len = strlen(base);
    size_t suffix_len = strlen(suffix);
    int separator = base_len != 0 && base[base_len - 1] != '/';
    char *result = (char *)malloc(base_len + (size_t)separator + suffix_len + 1);
    if (result == NULL) return NULL;
    memcpy(result, base, base_len);
    if (separator) result[base_len] = '/';
    memcpy(result + base_len + (size_t)separator, suffix, suffix_len + 1);
    return result;
}

static char *absolute_working_dir(const char *cwd) {
    char *current = getcwd(NULL, 0);
    char *result;
    if (current == NULL) return NULL;
    if (cwd == NULL || cwd[0] == '\0' || cwd[0] == '/') {
        if (cwd == NULL || cwd[0] == '\0') return current;
        result = strdup(cwd);
        free(current);
        return result;
    }
    result = join_paths(current, cwd);
    free(current);
    return result;
}

static char *resolve_executable(const char *name, const char *const *envp, const char *cwd) {
    const char *path = NULL;
    const char *cursor;
    const char *end;
    char *working_dir;
    char *candidate;
    char *relative;
    size_t name_len;
    if (name == NULL || name[0] == '\0') return NULL;
    working_dir = absolute_working_dir(cwd);
    if (working_dir == NULL) return NULL;
    if (name[0] == '/') {
        if (access(name, X_OK) == 0) {
            candidate = strdup(name);
            free(working_dir);
            return candidate;
        }
        free(working_dir);
        return NULL;
    }
    if (envp != NULL) {
        size_t i;
        for (i = 0; envp[i] != NULL; ++i) {
            if (strncmp(envp[i], "PATH=", 5) == 0) {
                path = envp[i] + 5;
                break;
            }
        }
    } else path = getenv("PATH");
    if (path == NULL) path = "/usr/bin:/bin";
    name_len = strlen(name);
    if (strchr(name, '/') != NULL) {
        candidate = join_paths(working_dir, name);
        free(working_dir);
        if (candidate == NULL || access(candidate, X_OK) != 0) {
            free(candidate);
            return NULL;
        }
        return candidate;
    }
    cursor = path;
    for (;;) {
        size_t directory_len;
        end = strchr(cursor, ':');
        directory_len = end == NULL ? strlen(cursor) : (size_t)(end - cursor);
        if (directory_len == 0) {
            candidate = join_paths(working_dir, name);
        } else if (cursor[0] == '/') {
            relative = (char *)malloc(directory_len + 1 + name_len + 1);
            if (relative == NULL) { free(working_dir); return NULL; }
            memcpy(relative, cursor, directory_len);
            relative[directory_len] = '/';
            memcpy(relative + directory_len + 1, name, name_len + 1);
            candidate = relative;
        } else {
            relative = (char *)malloc(directory_len + 1 + name_len + 1);
            if (relative == NULL) { free(working_dir); return NULL; }
            memcpy(relative, cursor, directory_len);
            relative[directory_len] = '/';
            memcpy(relative + directory_len + 1, name, name_len + 1);
            candidate = join_paths(working_dir, relative);
            free(relative);
        }
        if (candidate == NULL) { free(working_dir); return NULL; }
        if (access(candidate, X_OK) == 0) {
            free(working_dir);
            return candidate;
        }
        free(candidate);
        if (end == NULL) break;
        cursor = end + 1;
    }
    free(working_dir);
    return NULL;
}

static char **blob_vector(const char *blob, int count) {
    char **values;
    size_t index;
    if (count < 0 || (count > 0 && blob == NULL)) return NULL;
    values = (char **)calloc((size_t)count + 1, sizeof(*values));
    if (values == NULL) return NULL;
    for (index = 0; index < (size_t)count; ++index) {
        values[index] = (char *)blob;
        blob += strlen(blob) + 1;
    }
    values[count] = NULL;
    return values;
}

fala_process_result fala_process_start_blob(const char *argv_blob, int argc,
                                            const char *env_blob, int envc,
                                            const char *cwd,
                                            const char *stdin_path,
                                            const char *stdout_path,
                                            const char *stderr_path,
                                            int64_t timeout_ms,
                                            int64_t terminate_grace_ms,
                                            fala_process_host **out_process) {
    fala_process_options options;
    char **argv;
    char **envp;
    fala_process_result result;
    if (out_process == NULL) return FALA_PROCESS_INVALID_ARGUMENT;
    *out_process = NULL;
    if (argc <= 0 || envc < 0 || argv_blob == NULL || (envc > 0 && env_blob == NULL)) return FALA_PROCESS_INVALID_ARGUMENT;
    argv = blob_vector(argv_blob, argc);
    envp = blob_vector(env_blob, envc);
    if (argv == NULL || envp == NULL) { free(argv); free(envp); return FALA_PROCESS_SYSTEM_ERROR; }
    fala_process_options_init(&options);
    options.argv = (const char *const *)argv;
    options.envp = (const char *const *)envp;
    options.cwd = cwd;
    options.stdin_path = stdin_path;
    options.stdout_path = stdout_path;
    options.stderr_path = stderr_path;
    options.timeout_ms = timeout_ms;
    options.terminate_grace_ms = terminate_grace_ms;
    result = fala_process_start(&options, out_process);
    free(argv);
    free(envp);
    return result;
}
 
fala_process_result fala_process_start(const fala_process_options *options,
                                       fala_process_host **out_process) {
    posix_spawn_file_actions_t actions;
    posix_spawnattr_t attributes;
    fala_process_host *process;
    pid_t pid;
    int result;
    int in_open = -1, out_open = -1, err_open = -1;
    short flags = POSIX_SPAWN_SETPGROUP;
    const char *const *environment;
    char *executable = NULL;

    if (out_process == NULL) return FALA_PROCESS_INVALID_ARGUMENT;
    *out_process = NULL;
    if (options == NULL || options->argv == NULL || options->argv[0] == NULL || options->argv[0][0] == '\0') return FALA_PROCESS_INVALID_ARGUMENT;
    if (options->timeout_ms < -1 || options->terminate_grace_ms < -1) return FALA_PROCESS_INVALID_ARGUMENT;
    process = (fala_process_host *)calloc(1, sizeof(*process));
    if (process == NULL) return FALA_PROCESS_SYSTEM_ERROR;
    process->pid = -1;
    process->status = FALA_PROCESS_STATUS_ERROR;
    process->reaped = 0;
    process->exit_code = -1;
    process->term_signal = 0;
    process->timeout_ms = options->timeout_ms;
    process->terminate_grace_ms = options->terminate_grace_ms;

    result = posix_spawn_file_actions_init(&actions);
    if (result != 0) { set_errno_error(process, FALA_PROCESS_SYSTEM_ERROR, "file actions", result); goto fail; }
    result = add_redirect(&actions, STDIN_FILENO, options->stdin_fd, options->stdin_path, O_RDONLY, 0);
    if (result == 0) result = add_redirect(&actions, STDOUT_FILENO, options->stdout_fd, options->stdout_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (result == 0) result = add_redirect(&actions, STDERR_FILENO, options->stderr_fd, options->stderr_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (result != 0) { set_errno_error(process, FALA_PROCESS_SYSTEM_ERROR, "redirect", result); posix_spawn_file_actions_destroy(&actions); goto fail; }
    if (options->cwd != NULL && options->cwd[0] != '\0') {
        result = posix_spawn_file_actions_addchdir(&actions, options->cwd);
        if (result != 0) { set_errno_error(process, FALA_PROCESS_SYSTEM_ERROR, "chdir", result); posix_spawn_file_actions_destroy(&actions); goto fail; }
    }
    result = posix_spawnattr_init(&attributes);
    if (result != 0) { set_errno_error(process, FALA_PROCESS_SYSTEM_ERROR, "spawn attributes", result); posix_spawn_file_actions_destroy(&actions); goto fail; }
    result = posix_spawnattr_setflags(&attributes, flags);
    if (result == 0) result = posix_spawnattr_setpgroup(&attributes, 0);
    if (result != 0) { set_errno_error(process, FALA_PROCESS_SYSTEM_ERROR, "spawn attributes", result); posix_spawnattr_destroy(&attributes); posix_spawn_file_actions_destroy(&actions); goto fail; }
    environment = options->envp == NULL ? (const char *const *)environ : options->envp;
    executable = resolve_executable(options->argv[0], environment, options->cwd);
    if (executable == NULL) { set_errno_error(process, FALA_PROCESS_SYSTEM_ERROR, "resolve executable", ENOENT); posix_spawnattr_destroy(&attributes); posix_spawn_file_actions_destroy(&actions); goto fail; }
    result = posix_spawn(&pid, executable, &actions, &attributes,
                         (char *const *)options->argv, (char *const *)environment);
    free(executable);
    executable = NULL;
    posix_spawnattr_destroy(&attributes);
    posix_spawn_file_actions_destroy(&actions);
    if (in_open >= 0) (void)close(in_open);
    if (out_open >= 0) (void)close(out_open);
    if (err_open >= 0) (void)close(err_open);
    if (result != 0) { set_errno_error(process, FALA_PROCESS_SYSTEM_ERROR, "posix_spawn", result); goto fail; }
    process->pid = pid;
    process->status = FALA_PROCESS_RUNNING;
    *out_process = process;
    return FALA_PROCESS_OK;
fail:
    if (in_open >= 0) (void)close(in_open);
    if (out_open >= 0) (void)close(out_open);
    if (err_open >= 0) (void)close(err_open);
    /* Keep the failed handle so callers can inspect the concrete startup error. */
    *out_process = process;
    process->reaped = 1;
    return FALA_PROCESS_SYSTEM_ERROR;
}

fala_process_result fala_process_wait(fala_process_host *process) {
    int wait_status;
    int64_t start;
    int64_t deadline;
    pid_t result;
    if (process == NULL) return FALA_PROCESS_INVALID_ARGUMENT;
    if (process->reaped) {
        if (process->timed_out) return FALA_PROCESS_TIMED_OUT;
        if (process->cancelled) return FALA_PROCESS_CANCELLED;
        return process->status == FALA_PROCESS_STATUS_ERROR ? FALA_PROCESS_SYSTEM_ERROR : FALA_PROCESS_OK;
    }
    start = monotonic_ms();
    deadline = INT64_MAX;
    if (process->timeout_ms >= 0 && start >= 0 && process->timeout_ms <= INT64_MAX - start) deadline = start + process->timeout_ms;
    for (;;) {
        do { result = waitpid(process->pid, &wait_status, WNOHANG); } while (result < 0 && errno == EINTR);
        if (result == process->pid) { (void)record_wait(process, wait_status); return FALA_PROCESS_OK; }
        if (result < 0 && errno != EINTR) { set_errno_error(process, FALA_PROCESS_SYSTEM_ERROR, "waitpid", errno); process->status = FALA_PROCESS_STATUS_ERROR; return FALA_PROCESS_SYSTEM_ERROR; }
        if (deadline != INT64_MAX) {
            int64_t now = monotonic_ms();
            if (now >= 0 && now >= deadline) {
                process->timed_out = 1;
                terminate_group(process);
                return FALA_PROCESS_TIMED_OUT;
            }
        }
        sleep_ms(1);
    }
}

fala_process_result fala_process_cancel(fala_process_host *process) {
    if (process == NULL) return FALA_PROCESS_INVALID_ARGUMENT;
    if (!process->reaped) {
        process->cancelled = 1;
        terminate_group(process);
    }
    return process->status == FALA_PROCESS_STATUS_ERROR ? FALA_PROCESS_SYSTEM_ERROR : FALA_PROCESS_CANCELLED;
}

void fala_process_destroy(fala_process_host *process) {
    if (process == NULL) return;
    if (!process->reaped) {
        process->cancelled = 1;
        terminate_group(process);
    }
    free(process);
}

fala_process_status fala_process_get_status(const fala_process_host *process) { return process == NULL ? FALA_PROCESS_STATUS_ERROR : process->status; }
int fala_process_get_pid(const fala_process_host *process) { return process == NULL ? -1 : (int)process->pid; }
int fala_process_get_exit_code(const fala_process_host *process) { return process == NULL ? -1 : process->exit_code; }
int fala_process_get_term_signal(const fala_process_host *process) { return process == NULL ? 0 : process->term_signal; }
int fala_process_was_timed_out(const fala_process_host *process) { return process != NULL && process->timed_out; }
int fala_process_was_cancelled(const fala_process_host *process) { return process != NULL && process->cancelled; }
int fala_process_get_error_code(const fala_process_host *process) { return process == NULL ? EINVAL : process->error_code; }
const char *fala_process_get_error_message(const fala_process_host *process) { return process == NULL ? "invalid process handle" : process->error_message; }
