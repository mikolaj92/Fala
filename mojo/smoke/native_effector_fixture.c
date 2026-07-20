#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int has_environment(void) {
    return getenv("FALA_EFFECTOR_INPUT_DIR") != NULL &&
           getenv("FALA_EFFECTOR_OUTPUT_DIR") != NULL &&
           getenv("FALA_EFFECTOR_MANIFEST") != NULL;
}

int main(int argc, char **argv) {
    const char *mode = argc > 1 ? argv[1] : "";
    const char *output_dir = getenv("FALA_EFFECTOR_OUTPUT_DIR");
    const char *secret = getenv("SECRET");
    char result_path[4096];
    FILE *result;
    struct timespec delay;

    if (!has_environment() || output_dir == NULL || secret == NULL) return 8;
    (void)snprintf(result_path, sizeof(result_path), "%s/result.json", output_dir);
    (void)fprintf(stdout, "fixture-secret=%s\n", secret);
    (void)fprintf(stderr, "fixture-secret=%s\n", secret);
    if (strcmp(mode, "nonzero") == 0) {
        (void)fprintf(stderr, "fixture failed\n");
        return 7;
    }
    if (strcmp(mode, "sleep") == 0) {
        delay.tv_sec = 1;
        delay.tv_nsec = 0;
        (void)nanosleep(&delay, NULL);
        return 0;
    }
    if (strcmp(mode, "no-output") == 0) return 0;
    if (strcmp(mode, "success") != 0) return 9;
    result = fopen(result_path, "w");
    if (result == NULL) return 10;
    (void)fprintf(result, "{\"ok\":true,\"secret\":\"%s\"}\n", secret);
    (void)fclose(result);
    return 0;
}
