#define _POSIX_C_SOURCE 200809L

#if defined(__APPLE__)
#include <CommonCrypto/CommonDigest.h>
#define FALA_SHA256_DIGEST_LENGTH CC_SHA256_DIGEST_LENGTH
#else
#include <openssl/sha.h>
#define FALA_SHA256_DIGEST_LENGTH SHA256_DIGEST_LENGTH
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int has_environment(void) {
    return getenv("FALA_EFFECTOR_INPUT_DIR") != NULL &&
           getenv("FALA_EFFECTOR_OUTPUT_DIR") != NULL &&
           getenv("FALA_EFFECTOR_MANIFEST") != NULL;
}

static char *read_file(const char *path, size_t *out_len) {
    FILE *file = fopen(path, "rb");
    char *buf;
    long size;
    if (file == NULL) return NULL;
    if (fseek(file, 0, SEEK_END) != 0) { fclose(file); return NULL; }
    size = ftell(file);
    if (size < 0) { fclose(file); return NULL; }
    if (fseek(file, 0, SEEK_SET) != 0) { fclose(file); return NULL; }
    buf = malloc((size_t)size + 1);
    if (buf == NULL) { fclose(file); return NULL; }
    if (fread(buf, 1, (size_t)size, file) != (size_t)size) { free(buf); fclose(file); return NULL; }
    buf[size] = '\0';
    fclose(file);
    if (out_len) *out_len = (size_t)size;
    return buf;
}

static char *json_string(const char *json, const char *key) {
    char needle[128];
    const char *start;
    const char *end;
    size_t n;
    char *out;
    if (snprintf(needle, sizeof(needle), "\"%s\":\"", key) >= (int)sizeof(needle)) return NULL;
    start = strstr(json, needle);
    if (start == NULL) return NULL;
    start += strlen(needle);
    end = strchr(start, '"');
    if (end == NULL) return NULL;
    n = (size_t)(end - start);
    out = malloc(n + 1);
    if (out == NULL) return NULL;
    memcpy(out, start, n);
    out[n] = '\0';
    return out;
}

static char *hex_sha256(const char *text) {
    unsigned char digest[FALA_SHA256_DIGEST_LENGTH];
    static const char *hex = "0123456789abcdef";
    char *out = malloc(FALA_SHA256_DIGEST_LENGTH * 2 + 1);
    int i;
    if (out == NULL) return NULL;
#if defined(__APPLE__)
    CC_SHA256(text, (CC_LONG)strlen(text), digest);
#else
    if (SHA256((const unsigned char *)text, strlen(text), digest) == NULL) {
        free(out);
        return NULL;
    }
#endif
    for (i = 0; i < FALA_SHA256_DIGEST_LENGTH; i++) {
        out[i * 2] = hex[(digest[i] >> 4) & 0xf];
        out[i * 2 + 1] = hex[digest[i] & 0xf];
    }
    out[FALA_SHA256_DIGEST_LENGTH * 2] = '\0';
    return out;
}

int main(int argc, char **argv) {
    const char *mode = argc > 1 ? argv[1] : "";
    const char *output_dir = getenv("FALA_EFFECTOR_OUTPUT_DIR");
    const char *secret = getenv("SECRET");
    const char *preset = getenv("FALA_RESULT");
    const char *payload_env = getenv("FALA_PAYLOAD");
    char result_path[4096];
    FILE *result;
    struct timespec delay;
    char *manifest;
    char *from;
    char *to;
    char *job;
    char *ref;
    const char *payload;
    char *body;
    char *digest;
    int body_n;

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
    if (preset != NULL && preset[0] != '\0') {
        (void)fprintf(result, "%s\n", preset);
        (void)fclose(result);
        return 0;
    }
    manifest = read_file(getenv("FALA_EFFECTOR_MANIFEST"), NULL);
    if (manifest == NULL) { fclose(result); return 11; }
    from = json_string(manifest, "to");
    to = json_string(manifest, "from");
    job = json_string(manifest, "job");
    ref = json_string(manifest, "id");
    free(manifest);
    if (from == NULL || to == NULL || job == NULL || ref == NULL) {
        free(from); free(to); free(job); free(ref); fclose(result); return 12;
    }
    payload = (payload_env != NULL && payload_env[0] != '\0') ? payload_env : "{\"ok\":true}";
    body_n = snprintf(NULL, 0,
        "{\"from\":\"%s\",\"job\":\"%s\",\"kind\":\"result\",\"payload\":%s,\"protocol\":\"fala\",\"ref\":\"%s\",\"status\":\"ok\",\"to\":\"%s\"}",
        from, job, payload, ref, to);
    body = malloc((size_t)body_n + 1);
    if (body == NULL) { free(from); free(to); free(job); free(ref); fclose(result); return 13; }
    (void)snprintf(body, (size_t)body_n + 1,
        "{\"from\":\"%s\",\"job\":\"%s\",\"kind\":\"result\",\"payload\":%s,\"protocol\":\"fala\",\"ref\":\"%s\",\"status\":\"ok\",\"to\":\"%s\"}",
        from, job, payload, ref, to);
    digest = hex_sha256(body);
    if (digest == NULL) { free(body); free(from); free(to); free(job); free(ref); fclose(result); return 14; }
    (void)fprintf(result,
        "{\"from\":\"%s\",\"id\":\"msg:sha256:%s\",\"job\":\"%s\",\"kind\":\"result\",\"payload\":%s,\"protocol\":\"fala\",\"ref\":\"%s\",\"status\":\"ok\",\"to\":\"%s\"}\n",
        from, digest, job, payload, ref, to);
    free(digest); free(body); free(from); free(to); free(job); free(ref);
    (void)fclose(result);
    return 0;
}
