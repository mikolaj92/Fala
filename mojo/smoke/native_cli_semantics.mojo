from std.os import remove
from std.ffi import CStringSlice, c_int, external_call
from std.memory import UnsafePointer
from std.pathlib import Path
from fala.json import parse_json
from fala.native_cli_surface import _count, _word, _homeostat_domain_values, dispatch_native_command
from fala.schema import initialize_native_schema
from fala.domain import BridgeDelivery, EventRef, Impulse, RuntimeBudget, RuntimeRef, RunRef
from fala.domain_store import NativeDomainStore
from fala.sqlite import Connection



def _clean_bridge_path(path: String):
    try:
        remove(path)
    except err:
        pass
    try:
        remove(path + "-wal")
    except err:
        pass
    try:
        remove(path + "-shm")
    except err:
        pass


def _remove_init_tree(path: Path) raises:
    if path.__fspath__() == "" or not path.exists():
        return
    if path.is_dir():
        for entry in path.listdir():
            _remove_init_tree(path / entry.name())
        var text = path.__fspath__() + "\0"
        var c_path = CStringSlice(text)
        var result = external_call["rmdir", c_int](c_path.unsafe_ptr())
        if result != 0:
            raise Error("native CLI semantics: unable to remove init workspace directory")
    else:
        remove(path)
    if path.exists():
        raise Error("native CLI semantics: init workspace cleanup left a path")

def _fresh_init_root() raises -> String:
    var template = "/tmp/fala-native-cli-init-XXXXXX\0"
    var c_template = CStringSlice(template)
    var root_ptr = external_call["mkdtemp", UnsafePointer[UInt8, MutUntrackedOrigin]](c_template.unsafe_ptr())
    if Int(root_ptr) == 0:
        raise Error("native CLI semantics: unable to create unique init workspace")
    return String(unsafe_from_utf8_ptr=root_ptr)

def _seed_bridge_run(mut store: NativeDomainStore, run_id: String) raises:
    var stmt = store.db.query("INSERT INTO runs (id,status,metadata,created_at,updated_at,schema_version) VALUES (?, 'completed', '{}', ?, ?, 6)")
    stmt.bind_text(1, run_id)
    stmt.bind_text(2, "2026-01-01T00:00:00Z")
    stmt.bind_text(3, "2026-01-01T00:00:00Z")
    _ = stmt.step()
    stmt.close()
    store.db.commit()


def _bridge_delivery() -> BridgeDelivery:
    var source = RunRef(RuntimeRef("runtime-source", "file://source", "{}"), "bridge-source")
    var target = RunRef(RuntimeRef("runtime-target", "file://target", "{}"), "bridge-target")
    var impulse = Impulse("bridge-impulse", "bridge-source", "bridge.smoke", "{\"value\":1}", "{}", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
    return BridgeDelivery(
        id="bridge-delivery",
        run_id="bridge-source",
        idempotency_key="source-bridge-key",
        source=source^,
        target=target^,
        impulse=impulse^,
        event_ref=EventRef(RuntimeRef("runtime", "", "{}"), "run", "", 0),
        pool_id="",
        budget=RuntimeBudget(runtime_hops=2, impulse_count=2),
        status="pending",
        attempts=0,
        metadata="{}",
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
    )



def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("native CLI semantics: " + message)


def _has(value: String, needle: String) -> Bool:
    return value.find(needle) >= 0


def _durable_init_smoke() raises:
    var init_root = _fresh_init_root()
    var init_root_path = Path(init_root)
    try:
        var init_db = init_root + "/workspace/state.sqlite"
        var init_reaction_root = init_root + "/workspace/reactions"
        var initialized_workspace = dispatch_native_command("init --db '" + init_db + "' --reaction-root '" + init_reaction_root + "'")
        _check(_has(initialized_workspace, "\"ok\":true") and _has(initialized_workspace, init_db) and _has(initialized_workspace, init_reaction_root) and _has(initialized_workspace, "\"schema_version\":6"), "durable workspace initialization")
        _check(Path(init_db).exists() and Path(init_db).is_file(), "initialized database file")
        _check((init_root_path / "workspace").is_dir() and Path(init_reaction_root).is_dir(), "initialized workspace directories")
        _check(Path(init_reaction_root + "/blobs/sha256").is_dir(), "initialized reaction CAS directory")
        var repeated_workspace = dispatch_native_command("init --db '" + init_db + "' --reaction-root '" + init_reaction_root + "'")
        _check(_has(repeated_workspace, "\"ok\":true") and Path(init_db).is_file() and Path(init_reaction_root + "/blobs/sha256").is_dir(), "repeatable workspace initialization")
    except err:
        try:
            _remove_init_tree(init_root_path)
        except cleanup_err:
            pass
        raise err^
    _remove_init_tree(init_root_path)

def main() raises:
    var empty_token_command = "a '' b"
    _check(_count(empty_token_command) == 3, "empty quoted token counted")
    _check(_word(empty_token_command, 0) == "a" and _word(empty_token_command, 1) == "" and _word(empty_token_command, 2) == "b", "empty quoted token indexed")
    var path = "/tmp/fala-native-cli-semantic-20260716.sqlite"
    try:
        remove(path)
    except err:
        pass
    var initialized = dispatch_native_command("db init --db " + path)
    var missing_db = dispatch_native_command("db status --db")
    _check(_has(missing_db, "argument_error") and _has(missing_db, "missing value") and _has(missing_db, "--db"), "missing --db value")

    var spaced_path = "/tmp/fala native cli semantic 20260716.sqlite"
    try:
        remove(spaced_path)
    except err:
        pass
    var spaced_init = dispatch_native_command("db init --db '" + spaced_path + "'")
    _check(_has(spaced_init, "\"ok\":true") and _has(spaced_init, spaced_path), "whitespace-bearing database path")
    _check(_has(initialized, "\"ok\":true") and _has(initialized, path), "database initialization")
    _durable_init_smoke()
    var missing_process_list_run = dispatch_native_command("processes list --db " + path)
    _check(_has(missing_process_list_run, "argument_error") and _has(missing_process_list_run, "--run-id is required"), "process listing requires run id")
    var process_list_with_run = dispatch_native_command("processes list --db " + path + " --run-id cli-run")
    _check(_has(process_list_with_run, "\"ok\":true") and _has(process_list_with_run, "\"resource\":\"processes\""), "process listing accepts run id")
    var missing_trace_initial_run = dispatch_native_command("trace --db " + path)
    _check(_has(missing_trace_initial_run, "argument_error") and _has(missing_trace_initial_run, "--run-id is required"), "trace requires run id")
    # Shell-like quoting must preserve paths containing apostrophes, backslashes,
    # tabs, and spaces as one argument and escape them in JSON responses.
    var apostrophe_path = "/tmp/fala native cli semantic 20260716-operator's.sqlite"
    try:
        remove(apostrophe_path)
    except err:
        pass
    var apostrophe_init = dispatch_native_command("db init --db \"" + apostrophe_path + "\"")
    _check(_has(apostrophe_init, "\"ok\":true") and _has(apostrophe_init, apostrophe_path), "apostrophe-bearing database path")

    var backslash_path = "/tmp/fala-native-cli-semantic-20260716-back\\slash.sqlite"
    try:
        remove(backslash_path)
    except err:
        pass
    var backslash_init = dispatch_native_command("db init --db '" + backslash_path + "'")
    var backslash_status = dispatch_native_command("db status --db '" + backslash_path + "'")
    _check(_has(backslash_init, "\"ok\":true") and _has(backslash_status, "\"current\":true") and _has(backslash_status, "back\\\\slash.sqlite"), "backslash-bearing database path")

    var tab_path = "/tmp/fala-native-cli-semantic-20260716-tab\tpath.sqlite"
    try:
        remove(tab_path)
    except err:
        pass
    var tab_init = dispatch_native_command("db init --db '" + tab_path + "'")
    var tab_status = dispatch_native_command("db status --db '" + tab_path + "'")
    _check(_has(tab_init, "\"ok\":true") and _has(tab_status, "\"current\":true") and _has(tab_status, "tab\\tpath.sqlite"), "tab-bearing database path")
    var positional_status = dispatch_native_command("db status " + path)
    _check(_has(positional_status, "\"current\":true") and _has(positional_status, path), "database status positional path")

    var flagged_status = dispatch_native_command("db status --db " + path)
    _check(_has(flagged_status, "\"current\":true") and _has(flagged_status, path), "database status flagged path")
    var equals_status = dispatch_native_command("db status --db=" + path)
    _check(_has(equals_status, "\"current\":true") and _has(equals_status, path), "database status equals-form path")

    var positional_migrate = dispatch_native_command("db migrate " + path)
    _check(_has(positional_migrate, "\"ok\":true") and _has(positional_migrate, path), "database migration positional path")

    var flagged_migrate = dispatch_native_command("db migrate --db " + path)
    _check(_has(flagged_migrate, "\"ok\":true") and _has(flagged_migrate, path), "database migration flagged path")

    var positional_vacuum = dispatch_native_command("db vacuum " + path)
    _check(_has(positional_vacuum, "\"vacuumed\":true") and _has(positional_vacuum, path), "database vacuum positional path")

    var flagged_vacuum = dispatch_native_command("db vacuum --db " + path)
    _check(_has(flagged_vacuum, "\"vacuumed\":true") and _has(flagged_vacuum, path), "database vacuum flagged path")
    var invalid_init_ensure = dispatch_native_command("db init --db " + path + " --ensure-schema")
    _check(_has(invalid_init_ensure, "argument_error") and _has(invalid_init_ensure, "--ensure-schema"), "db init rejects ensure-schema")
    var invalid_migrate_ensure = dispatch_native_command("db migrate --db " + path + " --ensure-schema")
    _check(_has(invalid_migrate_ensure, "argument_error") and _has(invalid_migrate_ensure, "--ensure-schema"), "db migrate rejects ensure-schema")
    var invalid_vacuum_ensure = dispatch_native_command("db vacuum --db " + path + " --ensure-schema")
    _check(_has(invalid_vacuum_ensure, "argument_error") and _has(invalid_vacuum_ensure, "--ensure-schema"), "db vacuum rejects ensure-schema")

    var near_match = dispatch_native_command("runs starter --db " + path)
    _check(near_match == "{\"ok\":false,\"runtime\":\"mojo\",\"error\":{\"type\":\"unsupported_command\",\"message\":\"unsupported_command\"}}", "near-match command rejection")

    var runtime_list_removed = dispatch_native_command("runtimes list --db " + path)
    _check(_has(runtime_list_removed, "unsupported_command"), "runtimes list is removed from product surface")

    var legacy_run_create = dispatch_native_command("runs create --db " + path)
    _check(_has(legacy_run_create, "\"type\":\"legacy_alias\"") and _has(legacy_run_create, "use create-run"), "legacy runs create alias")

    var legacy_run_create_singular = dispatch_native_command("run create --db " + path)
    _check(_has(legacy_run_create_singular, "\"type\":\"legacy_alias\"") and _has(legacy_run_create_singular, "use create-run"), "legacy run create alias")

    var legacy_relations = dispatch_native_command("relations list --db " + path + " --run-id cli-run")
    _check(_has(legacy_relations, "\"resource\":\"relations\"") and _has(legacy_relations, "\"ok\":true"), "legacy relations alias")
    var legacy_bridges = dispatch_native_command("bridges list --db " + path + " --run-id cli-run")
    _check(_has(legacy_bridges, "\"resource\":\"bridges\"") and _has(legacy_bridges, "\"ok\":true"), "legacy bridges alias")


    var export_boundary = dispatch_native_command("export --db " + path)
    _check(export_boundary == "{\"ok\":false,\"runtime\":\"mojo\",\"error\":{\"type\":\"native_boundary\",\"message\":\"export requires a native file encoder\"}}", "export boundary")

    var storage_path = "/dev/null/fala-native-cli.sqlite"
    var storage = dispatch_native_command("db status --db " + storage_path)
    _check(_has(storage, "\"type\":\"storage_error\"") and _has(storage, "\"ok\":false"), "storage error envelope")
    var unknown_argument = dispatch_native_command("db status --db " + path + " --unknown")
    _check(_has(unknown_argument, "argument_error") and _has(unknown_argument, "--unknown"), "unknown option rejection")
    var unknown_list_argument = dispatch_native_command("events list --db " + path + " --unknown")
    _check(_has(unknown_list_argument, "argument_error") and _has(unknown_list_argument, "--unknown"), "list unknown option rejection")
    var known_but_invalid_event_option = dispatch_native_command("events list --db " + path + " --run-id cli-run --relation-type relation")
    _check(_has(known_but_invalid_event_option, "argument_error") and _has(known_but_invalid_event_option, "--relation-type"), "event list rejects irrelevant option")
    var known_but_invalid_command_option = dispatch_native_command("commands list --db " + path + " --run-id cli-run --relation-type relation")
    _check(_has(known_but_invalid_command_option, "argument_error") and _has(known_but_invalid_command_option, "--relation-type"), "command list rejects irrelevant option")
    var invalid_run_status = dispatch_native_command("runs list --db " + path + " --status nonsense")
    _check(_has(invalid_run_status, "argument_error") and _has(invalid_run_status, "invalid value for --status"), "run status enum validation")
    var invalid_process_status = dispatch_native_command("processes list --db " + path + " --run-id cli-run --status nonsense")
    _check(_has(invalid_process_status, "argument_error") and _has(invalid_process_status, "invalid value for --status"), "process status enum validation")
    var invalid_homeostat_status = dispatch_native_command("homeostats list --db " + path + " --run-id cli-run --status nonsense")
    _check(_has(invalid_homeostat_status, "argument_error") and _has(invalid_homeostat_status, "invalid value for --status"), "homeostat status enum validation")
    var invalid_bridge_status = dispatch_native_command("bridge list --db " + path + " --run-id cli-run --status nonsense")
    _check(_has(invalid_bridge_status, "argument_error") and _has(invalid_bridge_status, "invalid value for --status"), "bridge status enum validation")
    var trailing_argument = dispatch_native_command("runs list --db " + path + " trailing")
    _check(_has(trailing_argument, "argument_error") and _has(trailing_argument, "trailing"), "trailing argument rejection")


    var missing_create_id = dispatch_native_command("create-run --db " + path + " --now 2026-01-01T00:00:00Z")
    _check(_has(missing_create_id, "argument_error") and _has(missing_create_id, "--run-id is required; native run-id generation is unavailable"), "create-run omitted id fails closed")
    var missing_create_now = dispatch_native_command("create-run --db " + path + " --run-id cli-missing-now")
    _check(_has(missing_create_now, "argument_error") and _has(missing_create_now, "--now is required"), "create-run requires timestamp")
    var missing_impulse_now = dispatch_native_command("impulses create --db " + path + " --run-id cli-run --impulse-id missing-now --impulse-type demo")
    _check(_has(missing_impulse_now, "argument_error") and _has(missing_impulse_now, "--now is required"), "impulse creation requires timestamp")

    var created = dispatch_native_command(
        "create-run --db " + path + " --run-id cli-run --title smoke --metadata {\"x\":1} --now 2026-01-01T00:00:00Z"
    )
    _check(_has(created, "\"ok\":true") and _has(created, "\"run\":") and _has(created, "\"command\":") and _has(created, "\"replayed\":false") and _has(created, "\"id\":\"cli-run\"") and _has(created, "\"status\":\"created\"") and _has(created, "\"metadata\":{\"x\":1}") and _has(created, "\"created_at\":\"2026-01-01T00:00:00Z\"") and _has(created, "\"title\":\"smoke\"") and _has(created, "\"package_id\":null") and _has(created, "\"started_at\":null") and _has(created, "\"command_type\":\"run.create\"") and _has(created, "\"actor\":\"cli:user\"") and _has(created, "\"payload\":{\"run_id\":\"cli-run\",\"status\":\"created\"}") and not _has(created, "\"runtime\":"), "full create response")
    var replayed_create = dispatch_native_command(
        "create-run --db " + path + " --run-id cli-run --title smoke --metadata {\"x\":1} --now 2026-01-01T00:00:00Z"
    )
    _check(_has(replayed_create, "\"replayed\":true") and _has(replayed_create, "\"run\":") and _has(replayed_create, "\"command\":") and _has(replayed_create, "\"metadata\":{\"x\":1}"), "create replay response")
    var create_commands = dispatch_native_command("commands list --db " + path + " --run-id cli-run")
    var create_events = dispatch_native_command("events list --db " + path + " --run-id cli-run")
    _check(_has(create_commands, "\"count\":1") and _has(create_commands, "run.create") and _has(create_commands, "\"payload\":{\"run_id\":\"cli-run\",\"status\":\"created\"}") and _has(create_commands, "\"actor\":\"cli:user\""), "create command persisted exactly once")
    _check(_has(create_events, "\"count\":1") and _has(create_events, "run.created") and _has(create_events, "\"payload\":{\"run_id\":\"cli-run\",\"status\":\"created\"}") and _has(create_events, "\"actor\":\"cli:user\""), "create event persisted exactly once")
    var create_conflict = dispatch_native_command(
        "create-run --db " + path + " --run-id cli-run --title changed --metadata {\"x\":1} --now 2026-01-01T00:00:00Z"
    )
    _check(_has(create_conflict, "\"replayed\":true") and _has(create_conflict, "\"title\":\"smoke\"") and not _has(create_conflict, "changed"), "create replay keeps stored command")
    var observed = dispatch_native_command("runs observe --db " + path + " --run-id cli-run")
    _check(_has(observed, "\"ok\":true") and _has(observed, "\"run_id\":\"cli-run\"") and _has(observed, "\"event_watermark\":"), "run boundary observation")
    var missing_observed_run = dispatch_native_command("runs observe --db " + path)
    _check(_has(missing_observed_run, "argument_error") and _has(missing_observed_run, "--run-id is required"), "run observation requires run id")
    var missing_observed = dispatch_native_command("runs observe --db " + path + " --run-id missing-run")
    _check(_has(missing_observed, "\"type\":\"storage_error\"") and _has(missing_observed, "\"ok\":false"), "unknown run boundary error")
    var quoted_metadata = dispatch_native_command(
        "create-run --db " + path + " --run-id cli-spaced --metadata '{\"note\": \"hello world\"}' --now 2026-01-01T00:00:00Z"
    )
    _check(_has(quoted_metadata, "\"status\":\"created\"") and _has(quoted_metadata, "cli-spaced"), "quoted JSON metadata with spaces")
    var equals_metadata = dispatch_native_command(
        "create-run --db=" + path + " --run-id=cli-equals --title=equals --metadata={\"eq\":1} --now=2026-01-01T00:00:00Z"
    )
    _check(_has(equals_metadata, "\"status\":\"created\"") and _has(equals_metadata, "cli-equals"), "equals-form JSON metadata")
    var empty_quoted_value = dispatch_native_command(
        "create-run --db " + path + " --run-id cli-empty-quoted --title '' --metadata={} --now 2026-01-01T00:00:00Z"
    )
    _check(_has(empty_quoted_value, "\"status\":\"created\"") and _has(empty_quoted_value, "cli-empty-quoted"), "empty quoted option value")
    var repeated_metadata = dispatch_native_command(
        "create-run --db " + path + " --run-id cli-repeat --metadata tenant=demo --metadata region=us --metadata tenant=last --now 2026-01-01T00:00:00Z"
    )
    _check(_has(repeated_metadata, "\"status\":\"created\"") and _has(repeated_metadata, "cli-repeat"), "repeatable key=value metadata")
    var repeated_listing = dispatch_native_command("runs list --db " + path + " --run-id cli-repeat")
    _check(_has(repeated_listing, "\"region\":\"us\"") and _has(repeated_listing, "\"tenant\":\"last\""), "repeatable metadata persisted")
    var jsonl_with_next_option = dispatch_native_command("runs list --db " + path + " --jsonl --status created")
    _check(_has(jsonl_with_next_option, "cli-run") and not _has(jsonl_with_next_option, "\"items\":"), "runs list JSONL flag does not consume next option")
    var jsonl_disabled = dispatch_native_command("runs list --db " + path + " --jsonl=false")
    _check(_has(jsonl_disabled, "\"items\":"), "runs list JSONL false uses envelope")
    var malformed_metadata = dispatch_native_command("create-run --db " + path + " --run-id cli-bad-metadata --metadata malformed --now 2026-01-01T00:00:00Z")
    var missing_equals_metadata = dispatch_native_command("create-run --db " + path + " --run-id cli-missing-equals-metadata --metadata= --now 2026-01-01T00:00:00Z")
    var empty_key_metadata = dispatch_native_command("create-run --db " + path + " --run-id cli-empty-key-metadata --metadata =value --now 2026-01-01T00:00:00Z")
    _check(_has(empty_key_metadata, "argument_error") and _has(empty_key_metadata, "expected key=value"), "empty metadata key")
    _check(_has(missing_equals_metadata, "argument_error") and _has(missing_equals_metadata, "missing value for --metadata"), "missing equals metadata value")
    _check(_has(malformed_metadata, "argument_error") and _has(malformed_metadata, "expected key=value"), "malformed key=value metadata")
    var missing_metadata = dispatch_native_command("create-run --db " + path + " --run-id cli-missing-metadata --metadata --now 2026-01-01T00:00:00Z")
    _check(_has(missing_metadata, "argument_error") and _has(missing_metadata, "missing value for --metadata"), "missing metadata value")
    var repeated_json_metadata = dispatch_native_command("create-run --db " + path + " --run-id cli-repeated-json --metadata={\"a\":1} --metadata={\"b\":2} --now 2026-01-01T00:00:00Z")
    _check(_has(repeated_json_metadata, "argument_error") and _has(repeated_json_metadata, "JSON objects are ambiguous"), "ambiguous repeated JSON metadata")
    var unicode_metadata = dispatch_native_command("create-run --db " + path + " --run-id cli-unicode --metadata café=żółć --now 2026-01-01T00:00:00Z")
    _check(_has(unicode_metadata, "\"status\":\"created\"") and _has(unicode_metadata, "cli-unicode"), "unicode metadata creation")
    var unicode_listing = dispatch_native_command("runs list --db " + path + " --run-id cli-unicode")
    _check(_has(unicode_listing, "café") and _has(unicode_listing, "żółć"), "unicode metadata persisted")
    var control_title = dispatch_native_command(
        "create-run --db=" + path + " --run-id=cli-control --title=line\nfeed --metadata={} --now=2026-01-01T00:00:00Z"
    )
    _check(_has(control_title, "\"status\":\"created\"") and _has(control_title, "cli-control"), "control-character title creation")
    var control_listing = dispatch_native_command("runs list --db=" + path + " --run-id=cli-control")
    var parsed_control_listing = parse_json(control_listing)
    _ = parsed_control_listing
    _check(_has(control_listing, "\"title\":\"line\\nfeed\"") and not _has(control_listing, "\\u0000"), "control-character title JSON escaping")
    var metadata_listing = dispatch_native_command("runs list --db " + path)
    _check(_has(metadata_listing, "hello world"), "quoted metadata persisted")

    var relation_db = Connection(path)
    initialize_native_schema(relation_db)
    relation_db.execute("INSERT INTO impulses (run_id,id,impulse_type,payload,metadata,created_at,updated_at) VALUES ('cli-run','impulse-a','demo','{}','{}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')")
    relation_db.execute("INSERT INTO impulses (run_id,id,impulse_type,payload,metadata,created_at,updated_at) VALUES ('cli-run','impulse-b','demo','{}','{}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')")
    relation_db.execute("INSERT INTO impulses (run_id,id,impulse_type,payload,metadata,created_at,updated_at) VALUES ('cli-run','impulse-c','demo','{}','{}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')")
    relation_db.execute("INSERT INTO impulses (run_id,id,impulse_type,payload,metadata,created_at,updated_at) VALUES ('cli-run','impulse-d','demo','{}','{}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')")
    relation_db.execute("INSERT INTO impulse_relations (run_id,id,relation_type,source_impulse_id,target_impulse_id,metadata,created_at) VALUES ('cli-run','rel-source','derived','impulse-a','impulse-b','{}','2026-01-01T00:00:00Z')")
    relation_db.execute("INSERT INTO impulse_relations (run_id,id,relation_type,source_impulse_id,target_impulse_id,metadata,created_at) VALUES ('cli-run','rel-target','derived','impulse-c','impulse-a','{}','2026-01-01T00:00:01Z')")
    relation_db.execute("INSERT INTO impulse_relations (run_id,id,relation_type,source_impulse_id,target_impulse_id,metadata,created_at) VALUES ('cli-run','rel-other','derived','impulse-c','impulse-d','{}','2026-01-01T00:00:02Z')")
    relation_db.execute("INSERT INTO impulse_relations (run_id,id,relation_type,source_impulse_id,target_impulse_id,metadata,created_at) VALUES ('cli-run','rel-causal','causal','impulse-a','impulse-d','{}','2026-01-01T00:00:03Z')")
    relation_db.execute("INSERT INTO impulse_types (run_id,id,title,description,media_types,value_schema_json,metadata,created_at,updated_at) VALUES ('cli-run','type-demo','Demo title',NULL,'[\\\"text/plain\\\"]','{}','{}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')")
    relation_db.execute("INSERT INTO associations (run_id,id,kind,impulse_id,values_json,metadata,created_at) VALUES ('cli-run','assoc-demo','accepted','impulse-a','[{\"value\":1}]','{}','2026-01-01T00:00:00Z')")
    relation_db.execute("INSERT INTO reactions (run_id,id,kind,uri,impulse_id,media_type,size_bytes,content_hash,metadata,created_at) VALUES ('cli-run','reaction-demo','stored','memory://reaction-demo','impulse-a','text/plain',4,'hash-demo','{}','2026-01-01T00:00:00Z')")
    relation_db.execute("INSERT INTO homeostats (run_id,id,kind,impulse_id,status,values_json,metadata,attempt,max_attempts,created_at,updated_at) VALUES ('cli-run','homeostat-demo','manual','impulse-a','open','{}','{}',0,2,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')")
    relation_db.close()
    var relation_filter = dispatch_native_command("impulse-relations list --db " + path + " --run-id cli-run --impulse-id impulse-a")
    _check(_has(relation_filter, "rel-source") and _has(relation_filter, "rel-target") and not _has(relation_filter, "rel-other"), "impulse relation OR filtering")
    var relation_type_filter = dispatch_native_command("impulse-relations list --db " + path + " --run-id cli-run --relation-type causal")
    _check(_has(relation_type_filter, "rel-causal") and not _has(relation_type_filter, "rel-source") and not _has(relation_type_filter, "rel-target"), "relation type filtering")
    var inspected_impulse = dispatch_native_command("impulses inspect --db " + path + " --run-id cli-run --impulse-id impulse-a")
    _check(_has(inspected_impulse, "\"ok\":true") and _has(inspected_impulse, "\"impulse_type\":\"demo\"") and _has(inspected_impulse, "\"id\":\"impulse-a\""), "impulse inspection")
    var missing_impulse_id = dispatch_native_command("impulses inspect --db " + path + " --run-id cli-run")
    _check(_has(missing_impulse_id, "argument_error") and _has(missing_impulse_id, "--impulse-id is required"), "impulse inspect requires id")
    var inspected_type = dispatch_native_command("impulse-types inspect --db " + path + " --run-id cli-run --impulse-type-id type-demo")
    _check(_has(inspected_type, "\"ok\":true") and _has(inspected_type, "\"title\":\"Demo title\"") and _has(inspected_type, "\"description\":null"), "impulse type inspection")
    var missing_type = dispatch_native_command("impulse-types inspect --db " + path + " --run-id cli-run --impulse-type-id missing-type")
    _check(_has(missing_type, "\"ok\":false") and _has(missing_type, "\"impulse_type\":null"), "missing impulse type inspection")
    var inspected_relation = dispatch_native_command("relations inspect --db " + path + " --run-id cli-run --relation-id rel-source")
    _check(_has(inspected_relation, "\"ok\":true") and _has(inspected_relation, "\"relation_type\":\"derived\"") and _has(inspected_relation, "\"source_impulse_id\":\"impulse-a\""), "relation inspection")
    var missing_relation = dispatch_native_command("relations inspect --db " + path + " --run-id cli-run --relation-id missing-relation")
    _check(_has(missing_relation, "\"ok\":false") and _has(missing_relation, "\"impulse_relation\":null"), "missing relation inspection")
    var missing_relation_id = dispatch_native_command("impulse-relations inspect --db " + path + " --run-id cli-run")
    _check(_has(missing_relation_id, "argument_error") and _has(missing_relation_id, "--relation-id is required"), "relation inspect requires id")
    var inspected_association = dispatch_native_command("associations inspect --db " + path + " --run-id cli-run --association-id assoc-demo")
    _check(_has(inspected_association, "\"ok\":true") and _has(inspected_association, "\"kind\":\"accepted\"") and _has(inspected_association, "\"values\":["), "association inspection")
    var inspected_reaction = dispatch_native_command("reactions inspect --db " + path + " --run-id cli-run --reaction-id reaction-demo")
    _check(_has(inspected_reaction, "\"ok\":true") and _has(inspected_reaction, "\"uri\":\"memory://reaction-demo\"") and _has(inspected_reaction, "\"size_bytes\":4"), "reaction inspection")
    var homeostat_listing = dispatch_native_command("homeostats list --db " + path + " --run-id cli-run")
    _check(_has(homeostat_listing, "\"resource\":\"homeostats\"") and _has(homeostat_listing, "homeostat-demo") and _has(homeostat_listing, "\"status\":\"open\""), "homeostat storage listing")
    var homeostat_filtered = dispatch_native_command("homeostats list --db " + path + " --run-id cli-run --status open --impulse-id impulse-a")
    _check(_has(homeostat_filtered, "\"count\":1") and _has(homeostat_filtered, "homeostat-demo"), "homeostat list filters")
    var homeostat_unknown = dispatch_native_command("homeostats list --db " + path + " --run-id cli-run --unknown")
    _check(_has(homeostat_unknown, "argument_error") and _has(homeostat_unknown, "--unknown"), "homeostat list unknown option rejection")
    var homeostat_inspect = dispatch_native_command("homeostats inspect --db " + path + " --run-id cli-run --homeostat-id homeostat-demo")
    _check(_has(homeostat_inspect, "unsupported_command"), "homeostat inspect remains unsupported")
    var homeostat_open_plural = dispatch_native_command("homeostats open --db " + path + " --run-id cli-run --homeostat-id homeostat-missing --process-id process-missing --metadata-json '{}'")
    var homeostat_open_singular = dispatch_native_command("homeostat open --db " + path + " --run-id cli-run --homeostat-id homeostat-missing --process-id process-missing --metadata-json '{}'")
    _check(homeostat_open_singular == homeostat_open_plural and _has(homeostat_open_plural, "argument_error") and _has(homeostat_open_plural, "--process-id"), "homeostat open aliases reject process option")
    var homeostat_complete_plural = dispatch_native_command("homeostats complete --db " + path + " --run-id cli-run --homeostat-id homeostat-missing --process-id process-missing --error-json '{}' --metadata-json '{}'")
    var homeostat_complete_singular = dispatch_native_command("homeostat complete --db " + path + " --run-id cli-run --homeostat-id homeostat-missing --process-id process-missing --error-json '{}' --metadata-json '{}'")
    _check(homeostat_complete_singular == homeostat_complete_plural and _has(homeostat_complete_plural, "argument_error") and _has(homeostat_complete_plural, "--process-id"), "homeostat complete aliases reject process option")
    var homeostat_cancel_plural = dispatch_native_command("homeostats cancel --db " + path + " --run-id cli-run --homeostat-id homeostat-missing --process-id process-missing --error-json '{}'")
    var homeostat_cancel_singular = dispatch_native_command("homeostat cancel --db " + path + " --run-id cli-run --homeostat-id homeostat-missing --process-id process-missing --error-json '{}'")
    _check(homeostat_cancel_singular == homeostat_cancel_plural and _has(homeostat_cancel_plural, "argument_error") and _has(homeostat_cancel_plural, "--process-id"), "homeostat cancel aliases reject process option")
    var homeostat_expire_plural = dispatch_native_command("homeostats expire --db " + path + " --run-id cli-run --homeostat-id homeostat-missing --process-id process-missing --error-json '{}'")
    var homeostat_expire_singular = dispatch_native_command("homeostat expire --db " + path + " --run-id cli-run --homeostat-id homeostat-missing --process-id process-missing --error-json '{}'")
    _check(homeostat_expire_singular == homeostat_expire_plural and _has(homeostat_expire_plural, "argument_error") and _has(homeostat_expire_plural, "--process-id"), "homeostat expire aliases reject process option")
    var homeostat_clock_boundary = dispatch_native_command("homeostat open --db " + path + " --run-id cli-run --homeostat-id homeostat-clock --kind manual --values-json '{}' --metadata-json '{}'")
    _check(_has(homeostat_clock_boundary, "native_boundary") and _has(homeostat_clock_boundary, "native clock source") and not _has(homeostat_clock_boundary, "--now is required"), "domain homeostat owns timestamp boundary")
    var homeostat_missing_id = dispatch_native_command("homeostat complete --db " + path + " --run-id cli-run")
    _check(_has(homeostat_missing_id, "argument_error") and _has(homeostat_missing_id, "--homeostat-id is required"), "domain homeostat transition requires id")
    var homeostat_bad_value = dispatch_native_command("homeostat complete --db " + path + " --run-id cli-run --homeostat-id homeostat-demo --value malformed")
    _check(_has(homeostat_bad_value, "Invalid value 'malformed'; expected key=value"), "domain homeostat value format")
    var homeostat_bad_values_json = dispatch_native_command("homeostat open --db " + path + " --run-id cli-run --homeostat-id homeostat-json --kind manual --values-json [] --metadata-json '{}'")
    _check(_has(homeostat_bad_values_json, "invalid_json"), "domain homeostat values must be object JSON")
    var homeostat_bad_metadata_json = dispatch_native_command("homeostat open --db " + path + " --run-id cli-run --homeostat-id homeostat-json --kind manual --values-json '{}' --metadata-json []")
    _check(_has(homeostat_bad_metadata_json, "invalid_json"), "domain homeostat metadata must be object JSON")
    var homeostat_reopen = dispatch_native_command("homeostat reopen --db " + path + " --run-id cli-run --homeostat-id homeostat-demo")
    _check(_has(homeostat_reopen, "unsupported_command"), "reference homeostat reopen is unsupported")
    var homeostat_repeat_values = dispatch_native_command("homeostat complete --db " + path + " --run-id cli-run --homeostat-id homeostat-demo --value score=1 --value score=2 --value note=done")
    var homeostat_values_json = _homeostat_domain_values("homeostat complete --db " + path + " --run-id cli-run --homeostat-id homeostat-demo --value score=1 --value score=2 --value note=done")
    _check(_has(homeostat_values_json, "\"score\":\"2\"") and not _has(homeostat_values_json, "\"score\":\"1\"") and _has(homeostat_values_json, "\"note\":\"done\""), "domain homeostat duplicate values overwrite earlier key")
    _check(_has(homeostat_repeat_values, "native_boundary") and not _has(homeostat_repeat_values, "Invalid value"), "domain homeostat repeated values")
    var missing_homeostat_db = dispatch_native_command("homeostat open --db")
    _check(_has(missing_homeostat_db, "argument_error: missing value for --db"), "homeostat missing db value")
    var homeostat_unknown_option = dispatch_native_command("homeostat open --db " + path + " --run-id cli-run --homeostat-id homeostat-unknown --kind manual --unknown")
    _check(_has(homeostat_unknown_option, "argument_error") and _has(homeostat_unknown_option, "unknown argument --unknown"), "homeostat unknown option")
    var created_pool = dispatch_native_command("runtimes create-pool --db " + path + " --pool-id pool-cli --runtime-json '{\"id\":\"runtime-cli\",\"metadata\":{}}'")
    _check(_has(created_pool, "unsupported_command"), "runtime pool create is removed from product surface")
    var created_policy = dispatch_native_command("runtimes add-policy --db " + path + " --pool-id pool-cli --policy-id policy-cli")
    _check(_has(created_policy, "unsupported_command"), "runtime policy create is removed from product surface")
    var inspected_runtime = dispatch_native_command("runtimes inspect --db " + path + " --pool-id pool-demo")
    _check(_has(inspected_runtime, "unsupported_command"), "runtime pool inspect is removed from product surface")
    var started = dispatch_native_command(
        "runs start --db " + path + " --run-id cli-run --now 2026-01-01T00:00:01Z"
    )
    _check(_has(started, "\"status\":\"active\""), "run start")

    var waiting = dispatch_native_command(
        "runs wait --db " + path + " --run-id cli-run --now 2026-01-01T00:00:02Z"
    )
    _check(_has(waiting, "\"status\":\"waiting\""), "run wait")

    var completed = dispatch_native_command(
        "runs complete --db " + path + " --run-id cli-run --now 2026-01-01T00:00:03Z"
    )
    _check(_has(completed, "\"status\":\"completed\""), "run complete")
    var cancel_created = dispatch_native_command(
        "create-run --db " + path + " --run-id cli-cancel --now 2026-01-01T00:00:04Z"
    )
    _check(_has(cancel_created, "\"status\":\"created\"") and _has(cancel_created, "cli-cancel"), "cancellation run creation")
    var cancel_requested = dispatch_native_command(
        "runs cancel --db " + path + " --run-id cli-cancel --reason 'operator requested' --now 2026-01-01T00:00:05Z"
    )
    _check(_has(cancel_requested, "\"status\":\"cancel_requested\""), "runs cancel requests cancellation")
    var cancel_runs = dispatch_native_command("runs list --db " + path + " --run-id cli-cancel")
    _check(_has(cancel_runs, "\"status\":\"cancel_requested\"") and not _has(cancel_runs, "\"status\":\"cancelled\""), "runs cancel persists requested status")
    var cancel_events = dispatch_native_command("events list --db " + path + " --run-id cli-cancel")
    _check(_has(cancel_events, "run.cancel_requested") and _has(cancel_events, "operator requested") and _has(cancel_events, "\"actor\":\"cli:user\""), "run cancellation event actor and reason")
    var cancel_commands = dispatch_native_command("commands list --db " + path + " --run-id cli-cancel")
    _check(_has(cancel_commands, "run.cancel") and _has(cancel_commands, "operator requested") and _has(cancel_commands, "\"actor\":\"cli:user\""), "run cancellation command actor and reason")
    var cancel_null_created = dispatch_native_command("create-run --db " + path + " --run-id cli-cancel-null --now 2026-01-01T00:00:06Z")
    _check(_has(cancel_null_created, "\"status\":\"created\""), "nullable cancellation run creation")
    var cancel_null = dispatch_native_command("runs cancel --db " + path + " --run-id cli-cancel-null --now 2026-01-01T00:00:07Z")
    _check(_has(cancel_null, "\"status\":\"cancel_requested\""), "nullable cancellation request")
    var cancel_null_events = dispatch_native_command("events list --db " + path + " --run-id cli-cancel-null")
    _check(_has(cancel_null_events, "run.cancel_requested") and _has(cancel_null_events, "\"reason\":null"), "nullable cancellation event reason")
    var cancel_null_commands = dispatch_native_command("commands list --db " + path + " --run-id cli-cancel-null")
    _check(_has(cancel_null_commands, "run.cancel") and _has(cancel_null_commands, "\"reason\":null"), "nullable cancellation command reason")
 
    var listed = dispatch_native_command("runs list --db " + path)
    _check(_has(listed, "\"resource\":\"runs\"") and _has(listed, "\"count\":") and _has(listed, "cli-run"), "run listing count envelope")
    var completed_runs = dispatch_native_command("runs list --db " + path + " --status completed")
    _check(_has(completed_runs, "\"resource\":\"runs\"") and _has(completed_runs, "cli-run"), "run status filtering")
    var run_id_runs = dispatch_native_command("runs list --db " + path + " --run-id cli-run")
    _check(_has(run_id_runs, "cli-run") and not _has(run_id_runs, "cli-spaced"), "run id filtering")
    var limited_runs = dispatch_native_command("runs list --db " + path + " --limit 1")
    _check(_has(limited_runs, "cli-control") and not _has(limited_runs, "cli-run") and not _has(limited_runs, "cli-spaced"), "run limit filtering")
    var commands = dispatch_native_command("commands list --db " + path + " --run-id cli-run")
    _check(_has(commands, "\"resource\":\"commands\"") and _has(commands, "run.start"), "command listing")
    var inspected_command = dispatch_native_command("commands inspect --db " + path + " --run-id cli-run --command-id run.start")
    _check(_has(inspected_command, "\"ok\":true") and _has(inspected_command, "\"command_type\":\"run.start\"") and _has(inspected_command, "\"id\":\"run.start\"") and _has(inspected_command, "\"payload\":{\"reason\":\"\",\"target\":\"active\"}"), "command inspection")
    var missing_command = dispatch_native_command("commands inspect --db " + path + " --run-id cli-run --command-id missing-command")
    _check(_has(missing_command, "\"ok\":false") and _has(missing_command, "\"command\":null"), "missing command inspection")
    var filtered_commands = dispatch_native_command("commands list --db " + path + " --run-id cli-run --command-type run.start")
    _check(_has(filtered_commands, "\"count\":1") and _has(filtered_commands, "run.start") and not _has(filtered_commands, "run.completed"), "command type filtering")
    var processes = dispatch_native_command("processes list --db " + path + " --run-id cli-run")
    var actor_db = Connection(path)
    initialize_native_schema(actor_db)
    actor_db.execute("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,payload,created_at) VALUES ('cli-run','actor-cmd','actor.test','actor-key','alice','{}','2026-01-01T00:00:05Z')")
    actor_db.execute("INSERT INTO runtime_events (run_id,sequence,id,event_type,schema_version,actor,payload,created_at) VALUES ('cli-run',5,'actor-event','actor.tested',1,'alice','{}','2026-01-01T00:00:05Z')")
    actor_db.execute("INSERT INTO processes (run_id,id,process_type,impulse_id,status,priority,attempt,max_attempts,available_at,lease_owner,lease_expires_at,input_json,output_json,error_json,metadata,created_at,updated_at,started_at,finished_at,output_schema_json) VALUES ('cli-run','process-a','demo',NULL,'pending',1,0,3,'2026-01-01T00:00:05Z',NULL,NULL,'{}','{}','{}','{}','2026-01-01T00:00:05Z','2026-01-01T00:00:05Z',NULL,NULL,'{}')")
    actor_db.close()
    var actor_command_filter = dispatch_native_command("commands list --db " + path + " --run-id cli-run --actor alice")
    _check(_has(actor_command_filter, "actor-cmd") and not _has(actor_command_filter, "run.start"), "command actor filtering")
    var actor_event_filter = dispatch_native_command("events list --db " + path + " --run-id cli-run --actor alice")
    _check(_has(actor_event_filter, "actor-event") and not _has(actor_event_filter, "run.start"), "event actor filtering")
    var schema_db = Connection(path)
    initialize_native_schema(schema_db)
    schema_db.execute("INSERT INTO runtime_events (run_id,sequence,id,event_type,schema_version,payload,created_at) VALUES ('cli-run',6,'schema-v2','schema.future',2,'{}','2026-01-01T00:00:06Z')")
    schema_db.close()
    var event_schema = dispatch_native_command("events validate-schema --db " + path + " --run-id cli-run")
    _check(_has(event_schema, "\"ok\":false") and _has(event_schema, "\"event_count\":6") and _has(event_schema, "schema-v2") and _has(event_schema, "\"schema_versions\":{\"1\":5,\"2\":1}"), "event schema validation report")
    var event_schema_ok = dispatch_native_command("events validate-schema --db " + path + " --run-id cli-run --max-schema-version 2")
    _check(_has(event_schema_ok, "\"ok\":true") and _has(event_schema_ok, "\"unsupported_events\":[]"), "event schema max version")
    var event_schema_invalid = dispatch_native_command("events validate-schema --db " + path + " --run-id cli-run --max-schema-version 0")
    _check(_has(event_schema_invalid, "argument_error") and _has(event_schema_invalid, "greater than zero"), "event schema max validation")
    _check(_has(processes, "\"resource\":\"processes\"") and _has(processes, "\"items\":["), "process listing")
    var inspected_process = dispatch_native_command("processes inspect --db " + path + " --run-id cli-run --process-id process-a")
    _check(_has(inspected_process, "\"ok\":true") and _has(inspected_process, "\"run_id\":\"cli-run\"") and _has(inspected_process, "\"id\":\"process-a\"") and _has(inspected_process, "\"process_type\":\"demo\"") and _has(inspected_process, "\"status\":\"pending\""), "process inspection")
    var missing_process = dispatch_native_command("processes inspect --db " + path + " --run-id cli-run --process-id missing-process")
    _check(_has(missing_process, "\"ok\":false") and _has(missing_process, "\"process\":null"), "missing process inspection")
    var missing_process_id = dispatch_native_command("processes inspect --db " + path + " --run-id cli-run")
    _check(_has(missing_process_id, "argument_error") and _has(missing_process_id, "--process-id is required"), "process inspect requires id")
    var missing_process_run = dispatch_native_command("processes inspect --db " + path + " --process-id process-a")
    _check(_has(missing_process_run, "argument_error") and _has(missing_process_run, "--run-id is required"), "process inspect requires run id")

    var events = dispatch_native_command("events list --db " + path + " --run-id cli-run")
    _check(_has(events, "run.completed"), "event listing")
    var trace = dispatch_native_command("trace --db " + path + " --run-id cli-run")
    var missing_trace_run = dispatch_native_command("trace --db " + path)
    _check(_has(missing_trace_run, "argument_error") and _has(missing_trace_run, "--run-id is required"), "trace requires run id")
    var missing_trace = dispatch_native_command("trace --db " + path + " --run-id missing-trace-run")
    _check(_has(missing_trace, "\"ok\":true") and _has(missing_trace, "\"run\":null") and _has(missing_trace, "\"events\":[]") and _has(missing_trace, "\"counts\":{\"reactions\":0"), "trace missing run parity")
    _check(_has(trace, "\"ok\":true") and _has(trace, "\"trace\":") and _has(trace, "\"run_id\":\"cli-run\"") and _has(trace, "\"counts\":") and _has(trace, "\"timeline\":") and _has(trace, "\"events\":") and _has(trace, "\"impulses\":") and _has(trace, "\"impulse_relations\":") and _has(trace, "\"associations\":") and _has(trace, "\"reactions\":") and _has(trace, "\"processes\":") and _has(trace, "\"homeostats\":") and _has(trace, "\"projections\":") and _has(trace, "run.completed") and not _has(trace, "\"resource\":\"trace\""), "trace run filtering")
    var filtered_events = dispatch_native_command("events list --db " + path + " --run-id cli-run --after-sequence 1 --limit 1")
    _check(_has(filtered_events, "\"count\":1") and _has(filtered_events, "\"sequence\":2") and not _has(filtered_events, "\"sequence\":1"), "event after-sequence filtering")
    var projection_rebuild = dispatch_native_command("projections rebuild --db " + path + " --run-id cli-run --name run_summary --now 2026-01-01T00:00:04Z")
    _check(_has(projection_rebuild, "\"ok\":true") and _has(projection_rebuild, "\"resource\":\"projections\"") and _has(projection_rebuild, "run_summary"), "projection rebuild")
    var projection_missing_list = dispatch_native_command("projections list --db " + path)
    _check(_has(projection_missing_list, "argument_error") and _has(projection_missing_list, "--run-id is required"), "projection list requires run id")
    var projection_listing = dispatch_native_command("projections list --db " + path + " --run-id cli-run")
    _check(_has(projection_listing, "\"run_id\":\"cli-run\"") and _has(projection_listing, "\"stale\":false"), "projection listing fields")
    var projection_jsonl = dispatch_native_command("projections list --db " + path + " --run-id cli-run --jsonl")
    _check(_has(projection_jsonl, "\"run_id\":\"cli-run\"") and _has(projection_jsonl, "\"stale\":false") and not _has(projection_jsonl, "\"items\":"), "projection JSONL fields")
    var projection_missing_run = dispatch_native_command("projections rebuild --db " + path)
    _check(_has(projection_missing_run, "argument_error") and _has(projection_missing_run, "--run-id is required"), "projection rebuild requires run id")
    var maintenance_dry = dispatch_native_command("maintain-journal --db " + path + " --older-than-days 1 --dry-run --no-vacuum")
    _check(_has(maintenance_dry, "\"ok\":true") and _has(maintenance_dry, "\"resource\":\"maintenance\"") and _has(maintenance_dry, "\"dry_run\":true"), "maintenance dry-run envelope")
    _check(_has(maintenance_dry, "\"deleted_run_count\":0"), "maintenance dry-run no deletion")
    var maintenance_apply = dispatch_native_command("maintain-journal --db " + path + " --older-than-days 1 --delete --no-vacuum")
    _check(_has(maintenance_apply, "\"ok\":true") and _has(maintenance_apply, "\"dry_run\":false") and _has(maintenance_apply, "\"deleted_run_count\":1"), "maintenance apply envelope")
    var after_maintenance = dispatch_native_command("runs list --db " + path)
    _check(not _has(after_maintenance, "cli-run"), "maintenance persisted deletion")

    var doctor = dispatch_native_command("doctor " + path)
    _check(_has(doctor, "\"current\":true"), "doctor positional path")

    var unsafe = dispatch_native_command("doctor /tmp/fala-bad\0path")
    _check(_has(unsafe, "unsafe_path"), "unsafe path rejection")
    var invalid_option = dispatch_native_command("db init --db " + path + " --unknown")
    _check(_has(invalid_option, "argument_error") and _has(invalid_option, "--unknown"), "invalid option rejection")
    var missing_lifecycle_run = dispatch_native_command("runs start --db " + path + " --now 2026-01-01T00:00:05Z")
    _check(_has(missing_lifecycle_run, "argument_error") and _has(missing_lifecycle_run, "--run-id is required"), "lifecycle requires run id")
    var lifecycle_unknown = dispatch_native_command("runs start --db " + path + " --run-id cli-control --unknown")
    _check(_has(lifecycle_unknown, "argument_error") and _has(lifecycle_unknown, "--unknown"), "lifecycle unknown option rejection")
    var lifecycle_empty = dispatch_native_command("runs start --db " + path + " --run-id cli-control --now=")
    _check(_has(lifecycle_empty, "argument_error") and _has(lifecycle_empty, "missing value") and _has(lifecycle_empty, "--now"), "lifecycle empty equals rejection")
    var bridge_missing_now = dispatch_native_command("bridge deliver --db " + path + " --run-id cli-control --delivery-id delivery-1 --target-db target.sqlite")
    _check(_has(bridge_missing_now, "argument_error") and _has(bridge_missing_now, "--now is required"), "bridge delivery requires timestamp")
    var bridge_source_path = "/tmp/fala-native-cli-semantic-20260716-bridge-source.sqlite"
    var bridge_target_path = "/tmp/fala-native-cli-semantic-20260716-bridge-target.sqlite"
    _clean_bridge_path(bridge_source_path)
    _clean_bridge_path(bridge_target_path)
    var bridge_source = NativeDomainStore.open(bridge_source_path)
    bridge_source.initialize()
    _seed_bridge_run(bridge_source, "bridge-source")
    var bridge_delivery = _bridge_delivery()
    _ = bridge_source.put_bridge_delivery(bridge_delivery)
    bridge_source.close()
    var bridge_target = NativeDomainStore.open(bridge_target_path)
    bridge_target.initialize()
    _seed_bridge_run(bridge_target, "bridge-target")
    bridge_target.close()
    var bridge_command = "bridge deliver --db " + bridge_source_path + " --run-id bridge-source --delivery-id bridge-delivery --target-db " + bridge_target_path + " --idempotency-key bridge.deliver.smoke --import-idempotency-key bridge.import.smoke --now 2026-01-01T00:00:01Z"
    var bridge_fresh = dispatch_native_command(bridge_command)
    _check(_has(bridge_fresh, "\"ok\":true") and _has(bridge_fresh, "\"delivery_replayed\":false") and _has(bridge_fresh, "\"import_replayed\":false") and _has(bridge_fresh, "\"status\":\"delivered\"") and _has(bridge_fresh, "\"status\":\"imported\""), "fresh bridge delivery")
    var bridge_source_after = NativeDomainStore.open(bridge_source_path)
    bridge_source_after.initialize()
    var source_rows = bridge_source_after.list_outbox_records("bridge-source")
    _check(len(source_rows) == 1 and source_rows[0].status == "delivered" and source_rows[0].attempts == 1, "source durable delivery state")
    bridge_source_after.close()
    var bridge_target_after = NativeDomainStore.open(bridge_target_path)
    bridge_target_after.initialize()
    var target_rows = bridge_target_after.list_inbox_records("bridge-target")
    _check(len(target_rows) == 1 and target_rows[0].status == "imported" and target_rows[0].attempts == 1 and len(bridge_target_after.list_impulses("bridge-target")) == 1, "target durable import state")
    bridge_target_after.close()
    var bridge_replay = dispatch_native_command(bridge_command.replace("2026-01-01T00:00:01Z", "2026-01-01T00:00:02Z"))
    _check(_has(bridge_replay, "\"ok\":true") and _has(bridge_replay, "\"delivery_replayed\":true") and _has(bridge_replay, "\"import_replayed\":true"), "bridge delivery replay flags")
    var bridge_source_replay = NativeDomainStore.open(bridge_source_path)
    bridge_source_replay.initialize()
    var source_replay_rows = bridge_source_replay.list_outbox_records("bridge-source")
    _check(len(source_replay_rows) == 1 and source_replay_rows[0].attempts == 1, "source replay does not duplicate rows")
    bridge_source_replay.close()
    var bridge_target_replay = NativeDomainStore.open(bridge_target_path)
    bridge_target_replay.initialize()
    var target_replay_rows = bridge_target_replay.list_inbox_records("bridge-target")
    var target_replay_impulses = bridge_target_replay.list_impulses("bridge-target")
    _check(len(target_replay_rows) == 1 and len(target_replay_impulses) == 1, "target replay does not duplicate rows")
    bridge_target_replay.close()

    var runtime_unknown = dispatch_native_command("runtimes create-pool --db " + path + " --pool-id pool-invalid --runtime-json '{\"id\":\"runtime-invalid\"}' --unknown value")
    _check(_has(runtime_unknown, "unsupported_command"), "runtime pool commands remain unsupported")
    var unknown = dispatch_native_command("not-a-command")
    _check(_has(unknown, "unsupported_command"), "unknown command envelope")
    # Retain one deterministic machine-readable end marker for CI and callers.
    print("{\"ok\":true,\"runtime\":\"mojo\",\"scenario\":\"native_cli_semantics\",\"status\":\"passed\"}")
    print("cli-stage-done")

