from fala.domain_store import NativeDomainStore
from fala.ops_maintenance import (
    delete_run, run_retention, maintain_journal, collect_reaction_garbage,
)
from fala.ops_projections import rebuild_projection, rebuild_projections_with_command
from fala.domain import Impulse, ImpulseType, ImpulseRelation, Association, Reaction, Homeostat, Projection
from fala.journal import EventInput
from fala.reactions import FileReactionStore
from std.ffi import CStringSlice, c_int, external_call
from std.os import remove
from std.pathlib import Path


def _fresh_reaction_root() raises -> String:
    var template = "/tmp/fala-maintenance-reactions-smoke-XXXXXX\0"
    var c_template = CStringSlice(template)
    var fd = external_call["mkstemp", c_int](c_template.unsafe_ptr())
    if fd < 0:
        raise Error("domain store maintenance smoke: unable to create unique reaction root")
    _ = external_call["close", c_int](fd)
    var root = String(template[byte=0:template.byte_length() - 1])
    try:
        remove(root)
    except err:
        pass
    return root
def _remove_tree(path: Path) raises:
    if path.__fspath__() == "" or not path.exists():
        return
    if path.is_dir():
        for entry in path.listdir():
            _remove_tree(path / entry.name())
        var text = path.__fspath__() + "\0"
        var c_path = CStringSlice(text)
        var result = external_call["rmdir", c_int](c_path.unsafe_ptr())
        if result != 0:
            raise Error("domain store maintenance smoke: unable to remove reaction directory")
    else:
        remove(path)
    if path.exists():
        raise Error("domain store maintenance smoke: reaction cleanup left a path")


struct ReactionRootGuard:
    var root: String

    def __init__(out self) raises:
        self.root = _fresh_reaction_root()

    def cleanup(mut self) raises:
        if self.root != "":
            var path = Path(self.root)
            _remove_tree(path)
            if path.exists():
                raise Error("domain store maintenance smoke: reaction root still exists")
            self.root = ""

    def __del__(deinit self):
        if self.root != "":
            try:
                _remove_tree(Path(self.root))
            except err:
                pass

    def path(self) -> String:
        return self.root






def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("domain store maintenance smoke: " + message)


def _seed_run(mut store: NativeDomainStore, run_id: String, created_at: String) raises:
    var stmt = store.db.query("INSERT INTO runs (id,status,metadata,created_at,updated_at,schema_version) VALUES (?, 'completed', '{}', ?, ?, 6)")
    stmt.bind_text(1, run_id)
    stmt.bind_text(2, created_at)
    stmt.bind_text(3, created_at)
    _ = stmt.step()
    store.db.commit()


def main() raises:
    var store = NativeDomainStore.open(":memory:\0")
    store.initialize()
    _seed_run(store, "run-maintain", "2026-01-01T00:00:00Z")
    _seed_run(store, "run-keep", "2026-01-01T00:00:01Z")
    _seed_run(store, "run-offset", "2026-01-01T00:00:00+01:00")

    store.put_impulse(Impulse(
        id="impulse-maintain", run_id="run-maintain", impulse_type="case",
        payload="{\"value\":1}", created_at="2026-01-01T00:00:02Z",
    ))
    store.put_association(Association(
        id="association-maintain", run_id="run-maintain", kind="tag",
        impulse_id="impulse-maintain", created_at="2026-01-01T00:00:03Z",
    ))
    store.put_impulse(Impulse(
        id="impulse-keep", run_id="run-keep", impulse_type="case",
        created_at="2026-01-01T00:00:04Z",
    ))
    store.put_impulse_type(ImpulseType(id="type-null", run_id="run-keep", title="", description="", created_at="2026-01-01T00:00:04Z", updated_at="2026-01-01T00:00:04Z"))
    store.put_association(Association(id="association-null", run_id="run-keep", kind="tag", impulse_id="", created_at="2026-01-01T00:00:05Z"))
    store.put_reaction(Reaction(id="reaction-null", run_id="run-keep", kind="binary", uri="memory://null", impulse_id="", media_type="", size_bytes=-1, content_hash=""))
    store.put_homeostat(Homeostat(id="homeostat-null", run_id="run-keep", kind="manual", impulse_id="", status="open", attempt=1, max_attempts=3))
    var nullable_type = store.get_impulse_type("run-keep", "type-null")
    _check(nullable_type.title == "" and nullable_type.description == "", "typed nullable impulse type")
    var nullable_types = store.list_impulse_types("run-keep")
    _check(nullable_types[0].find("\"title\":null") >= 0 and nullable_types[0].find("\"description\":null") >= 0, "JSON nullable impulse type")
    var nullable_reaction = store.get_reaction("run-keep", "reaction-null")
    _check(nullable_reaction.impulse_id == "" and nullable_reaction.media_type == "" and nullable_reaction.size_bytes == -1 and nullable_reaction.content_hash == "", "typed nullable reaction")
    var nullable_reactions = store.list_reactions("run-keep")
    _check(nullable_reactions[0].find("\"size_bytes\":null") >= 0, "JSON nullable reaction size")
    var nullable_homeostat = store.get_homeostat("run-keep", "homeostat-null")
    _check(nullable_homeostat.impulse_id == "", "typed nullable homeostat")
    _check(nullable_homeostat.attempt == 1 and nullable_homeostat.max_attempts == 3, "typed homeostat attempts")
    var nullable_homeostats = store.list_homeostats("run-keep")
    _check(nullable_homeostats[0].find("\"impulse_id\":null") >= 0, "JSON nullable homeostat")
    _check(nullable_homeostats[0].find("\"attempt\":1") >= 0 and nullable_homeostats[0].find("\"max_attempts\":3") >= 0, "JSON homeostat attempts")
    var retention = run_retention(store, 
        "2026-01-02T00:00:00Z", dry_run=True, keep_run_ids=List[String]()
    )
    _check(retention.candidate_count == 3, "retention candidate count")
    _check(retention.deleted_run_count == 0 and not retention.runs[0].deleted, "retention dry run")
    var offset_retention = run_retention(store, 
        "2025-12-31T23:30:00Z", dry_run=True, keep_run_ids=List[String]()
    )
    _check(offset_retention.candidate_count == 1 and offset_retention.runs[0].run_id == "run-offset", "offset timestamp retention")
    var applied = run_retention(store, 
        "2025-12-31T23:30:00Z", dry_run=False, keep_run_ids=List[String]()
    )
    _check(applied.deleted_run_count == 1 and applied.runs[0].deleted, "retention deletion")
    var maintenance = maintain_journal(store, older_than_days=1.0, vacuum=True, dry_run=True)
    var first_projection = rebuild_projection(store, "run-keep", "run_summary", "2026-01-02T00:00:00Z")
    _check(first_projection.name == "run_summary", "run_summary projection name")
    _check(first_projection.data.find("\"impulse_count\":1") >= 0, "run_summary accounting")
    var event_stmt = store.db.query("INSERT INTO runtime_events (run_id,sequence,id,event_type,payload,created_at) VALUES (?,?,?,?,?,?)")
    event_stmt.bind_text(1, "run-keep"); event_stmt.bind_int(2, 1); event_stmt.bind_text(3, "event-maintain"); event_stmt.bind_text(4, "maintenance.test"); event_stmt.bind_text(5, "{}"); event_stmt.bind_text(6, "2026-01-02T00:00:01Z"); _ = event_stmt.step()
    var stale_projection = store.get_projection("run-keep", "run_summary")
    _check(stale_projection.stale, "projection staleness")
    var rebuilt_projection = rebuild_projection(store, "run-keep", "run_summary", "2026-01-02T00:00:02Z")
    _check(not rebuilt_projection.stale and rebuilt_projection.source_event_sequence == 1, "projection rebuild watermark")
    _check(maintenance.dry_run and maintenance.retention.candidate_count == 2, "maintenance retention plan")
    _check(not maintenance.vacuumed, "maintenance dry-run vacuum")
    # Typed CRUD filters, deterministic ordering, limits, and run isolation.
    store.put_impulse(Impulse(id="impulse-z", run_id="run-keep", impulse_type="case", created_at="2026-01-01T00:00:04Z"))
    store.put_impulse(Impulse(id="impulse-a", run_id="run-keep", impulse_type="other", created_at="2026-01-01T00:00:04Z"))
    store.put_impulse_type(ImpulseType(id="type-a", run_id="run-keep", title="A", created_at="2026-01-01T00:00:06Z"))
    store.put_impulse_relation(ImpulseRelation(id="relation-z", run_id="run-keep", relation_type="link", source_impulse_id="impulse-z", target_impulse_id="impulse-a", created_at="2026-01-01T00:00:07Z"))
    store.put_impulse_relation(ImpulseRelation(id="relation-a", run_id="run-keep", relation_type="other", source_impulse_id="impulse-a", target_impulse_id="impulse-z", created_at="2026-01-01T00:00:07Z"))
    store.put_association(Association(id="association-z", run_id="run-keep", kind="tag", impulse_id="impulse-z", created_at="2026-01-01T00:00:08Z"))
    store.put_association(Association(id="association-a", run_id="run-keep", kind="note", impulse_id="impulse-a", created_at="2026-01-01T00:00:08Z"))
    store.put_reaction(Reaction(id="reaction-z", run_id="run-keep", kind="text", uri="memory://z", impulse_id="impulse-z", created_at="2026-01-01T00:00:09Z"))
    store.put_reaction(Reaction(id="reaction-a", run_id="run-keep", kind="binary", uri="memory://a", impulse_id="impulse-a", created_at="2026-01-01T00:00:09Z"))
    var typed_impulses = store.list_impulse_records("run-keep", "", -1)
    _check(len(typed_impulses) == 3 and typed_impulses[0].id == "impulse-a" and typed_impulses[1].id == "impulse-keep", "impulse ordering and unlimited")
    _check(len(store.list_impulse_records("run-keep", "case", 0)) == 0 and len(store.list_impulse_records("run-keep", "case", 1)) == 1, "impulse filter and limits")
    var invalid_impulse_limit = False
    try:
        _ = store.list_impulse_records("run-keep", "", -2)
    except err:
        invalid_impulse_limit = True
    _check(invalid_impulse_limit, "invalid impulse limit")
    var typed_types = store.list_impulse_type_records("run-keep", -1)
    _check(len(typed_types) == 2 and typed_types[0].id == "type-a" and len(store.list_impulse_type_records("run-keep", 0)) == 0, "impulse type ordering and empty limit")
    var typed_relations = store.list_impulse_relation_records("run-keep", "impulse-a", "", -1)
    _check(len(typed_relations) == 2 and typed_relations[0].id == "relation-a" and len(store.list_impulse_relation_records("run-keep", "", "link", 1)) == 1, "relation filters and ordering")
    var typed_associations = store.list_association_records("run-keep", "impulse-a", "", -1)
    _check(len(typed_associations) == 1 and typed_associations[0].id == "association-a" and len(store.list_association_records("run-keep", "", "tag", 0)) == 0, "association filters and zero limit")
    var typed_reactions = store.list_reaction_records("run-keep", "impulse-a", "binary", -1)
    _check(len(typed_reactions) == 1 and typed_reactions[0].id == "reaction-a" and typed_reactions[0].size_bytes == -1, "reaction filter")
    var nullable_reaction_record = store.list_reaction_records("run-keep", "", "binary", -1)
    _check(len(nullable_reaction_record) == 2 and nullable_reaction_record[0].id == "reaction-null" and nullable_reaction_record[0].size_bytes == -1, "reaction nullable round trip")
    var invalid_record_limit = False
    try:
        _ = store.list_reaction_records("run-keep", "", "", -2)
    except err:
        invalid_record_limit = True
    _check(invalid_record_limit, "invalid record limit")
    _check(len(store.list_impulse_records("run-maintain", "", -1)) == 1, "cross-run isolation")
    var missing_type = False
    try:
        _ = store.get_impulse_type("run-keep", "missing-type")
    except err:
        missing_type = True
    var missing_relation = False
    try:
        _ = store.get_impulse_relation("run-keep", "missing-relation")
    except err:
        missing_relation = True
    var missing_association = False
    try:
        _ = store.get_association("run-keep", "missing-association")
    except err:
        missing_association = True
    var missing_reaction = False
    try:
        _ = store.get_reaction("run-keep", "missing-reaction")
    except err:
        missing_reaction = True
    _check(missing_type and missing_relation and missing_association and missing_reaction, "missing getter errors")

    # Every domain wrapper must roll back its row, command, and event batch on an invalid event.
    var bad_events = List[EventInput]()
    bad_events.append(EventInput("bad-event", "", "{}", "2026-01-01T00:00:10Z", "", "", 1, "", "", ""))
    var type_atomic_failed = False
    try:
        _ = store.register_impulse_type(ImpulseType(id="type-atomic", run_id="run-keep", title="atomic"), "cmd-type-atomic", "impulse_type.register", "key-type-atomic", "2026-01-01T00:00:10Z", bad_events)
    except err:
        type_atomic_failed = True
    _check(type_atomic_failed and len(store.list_impulse_type_records("run-keep", -1)) == 2, "impulse type atomic rollback")
    var relation_atomic_failed = False
    try:
        _ = store.record_impulse_relation(ImpulseRelation(id="relation-atomic", run_id="run-keep", relation_type="link", source_impulse_id="impulse-z", target_impulse_id="impulse-a"), "cmd-relation-atomic", "impulse_relation.record", "key-relation-atomic", "2026-01-01T00:00:11Z", bad_events)
    except err:
        relation_atomic_failed = True
    _check(relation_atomic_failed and len(store.list_impulse_relation_records("run-keep", "", "", -1)) == 2, "relation atomic rollback")
    var association_atomic_failed = False
    try:
        _ = store.record_association(Association(id="association-atomic", run_id="run-keep", kind="tag"), "cmd-association-atomic", "association.record", "key-association-atomic", "2026-01-01T00:00:12Z", bad_events)
    except err:
        association_atomic_failed = True
    _check(association_atomic_failed and len(store.list_association_records("run-keep", "", "", -1)) == 3, "association atomic rollback")
    var reaction_atomic_failed = False
    try:
        _ = store.record_reaction(Reaction(id="reaction-atomic", run_id="run-keep", kind="text", uri="memory://atomic"), "cmd-reaction-atomic", "reaction.record", "key-reaction-atomic", "2026-01-01T00:00:13Z", bad_events)
    except err:
        reaction_atomic_failed = True
    _check(reaction_atomic_failed and len(store.list_reaction_records("run-keep", "", "", -1)) == 3, "reaction atomic rollback")
    var command_count = store.db.query("SELECT COUNT(*) FROM runtime_commands WHERE run_id=?")
    command_count.bind_text(1, "run-keep")
    _check(command_count.step() and command_count.column_int(0) == 0, "atomic command rollback")
    command_count.close()

    # Replays are idempotent; changed payloads conflict.
    var replay_events = List[EventInput]()
    var first_type = store.register_impulse_type(ImpulseType(id="type-replay", run_id="run-keep", title="same"), "cmd-type-replay", "impulse_type.register", "key-type-replay", "2026-01-01T00:00:14Z", replay_events)
    var second_type = store.register_impulse_type(ImpulseType(id="type-replay", run_id="run-keep", title="same"), "cmd-type-replay-2", "impulse_type.register", "key-type-replay", "2026-01-01T00:00:14Z", replay_events)
    _check(not first_type.replayed and second_type.replayed and len(store.list_impulse_type_records("run-keep", -1)) == 3, "wrapper idempotent replay")
    var replay_conflict = False
    try:
        _ = store.register_impulse_type(ImpulseType(id="type-replay", run_id="run-keep", title="changed"), "cmd-type-replay-3", "impulse_type.register", "key-type-replay", "2026-01-01T00:00:14Z", replay_events)
    except err:
        replay_conflict = True
    _check(replay_conflict, "wrapper idempotency conflict")
    var relation_wrapper = ImpulseRelation(id="relation-wrapper", run_id="run-keep", relation_type="link", source_impulse_id="impulse-z", target_impulse_id="impulse-a")
    var relation_first = store.record_impulse_relation(relation_wrapper, "cmd-relation-wrapper", "impulse_relation.record", "key-relation-wrapper", "2026-01-01T00:00:15Z", replay_events)
    var relation_second = store.record_impulse_relation(relation_wrapper, "cmd-relation-wrapper-2", "impulse_relation.record", "key-relation-wrapper", "2026-01-01T00:00:15Z", replay_events)
    _check(not relation_first.replayed and relation_second.replayed, "relation idempotent replay")
    var relation_conflict = False
    try:
        _ = store.record_impulse_relation(ImpulseRelation(id="relation-wrapper", run_id="run-keep", relation_type="changed", source_impulse_id="impulse-z", target_impulse_id="impulse-a"), "cmd-relation-wrapper-3", "impulse_relation.record", "key-relation-wrapper", "2026-01-01T00:00:15Z", replay_events)
    except err:
        relation_conflict = True
    _check(relation_conflict, "relation idempotency conflict")
    var association_wrapper = Association(id="association-wrapper", run_id="run-keep", kind="tag")
    var association_first = store.record_association(association_wrapper, "cmd-association-wrapper", "association.record", "key-association-wrapper", "2026-01-01T00:00:16Z", replay_events)
    var association_second = store.record_association(association_wrapper, "cmd-association-wrapper-2", "association.record", "key-association-wrapper", "2026-01-01T00:00:16Z", replay_events)
    _check(not association_first.replayed and association_second.replayed, "association idempotent replay")
    var association_conflict = False
    try:
        _ = store.record_association(Association(id="association-wrapper", run_id="run-keep", kind="changed"), "cmd-association-wrapper-3", "association.record", "key-association-wrapper", "2026-01-01T00:00:16Z", replay_events)
    except err:
        association_conflict = True
    _check(association_conflict, "association idempotency conflict")
    var reaction_wrapper = Reaction(id="reaction-wrapper", run_id="run-keep", kind="text", uri="memory://wrapper")
    var reaction_first = store.record_reaction(reaction_wrapper, "cmd-reaction-wrapper", "reaction.record", "key-reaction-wrapper", "2026-01-01T00:00:17Z", replay_events)
    var reaction_second = store.record_reaction(reaction_wrapper, "cmd-reaction-wrapper-2", "reaction.record", "key-reaction-wrapper", "2026-01-01T00:00:17Z", replay_events)
    _check(not reaction_first.replayed and reaction_second.replayed, "reaction idempotent replay")
    var reaction_conflict = False
    try:
        _ = store.record_reaction(Reaction(id="reaction-wrapper", run_id="run-keep", kind="changed", uri="memory://wrapper"), "cmd-reaction-wrapper-3", "reaction.record", "key-reaction-wrapper", "2026-01-01T00:00:17Z", replay_events)
    except err:
        reaction_conflict = True
    _check(reaction_conflict, "reaction idempotency conflict")
    # Homeostat and projection atomic wrappers: persistence, replay, transitions, rebuild.
    var homeostat_wrapper = Homeostat(id="homeostat-wrapper", run_id="run-keep", kind="manual", status="open", max_attempts=2, created_at="2026-01-01T00:00:18Z", updated_at="2026-01-01T00:00:18Z")
    var homeostat_saved = store.save_homeostat(homeostat_wrapper, "cmd-homeostat-save", "homeostat.open", "key-homeostat-save", "2026-01-01T00:00:18Z", replay_events)
    _check(not homeostat_saved.replayed and store.get_homeostat("run-keep", "homeostat-wrapper").status == "open", "homeostat atomic save")
    var homeostat_replay = store.save_homeostat(homeostat_wrapper, "cmd-homeostat-save-2", "homeostat.open", "key-homeostat-save", "2026-01-01T00:00:18Z", replay_events)
    _check(homeostat_replay.replayed, "homeostat idempotent save")
    var homeostat_done = homeostat_wrapper.copy()
    homeostat_done.status = "completed"
    homeostat_done.updated_at = "2026-01-01T00:00:19Z"
    var transition = store.transition_homeostat(homeostat_done, "cmd-homeostat-complete", "homeostat.complete", "key-homeostat-complete", "2026-01-01T00:00:19Z", replay_events)
    _check(not transition.submission.replayed and transition.homeostat.status == "completed", "homeostat atomic transition")
    var transition_replay = store.transition_homeostat(homeostat_done, "cmd-homeostat-complete-2", "homeostat.complete", "key-homeostat-complete", "2026-01-01T00:00:19Z", replay_events)
    _check(transition_replay.submission.replayed and transition_replay.homeostat.status == "completed", "homeostat transition replay")
    var projection_wrapper = Projection(id="projection-wrapper:run-keep", run_id="run-keep", name="wrapper", version=1, data="{}", source_event_sequence=0, updated_at="2026-01-01T00:00:20Z")
    var projection_saved = store.save_projection(projection_wrapper, "cmd-projection-save", "projection.save", "key-projection-save", "2026-01-01T00:00:20Z", replay_events)
    _check(not projection_saved.replayed and store.get_projection("run-keep", "wrapper").name == "wrapper", "projection atomic save")
    var projection_replay = store.save_projection(projection_wrapper, "cmd-projection-save-2", "projection.save", "key-projection-save", "2026-01-01T00:00:20Z", replay_events)
    _check(projection_replay.replayed, "projection idempotent save")
    var rebuild = rebuild_projections_with_command(store, "run-keep", List[String](), "cmd-projection-rebuild", "projection.rebuild", "key-projection-rebuild", "2026-01-01T00:00:21Z", "2026-01-01T00:00:21Z", replay_events)
    _check(not rebuild.submission.replayed and len(rebuild.projections) == 1 and rebuild.projections[0].name == "run_summary", "projection atomic rebuild")
    var rebuild_replay = rebuild_projections_with_command(store, "run-keep", List[String](), "cmd-projection-rebuild-2", "projection.rebuild", "key-projection-rebuild", "2026-01-01T00:00:21Z", "2026-01-01T00:00:21Z", replay_events)
    _check(rebuild_replay.submission.replayed and len(rebuild_replay.projections) == 1, "projection rebuild replay")

    var deleted = delete_run(store, "run-maintain")
    _check(deleted.runs == 1 and deleted.impulses == 1 and deleted.associations == 1, "row counts")
    var deleted_run_rejected = False
    try:
        _ = store.list_impulses("run-maintain")
    except err:
        deleted_run_rejected = True
    _check(deleted_run_rejected, "deleted run rejected")

    var reaction_root_guard = ReactionRootGuard()
    var reaction_root = reaction_root_guard.path()
    var reaction_store = FileReactionStore(reaction_root)
    var referenced_blob = reaction_store.put_bytes("keep", "keep.txt")
    var orphan_blob = reaction_store.put_bytes("orphan", "orphan.txt")
    store.put_reaction(Reaction(
        id="reaction-keep", run_id="run-keep", kind="text", uri=referenced_blob.uri,
        size_bytes=referenced_blob.size_bytes, content_hash="sha256:" + referenced_blob.digest,
    ))
    var persisted_reactions = store.list_reaction_records("run-keep", "", "text", -1)
    _check(len(persisted_reactions) >= 1, "reaction persistence rows available")
    var gc_dry = maintain_journal(store, 
        older_than_days=1.0, keep_last=1, vacuum=False, dry_run=True,
        reaction_root=reaction_root,
    )
    _check(gc_dry.dry_run and gc_dry.reaction_gc.reaction_root == reaction_root, "maintenance GC root and dry-run")
    _check(gc_dry.reaction_gc.referenced_count == 1, "maintenance GC referenced count")
    _check(gc_dry.reaction_gc.bytes_reclaimed == 0, "maintenance GC dry-run reclaimed bytes")
    _check(gc_dry.reaction_gc.candidate_count == 1 and gc_dry.reaction_gc.kept_count == 1 and gc_dry.reaction_gc.candidates[0] == orphan_blob.digest, "maintenance GC candidates and counters")
    _check(reaction_store.resolve(orphan_blob.uri) != "", "dry-run preserves orphan")
    var gc_plan = maintain_journal(store, 
        older_than_days=1.0, keep_last=1, vacuum=False, dry_run=False,
        reaction_root=reaction_root,
    )
    _check(gc_plan.reaction_gc.deleted_count == 1 and gc_plan.reaction_gc.deleted[0] == orphan_blob.digest, "maintenance GC deletion")
    _check(gc_plan.reaction_gc.referenced_count == 1, "maintenance GC applied referenced count")
    _check(gc_plan.reaction_gc.bytes_reclaimed == orphan_blob.size_bytes, "maintenance GC applied reclaimed bytes")
    var remaining_digests = reaction_store.list_blob_digests()
    var orphan_remaining = False
    for digest in remaining_digests:
        if digest == orphan_blob.digest: orphan_remaining = True
    _check(not orphan_remaining, "maintenance GC removes orphan")
    _check(reaction_store.resolve(referenced_blob.uri) != "", "referenced reaction preserved")
    var missing_run_rejected = False
    try:
        _ = collect_reaction_garbage(store, reaction_root, "missing-run", True)
    except err:
        missing_run_rejected = True
    _check(missing_run_rejected, "reaction GC validates run")
    _seed_run(store, "run-vacuum", "2000-01-01T00:00:00Z")
    var applied_maintenance = maintain_journal(store, 
        older_than_days=1.0, keep_last=1, vacuum=True, dry_run=False
    )
    _check(not applied_maintenance.dry_run, "maintenance apply mode")
    _check(applied_maintenance.retention.deleted_run_count == 1, "maintenance keep-last deletion")
    _check(applied_maintenance.vacuumed, "maintenance vacuum")
    var vacuum_missing = False
    try:
        _ = delete_run(store, "run-vacuum")
    except err:
        vacuum_missing = True
    _check(vacuum_missing, "maintenance deleted run")
    var rejected = False
    try:
        _ = delete_run(store, "missing-run")
    except err:
        rejected = True
    _check(rejected, "unknown run rejected")
    reaction_root_guard.cleanup()
    store.close()
    print("domain store maintenance smoke ok")
