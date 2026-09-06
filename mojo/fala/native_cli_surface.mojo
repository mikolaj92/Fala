"""Python-free native CLI router; command implementations live in sibling modules."""
from fala.native_cli_help import cli_surface_help
from fala.native_cli_parse import (
    _word, _count, _flag, _has_option, _validate, _bool_option, _limit,
    _path, _require_db_value,
)
from fala.native_cli_inspect import (
    _events_validate_schema, _runs, _command_rows, _run_inspect, _run_observe,
    _diagnose_waits, _impulse_inspect, _command_inspect, _process_inspect,
    _domain_inspect, _rows, _trace, _schema_model, _schema_impulse,
    _graph, _explain, _bridge_rows,
)
from fala.native_cli_ops import (
    _gc, _maintain_journal, _projection_rebuild, _bridge_export, _bridge_import,
    _error, _bridge_deliver, _init, initialize_database, _db_status, _vacuum,
    _rehearse,
)
from fala.native_cli_lifecycle import (
    _create, _transition, _impulse_create, _process_schedule, _process_transition,
    _association_append, _reaction_record, _homeostat_transition, _homeostat_domain,
)


def dispatch_native_command(command: String) raises -> String:
    try:
        var first = _word(command, 0)
        var second = _word(command, 1)
        # Progressive disclosure: `ops <cmd>...` is an alias for operator tools.
        if first == "ops" and second != "":
            var rest = second
            var idx = 2
            while idx < _count(command):
                rest += " " + _word(command, idx)
                idx += 1
            return dispatch_native_command(rest)
        if first == "graph":
            if second != "expand" and second != "validate" and second != "fingerprint" and second != "diff": return _error("unsupported_command")
            _validate(command, "graph")
            return _graph(command, second)
        if first == "rehearse":
            _validate(command, "rehearse")
            return _rehearse(command)
        if first == "explain":
            _validate(command, "explain")
            return _explain(command)
        if first == "init": _validate(command, "init"); return _init(command)
        if first == "gc":
            _validate(command, "gc")
            return _gc(command)
        elif first == "archive-run" or first == "archive-gc": return _error("native_boundary", first + " requires native filesystem archive support")
        if first == "run-until-idle": return _error("native_boundary", "run-until-idle requires a native execution adapter registry")
        if first == "replay-execution": return _error("native_boundary", "replay-execution requires a native execution replay host")
        if first == "doctor" and (_has_option(command, "--package") or _has_option(command, "--output")): return _error("native_boundary", "doctor package/YAML and output paths require native filesystem support")
        if first == "schema": _validate(command, "schema", True)
        elif first == "db":
            _require_db_value(command, "db")
            if second != "status" and _has_option(command, "--ensure-schema"): return _error("argument_error", "--ensure-schema is supported only for db status")
            _validate(command, "db", True)
        elif first == "doctor":
            _require_db_value(command, "doctor")
            _validate(command, "doctor", True)
        elif first == "events" and second == "validate-schema": _validate(command, "event-schema")
        elif first == "projections" and second == "rebuild": _validate(command, "projection")
        elif first == "runs" and second == "list": _validate(command, "run-list")
        elif first == "runs" and second == "observe": _validate(command, "run-observe")
        elif first == "runs" and second == "start": _validate(command, "transition"); return _transition(command, "start")
        elif first == "runs" and second == "wait": _validate(command, "transition"); return _transition(command, "wait")
        elif first == "runs" and second == "complete": _validate(command, "transition"); return _transition(command, "complete")
        elif first == "runs" and second == "fail": _validate(command, "transition"); return _transition(command, "fail")
        elif first == "runs" and second == "request-cancel": _validate(command, "transition"); return _transition(command, "request_cancel")
        elif first == "runs" and second == "cancel": _validate(command, "transition"); return _transition(command, "cancel")
        elif first == "runs" and second == "timeout": _validate(command, "transition"); return _transition(command, "timeout")
        elif first == "homeostats" and second == "list": _validate(command, "homeostats-list")
        elif first == "homeostats" and second == "inspect": return _error("unsupported_command")
        if (first == "commands" or first == "events" or first == "processes" or first == "impulses" or first == "impulse-types" or first == "impulse-relations" or first == "relations" or first == "associations" or first == "reactions" or first == "projections" or first == "bridge" or first == "bridges" or first == "runs") and second == "inspect": _validate(command, "inspect")
        if command == "commands list" or command.startswith("commands list "): _validate(command, "commands-list"); return _command_rows(_path(command), command)
        elif first == "reactions" and second == "record": _validate(command, "reaction-record"); return _reaction_record(command)
        if command == "trace" or command.startswith("trace "): _validate(command, "trace"); return _trace(_path(command), command)
        if command == "diagnose-waits" or command.startswith("diagnose-waits "): _validate(command, "diagnose-waits"); return _diagnose_waits(_path(command), _flag(command, "--run-id"), _flag(command, "--impulse-id"))
        if first == "bridge" and second == "deliver": _validate(command, "bridge-deliver"); return _bridge_deliver(command)
        if first == "bridge" and (second == "export" or second == "import"): 
            if second == "export": _validate(command, "bridge-export"); return _bridge_export(command)
            _validate(command, "bridge-import"); return _bridge_import(command)
        if command == "commands inspect" or command.startswith("commands inspect "): return _command_inspect(_path(command), _flag(command, "--run-id"), _flag(command, "--command-id"))
        if command == "impulses inspect" or command.startswith("impulses inspect "): return _impulse_inspect(_path(command), _flag(command, "--run-id"), _flag(command, "--impulse-id"))
        elif first == "maintain-journal": _validate(command, "maintenance")
        if command == "schema impulse": return _schema_impulse()
        if command == "processes inspect" or command.startswith("processes inspect "): return _process_inspect(_path(command), _flag(command, "--run-id"), _flag(command, "--process-id"))
        if command == "impulse-types inspect" or command.startswith("impulse-types inspect "): return _domain_inspect(_path(command), "impulse_type", "impulse_types", _flag(command, "--run-id"), _flag(command, "--impulse-type-id"), "--impulse-type-id")
        if command == "impulse-relations inspect" or command.startswith("impulse-relations inspect ") or command == "relations inspect" or command.startswith("relations inspect "): return _domain_inspect(_path(command), "impulse_relation", "impulse_relations", _flag(command, "--run-id"), _flag(command, "--relation-id"), "--relation-id")
        if command == "reactions inspect" or command.startswith("reactions inspect "): return _domain_inspect(_path(command), "reaction", "reactions", _flag(command, "--run-id"), _flag(command, "--reaction-id"), "--reaction-id")
        if command == "associations inspect" or command.startswith("associations inspect "): return _domain_inspect(_path(command), "association", "associations", _flag(command, "--run-id"), _flag(command, "--association-id"), "--association-id")
        if command == "schema model" or command.startswith("schema model "): return _schema_model()
        if command == "schema fala-package" or command.startswith("schema fala-package "): return _error("native_boundary", "schema fala-package requires a native model schema encoder")
        if command == "db init" or command == "db migrate": return _error("argument_error", "--db is required")
        if command.startswith("db init "): return initialize_database(_path(command))
        if command.startswith("db migrate "): return initialize_database(_path(command))
        if command == "runs list" or command.startswith("runs list "): return _runs(_path(command), _flag(command,"--status"), _flag(command,"--run-id"), _limit(command), _bool_option(command, "--jsonl"))
        if command == "runs observe" or command.startswith("runs observe "):
            return _run_observe(_path(command), _flag(command, "--run-id"))
        if command == "runs inspect" or command.startswith("runs inspect "): _validate(command, "inspect"); return _run_inspect(_path(command), _flag(command, "--run-id"))
        if command == "db status" or command.startswith("db status "):
            return _db_status(command)
        if command == "db schema" or command.startswith("db schema "): return _schema_model()
        if command == "db vacuum" or command.startswith("db vacuum "): return _vacuum(_path(command))
        if command == "maintain-journal" or command.startswith("maintain-journal "): return _maintain_journal(command)
        if command == "create-run" or command.startswith("create-run "): _validate(command, "create"); return _create(command)
        if command == "runs create" or command.startswith("runs create ") or command == "run create" or command.startswith("run create "): return _error("legacy_alias", "use create-run")
        if command == "projections rebuild" or command.startswith("projections rebuild "): return _projection_rebuild(command)
        if command == "impulses create" or command.startswith("impulses create "): _validate(command, "impulse-create"); return _impulse_create(command)
        if command == "processes schedule" or command.startswith("processes schedule "): _validate(command, "process-schedule"); return _process_schedule(command)
        if command == "processes cancel" or command.startswith("processes cancel "): _validate(command, "process-transition"); return _process_transition(command, "cancel")
        if command == "processes timeout" or command.startswith("processes timeout "): _validate(command, "process-transition"); return _process_transition(command, "timeout")
        if command == "homeostats expire" or command.startswith("homeostats expire ") or command == "homeostat expire" or command.startswith("homeostat expire "): _validate(command, "homeostat-domain-transition"); return _homeostat_domain(command, "expire")
        if command == "homeostats reopen" or command.startswith("homeostats reopen ") or command == "homeostat reopen" or command.startswith("homeostat reopen "): _validate(command, "homeostat-transition"); return _homeostat_transition(command, "reopen")
        if command == "associations append" or command.startswith("associations append "): _validate(command, "association-append"); return _association_append(command)
        if command == "homeostats open" or command.startswith("homeostats open ") or command == "homeostat open" or command.startswith("homeostat open "): _validate(command, "homeostat-domain-open"); return _homeostat_domain(command, "open")
        if command == "homeostats complete" or command.startswith("homeostats complete ") or command == "homeostat complete" or command.startswith("homeostat complete "): _validate(command, "homeostat-domain-transition"); return _homeostat_domain(command, "complete")
        if command == "homeostats cancel" or command.startswith("homeostats cancel ") or command == "homeostat cancel" or command.startswith("homeostat cancel "): _validate(command, "homeostat-domain-transition"); return _homeostat_domain(command, "cancel")
        if command == "projections list" or command.startswith("projections list "): _validate(command, "projections-list"); return _rows(_path(command), "projections", "projections", command)
        if command == "commands list" or command.startswith("commands list "): _validate(command, "commands-list"); return _command_rows(_path(command), command)
        if command == "events list" or command.startswith("events list "): _validate(command, "events-list"); return _rows(_path(command), "events", "runtime_events", command)
        if command == "processes list" or command.startswith("processes list "): _validate(command, "processes-list"); return _rows(_path(command), "processes", "processes", command)
        if command == "impulses list" or command.startswith("impulses list "): _validate(command, "impulses-list"); return _rows(_path(command), "impulses", "impulses", command)
        if command == "events validate-schema" or command.startswith("events validate-schema "): return _events_validate_schema(_path(command), command)
        if command == "impulse-types list" or command.startswith("impulse-types list "): _validate(command, "impulse-types-list"); return _rows(_path(command), "impulse-types", "impulse_types", command)
        if command == "impulse-relations list" or command.startswith("impulse-relations list "): _validate(command, "impulse-relations-list"); return _rows(_path(command), "impulse-relations", "impulse_relations", command)
        if command == "relations list" or command.startswith("relations list "): _validate(command, "impulse-relations-list"); return _rows(_path(command), "relations", "impulse_relations", command)
        if command == "associations list" or command.startswith("associations list "): _validate(command, "associations-list"); return _rows(_path(command), "associations", "associations", command)
        if command == "reactions list" or command.startswith("reactions list "): _validate(command, "reactions-list"); return _rows(_path(command), "reactions", "reactions", command)
        if command == "homeostats list" or command.startswith("homeostats list "): _validate(command, "homeostats-list"); return _rows(_path(command), "homeostats", "homeostats", command)
        if command == "bridge list" or command.startswith("bridge list "):
            _validate(command, "bridge-list")
            return _bridge_rows(command, "bridge")
        if command == "bridges list" or command.startswith("bridges list "):
            _validate(command, "bridge-list")
            return _bridge_rows(command, "bridges")
        if command == "export" or command.startswith("export ") or command.startswith("export-html") or command.startswith("export-bundle"):
            return _error("native_boundary", "export requires a native file encoder")
        if command == "doctor" or command.startswith("doctor "):
            return _db_status(command)
        return _error("unsupported_command")
    except err:
        var detail = String(err)
        if detail.find("unsafe_path") >= 0: return _error("unsafe_path", "invalid database path")
        if detail.find("invalid_json") >= 0: return _error("invalid_json", "invalid JSON input")
        if detail.find("argument_error") >= 0 or detail.find("Invalid value '") >= 0: return _error("argument_error", detail)
        return _error("storage_error", "native command failed")
