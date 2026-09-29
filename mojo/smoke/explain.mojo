from std.pathlib import Path
from std.os import remove
from fala.sqlite import Connection
from fala.schema import initialize_native_schema
from fala.native_cli_surface import dispatch_native_command


def expect(value: Bool, message: String) raises:
    if not value: raise Error(message)


def main() raises:
    var db_path = "/tmp/fala-explain.sqlite"
    var package_path = "/tmp/fala-explain-package.json"
    try: remove(db_path)
    except: pass
    Path(package_path).write_text("{\"id\":\"pkg\",\"correlation_paths\":[{\"id\":\"delivery\",\"effectors\":[{\"id\":\"coding\",\"output_schema\":{\"type\":\"object\",\"required\":[\"ok\"],\"properties\":{\"ok\":{\"type\":\"boolean\"}}},\"adapter\":{\"kind\":\"manual_homeostat\"}},{\"id\":\"repair\",\"output_schema\":{\"type\":\"object\",\"required\":[\"ok\"],\"properties\":{\"ok\":{\"type\":\"boolean\"}}},\"adapter\":{\"kind\":\"manual_homeostat\"},\"conduction\":[\"coding\"],\"when\":{\"upstream\":\"coding\",\"path\":\"route\",\"equals\":\"repair\"}},{\"id\":\"publish\",\"output_schema\":{\"type\":\"object\",\"required\":[\"ok\"],\"properties\":{\"ok\":{\"type\":\"boolean\"}}},\"adapter\":{\"kind\":\"subprocess\",\"command\":[\"true\"]},\"conduction\":[\"repair\"]}],\"terminals\":[{\"id\":\"done\",\"source_effector\":\"publish\",\"status\":\"succeeded\",\"output_schema\":{\"type\":\"object\",\"required\":[\"ok\"],\"properties\":{\"ok\":{\"type\":\"boolean\"}}}}]}]}")
    var db = Connection(db_path); initialize_native_schema(db)
    db.execute("INSERT INTO runs (id,status,package_id,package_version,package_digest,correlation_path_id,correlation_path_digest,runtime_version,backend_version,schema_version,metadata,created_at,updated_at) VALUES ('r','active','pkg','2','x','delivery','y','z','sqlite',6,'{}','2026-01-01','2026-01-01')")
    db.execute("INSERT INTO processes (run_id,id,process_type,status,priority,attempt,max_attempts,available_at,input_json,output_json,error_json,metadata,created_at,updated_at,output_schema_json) VALUES ('r','r:delivery:coding','correlation','skipped',0,1,1,'2026-01-01','{\"secret\":\"must-not-leak\"}','{\"reason\":\"condition_not_met\"}','{}','{\"effector_id\":\"coding\",\"__correlation_conduction\":[],\"__correlation_when\":null}','2026-01-01','2026-01-01','{}')")
    db.execute("INSERT INTO processes (run_id,id,process_type,status,priority,attempt,max_attempts,available_at,input_json,output_json,error_json,metadata,created_at,updated_at,output_schema_json) VALUES ('r','r:delivery:repair','correlation','skipped',0,0,1,'2026-01-01','{}','{\"reason\":\"condition_not_met\"}','{}','{\"effector_id\":\"repair\",\"__correlation_conduction\":[\"coding\"],\"__correlation_when\":{\"upstream\":\"coding\",\"path\":\"route\",\"equals\":\"repair\"}}','2026-01-01','2026-01-01','{}')")
    db.execute("INSERT INTO processes (run_id,id,process_type,status,priority,attempt,max_attempts,available_at,input_json,output_json,error_json,metadata,created_at,updated_at,output_schema_json) VALUES ('r','r:delivery:publish','correlation','pending',0,0,1,'2026-01-01','{}','{}','{}','{\"effector_id\":\"publish\",\"__correlation_conduction\":[\"repair\"],\"__correlation_when\":null}','2026-01-01','2026-01-01','{}')")
    db.execute("INSERT INTO runtime_events (run_id,sequence,id,event_type,schema_version,process_id,payload,created_at) VALUES ('r',1,'event-skip','process.skipped',1,'r:delivery:repair','{}','2026-01-01')")
    db.close()
    var output = dispatch_native_command("explain --db " + db_path + " --package " + package_path + " --run-id r --process-id publish")
    expect(output.find("\"reason\":\"not_ready\"") >= 0 and output.find("repair") >= 0, "skipped upstream explains pending child")
    expect(output.find("must-not-leak") < 0, "payload and secrets stay redacted")
    var condition = dispatch_native_command("explain --db " + db_path + " --package " + package_path + " --run-id r --process-id repair")
    expect(condition.find("condition_not_met") >= 0 and condition.find("\"path\":\"route\"") >= 0 and condition.find("\"expected\":\"repair\"") >= 0 and condition.find("\"observed\":null") >= 0 and condition.find("event-skip") >= 0, "condition facts and events")
    expect(condition == dispatch_native_command("explain --db " + db_path + " --package " + package_path + " --run-id r --process-id repair"), "deterministic output")

    var missing_run = dispatch_native_command("explain --db " + db_path + " --package " + package_path + " --run-id missing")
    expect(missing_run.find("\"ok\":false") >= 0 and missing_run.find("\"reason\":\"not_materialized\"") >= 0, "unknown run is not_materialized")
    var missing_process = dispatch_native_command("explain --db " + db_path + " --package " + package_path + " --run-id r --process-id absent")
    expect(missing_process.find("\"ok\":false") >= 0 and missing_process.find("\"reason\":\"not_materialized\"") >= 0, "unknown process is not_materialized")
    var missing_terminal = dispatch_native_command("explain --db " + db_path + " --package " + package_path + " --run-id r --terminal absent")
    expect(missing_terminal.find("\"ok\":false") >= 0 and missing_terminal.find("\"reason\":\"not_declared\"") >= 0, "unknown terminal is not_declared")

    db = Connection(db_path)
    db.execute("INSERT INTO runs (id,status,package_id,package_version,package_digest,correlation_path_id,correlation_path_digest,runtime_version,backend_version,schema_version,metadata,created_at,updated_at) VALUES ('triage','active','pkg','2','x','delivery','y','z','sqlite',6,'{}','2026-01-01','2026-01-01')")
    db.execute("INSERT INTO processes (run_id,id,process_type,status,priority,attempt,max_attempts,available_at,input_json,output_json,error_json,metadata,created_at,updated_at,output_schema_json) VALUES ('triage','triage:delivery:coding','correlation','failed',0,1,1,'2026-01-01','{}','{}','{\"code\":\"fixture_failure\"}','{\"effector_id\":\"coding\",\"__correlation_conduction\":[],\"__correlation_when\":null}','2026-01-01','2026-01-01','{}')")
    db.execute("INSERT INTO processes (run_id,id,process_type,status,priority,attempt,max_attempts,available_at,input_json,output_json,error_json,metadata,created_at,updated_at,output_schema_json) VALUES ('triage','triage:delivery:repair','correlation','pending',0,0,1,'2026-01-01','{}','{}','{}','{\"effector_id\":\"repair\",\"__correlation_conduction\":[\"coding\"],\"__correlation_when\":{\"upstream\":\"coding\",\"path\":\"route\",\"equals\":\"repair\"}}','2026-01-01','2026-01-01','{}')")
    db.execute("INSERT INTO processes (run_id,id,process_type,status,priority,attempt,max_attempts,available_at,input_json,output_json,error_json,metadata,created_at,updated_at,output_schema_json) VALUES ('triage','triage:delivery:publish','correlation','waiting',0,0,1,'2026-01-01','{}','{}','{}','{\"effector_id\":\"publish\",\"__correlation_conduction\":[\"repair\"],\"__correlation_when\":null}','2026-01-01','2026-01-01','{}')")
    db.close()
    var upstream_failed = dispatch_native_command("explain --db " + db_path + " --package " + package_path + " --run-id triage --process-id repair")
    expect(upstream_failed.find("\"ok\":true") >= 0 and upstream_failed.find("\"reason\":\"upstream_failed\"") >= 0, "failed when-source is upstream_failed")
    var waiting = dispatch_native_command("explain --db " + db_path + " --package " + package_path + " --run-id triage --process-id publish")
    expect(waiting.find("\"ok\":true") >= 0 and waiting.find("\"reason\":\"waiting\"") >= 0, "waiting process reason")
    var terminal = dispatch_native_command("explain --db " + db_path + " --package " + package_path + " --run-id triage --process-id coding")
    expect(terminal.find("\"ok\":true") >= 0 and terminal.find("\"reason\":\"terminal\"") >= 0, "failed process is terminal")

    db = Connection(db_path)
    db.execute("INSERT INTO runs (id,status,package_id,package_version,package_digest,correlation_path_id,correlation_path_digest,runtime_version,backend_version,schema_version,metadata,created_at,updated_at) VALUES ('ready','active','pkg','2','x','delivery','y','z','sqlite',6,'{}','2026-01-01','2026-01-01')")
    db.execute("INSERT INTO processes (run_id,id,process_type,status,priority,attempt,max_attempts,available_at,input_json,output_json,error_json,metadata,created_at,updated_at,output_schema_json) VALUES ('ready','ready:delivery:coding','correlation','ready',0,0,1,'2026-01-01','{}','{}','{}','{\"effector_id\":\"coding\",\"__correlation_conduction\":[],\"__correlation_when\":null}','2026-01-01','2026-01-01','{}')")
    db.execute("INSERT INTO processes (run_id,id,process_type,status,priority,attempt,max_attempts,available_at,input_json,output_json,error_json,metadata,created_at,updated_at,output_schema_json) VALUES ('ready','ready:delivery:repair','correlation','running',0,1,1,'2026-01-01','{}','{}','{}','{\"effector_id\":\"repair\",\"__correlation_conduction\":[\"coding\"],\"__correlation_when\":{\"upstream\":\"coding\",\"path\":\"route\",\"equals\":\"repair\"}}','2026-01-01','2026-01-01','{}')")
    db.close()
    var ready = dispatch_native_command("explain --db " + db_path + " --package " + package_path + " --run-id ready --process-id coding")
    expect(ready.find("\"ok\":true") >= 0 and ready.find("\"reason\":\"ready\"") >= 0, "ready process reason")
    var running = dispatch_native_command("explain --db " + db_path + " --package " + package_path + " --run-id ready --process-id repair")
    expect(running.find("\"ok\":true") >= 0 and running.find("\"reason\":\"running\"") >= 0, "running process reason")
    var by_terminal = dispatch_native_command("explain --db " + db_path + " --package " + package_path + " --run-id r --terminal done")
    expect(by_terminal.find("\"ok\":true") >= 0 and by_terminal.find("publish") >= 0, "terminal filter selects source effector")
    print("explain smoke ok")
