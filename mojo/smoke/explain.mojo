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
    Path(package_path).write_text("{\"id\":\"pkg\",\"correlation_paths\":[{\"id\":\"delivery\",\"effectors\":[{\"id\":\"coding\",\"adapter\":{\"kind\":\"manual_homeostat\"}},{\"id\":\"repair\",\"adapter\":{\"kind\":\"manual_homeostat\"},\"conduction\":[\"coding\"],\"when\":{\"upstream\":\"coding\",\"path\":\"route\",\"equals\":\"repair\"}},{\"id\":\"publish\",\"adapter\":{\"kind\":\"subprocess\",\"command\":[\"true\"]},\"conduction\":[\"repair\"]}],\"terminals\":[{\"id\":\"done\",\"source_effector\":\"publish\",\"status\":\"succeeded\",\"output_schema\":{\"type\":\"object\"}}]}]}")
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
    print("explain smoke ok")
