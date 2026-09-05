from fala.fep import build_result, parse
from fala.sdk import load_manifest, write_result

request = parse(__import__("json").dumps(load_manifest()), "effector.request")
write_result(build_result(request, values={"text": request["input"]["text"]}))
