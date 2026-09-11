from fala.sdk import input_values, load_manifest, output, write_result

request = load_manifest()
write_result(output(request, {"text": input_values(request)["text"]}))
