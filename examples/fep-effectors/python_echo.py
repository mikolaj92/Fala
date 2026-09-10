from fala.sdk import input_values, load_manifest, output, write_result

write_result(output(values={"text": input_values(load_manifest())["text"]}))
