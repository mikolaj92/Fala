from fala.json import parse_json, canonical_json_text

def main() raises:
    var parsed = parse_json("{\"b\":1,\"a\":2}")
    print(canonical_json_text(parsed.serialize()))
    var array = parse_json("[true,null,3.5]")
    print(canonical_json_text(array.serialize()))
