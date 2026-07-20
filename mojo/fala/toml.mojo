"""Strict TOML frontend for native Fala package manifests.

This parser intentionally implements the manifest-safe TOML subset and fails
closed for values outside it. Supported syntax is documented by the grammar
implemented here: bare/basic/literal and dotted keys, tables, arrays of
 tables, comments, basic/literal strings, booleans, decimal/hex/bin/oct
integers, decimal floats, arrays, and inline tables. TOML date/time values,
multiline strings, and special float values are rejected because the package
schema has no native representation for them.

The result is an EmberJson Value and is normalized by the same JSON package
validator used for JSON manifests. No Python or YAML compatibility path is
used.
"""

from emberjson import Array, Object, Value, to_string
from fala.json import canonical_json_text


struct TomlParser(Movable):
    var text: String
    var path: String
    var index: Int
    var current: List[String]
    var tables: List[String]

    def __init__(out self, text: String, path: String):
        self.text = text.copy()
        self.path = path.copy()
        self.index = 0
        self.current = List[String]()
        self.tables = List[String]()

    def _error(self, message: String) raises:
        raise Error("toml.parse at " + self.path + ": " + message)

    def _eof(self) -> Bool:
        return self.index >= self.text.byte_length()

    def _char(self) -> String:
        if self._eof():
            return ""
        return String(self.text[byte=self.index])

    def _skip_spaces(mut self):
        while not self._eof() and (self._char() == " " or self._char() == "\t"):
            self.index += 1

    def _skip_comment(mut self) raises:
        if self._char() != "#":
            return
        while not self._eof() and self._char() != "\n":
            self.index += 1

    def _skip_blank_lines(mut self) raises:
        while True:
            self._skip_spaces()
            if self._char() == "#":
                self._skip_comment()
            if self._char() == "\n":
                self.index += 1
                continue
            return

    def _skip_value_space(mut self) raises:
        while True:
            self._skip_spaces()
            if self._char() == "#":
                self._skip_comment()
                if self._char() == "\n":
                    self.index += 1
                    continue
            if self._char() == "\n":
                self.index += 1
                continue
            return

    def _expect(mut self, token: String) raises:
        if self.text[byte=self.index:self.index + token.byte_length()] != token:
            self._error("expected '" + token + "'")
        self.index += token.byte_length()

    def _hex(self, ch: String) raises -> Int:
        if ch == "0": return 0
        if ch == "1": return 1
        if ch == "2": return 2
        if ch == "3": return 3
        if ch == "4": return 4
        if ch == "5": return 5
        if ch == "6": return 6
        if ch == "7": return 7
        if ch == "8": return 8
        if ch == "9": return 9
        if ch == "a" or ch == "A": return 10
        if ch == "b" or ch == "B": return 11
        if ch == "c" or ch == "C": return 12
        if ch == "d" or ch == "D": return 13
        if ch == "e" or ch == "E": return 14
        if ch == "f" or ch == "F": return 15
        return -1

    def _parse_basic_string(mut self) raises -> String:
        self._expect("\"")
        var json = String("\"")
        var segment = self.index
        while not self._eof():
            var ch = self._char()
            if ch == "\"":
                json += self.text[byte=segment:self.index]
                json += "\""
                self.index += 1
                try:
                    var parsed = Value(parse_string=json)
                    if not parsed.is_string():
                        self._error("invalid basic string")
                    return parsed.string().copy()
                except err:
                    self._error("invalid basic string escape")
            if ch == "\n" or ch == "\r":
                self._error("multiline strings are unsupported")
            if ch == "\\":
                json += self.text[byte=segment:self.index]
                self.index += 1
                if self._eof(): self._error("unterminated escape")
                var escaped = self._char()
                if escaped == "b": json += "\\b"
                elif escaped == "t": json += "\\t"
                elif escaped == "n": json += "\\n"
                elif escaped == "f": json += "\\f"
                elif escaped == "r": json += "\\r"
                elif escaped == "\"": json += "\\\""
                elif escaped == "\\": json += "\\\\"
                elif escaped == "u":
                    if self.index + 4 >= self.text.byte_length(): self._error("short unicode escape")
                    for offset in range(1, 5):
                        if self._hex(String(self.text[byte=self.index + offset])) < 0:
                            self._error("invalid unicode escape")
                    json += "\\u" + self.text[byte=self.index + 1:self.index + 5]
                    self.index += 4
                elif escaped == "U":
                    self._error("\\U unicode escapes are unsupported")
                else:
                    self._error("unsupported basic string escape")
                self.index += 1
                segment = self.index
                continue
            # EmberJson validates control characters in the reconstructed JSON string.
            pass
            self.index += 1
        self._error("unterminated basic string")
        return ""

    def _parse_literal_string(mut self) raises -> String:
        self._expect("'")
        var start = self.index
        while not self._eof():
            var ch = self._char()
            if ch == "'":
                var result = String(self.text[byte=start:self.index])
                self.index += 1
                return result
            if ch == "\n" or ch == "\r":
                self._error("multiline strings are unsupported")
            self.index += 1
        self._error("unterminated literal string")
        return ""

    def _parse_key_segment(mut self) raises -> String:
        self._skip_spaces()
        if self._char() == "\"": return self._parse_basic_string()
        if self._char() == "'": return self._parse_literal_string()
        var start = self.index
        while not self._eof():
            var ch = self._char()
            if ch == "." or ch == "=" or ch == "]" or ch == "," or ch == "#" or ch == " " or ch == "\t" or ch == "\n" or ch == "\r":
                break
            if not ((ch >= "a" and ch <= "z") or (ch >= "A" and ch <= "Z") or (ch >= "0" and ch <= "9") or ch == "_" or ch == "-"):
                self._error("unsupported character in bare key")
            self.index += 1
        if self.index == start:
            self._error("expected key")
        return String(self.text[byte=start:self.index])

    def _parse_key_path(mut self, closing: String = "") raises -> List[String]:
        var parts = List[String]()
        parts.append(self._parse_key_segment())
        while True:
            self._skip_spaces()
            if self._char() != ".": break
            self.index += 1
            parts.append(self._parse_key_segment())
        if closing != "":
            self._skip_spaces()
            if self._char() != closing:
                self._error("expected '" + closing + "' after table key")
        return parts^

    def _strip_underscores(self, token: String) raises -> String:
        if token.startswith("_") or token.endswith("_") or token.find("__") >= 0:
            self._error("invalid numeric underscore placement")
        var start = 0
        if token.startswith("+") or token.startswith("-"): start = 1
        if token.byte_length() > start + 2 and (token[byte=start + 1] == "x" or token[byte=start + 1] == "X" or token[byte=start + 1] == "o" or token[byte=start + 1] == "O" or token[byte=start + 1] == "b" or token[byte=start + 1] == "B") and token[byte=start + 2] == "_":
            self._error("invalid numeric underscore placement")
        var result = String("")
        for i in range(token.byte_length()):
            if String(token[byte=i]) != "_": result += String(token[byte=i])
        return result^

    def _parse_integer_base(self, token: String, base: Int) raises -> Value:
        var sign = 1
        var start = 0
        if token.startswith("+"):
            start = 1
        elif token.startswith("-"):
            sign = -1
            start = 1
        var prefix = 2
        if token.byte_length() <= start + prefix:
            self._error("invalid integer")
        var digits = token[byte=start + prefix:]
        var amount: UInt64 = 0
        for i in range(digits.byte_length()):
            var ch = String(digits[byte=i])
            var digit = self._hex(ch)
            if base == 2 and (ch != "0" and ch != "1"): digit = -1
            elif base == 8 and (digit < 0 or digit > 7): digit = -1
            if digit < 0 or digit >= base: self._error("invalid integer digit")
            if amount > (UInt64.MAX - UInt64(digit)) / UInt64(base): self._error("integer overflow")
            amount = amount * UInt64(base) + UInt64(digit)
        if sign < 0:
            if amount > UInt64(9223372036854775808): self._error("integer overflow")
            if amount == UInt64(9223372036854775808): return Value(Int64(-9223372036854775807) - 1)
            return Value(Int64(0) - Int64(amount))
        if amount <= UInt64(9223372036854775807): return Value(Int64(amount))
        return Value(amount)

    def _parse_number(self, token: String) raises -> Value:
        var clean = self._strip_underscores(token)
        var signless = clean
        if signless.startswith("+") or signless.startswith("-"): signless = String(signless[byte=1:])
        if signless.startswith("0x") or signless.startswith("0X"):
            return self._parse_integer_base(clean, 16)
        if signless.startswith("0o") or signless.startswith("0O"):
            return self._parse_integer_base(clean, 8)
        if signless.startswith("0b") or signless.startswith("0B"):
            return self._parse_integer_base(clean, 2)
        if clean.find(".") >= 0 or clean.find("e") >= 0 or clean.find("E") >= 0:
            try:
                var parsed = Value(parse_string=clean)
                if not parsed.is_float(): self._error("invalid decimal float")
                return parsed^
            except err:
                self._error("invalid decimal float")
        if clean.byte_length() > 1:
            var digits = signless
            if digits.startswith("0"): self._error("leading zero in decimal integer")
        try:
            var decimal = clean
            if decimal.startswith("+"): decimal = String(decimal[byte=1:])
            var parsed = Value(parse_string=decimal)
            if not parsed.is_int() and not parsed.is_uint(): self._error("invalid decimal integer")
            return parsed^
        except err:
            self._error("invalid decimal integer")
        return Value()

    def _parse_inline_key_value(mut self, mut object: Object) raises:
        var key = self._parse_key_path()
        self._skip_spaces()
        self._expect("=")
        self._skip_spaces()
        var value = self._parse_value()
        _put_object(object, key, value^, self.path)

    def _parse_array(mut self) raises -> Value:
        self._expect("[")
        var result = Array()
        self._skip_value_space()
        if self._char() == "]":
            self.index += 1
            return Value(result^)
        while True:
            var value = self._parse_value()
            result.append(value^)
            self._skip_value_space()
            if self._char() == ",":
                self.index += 1
                self._skip_value_space()
                if self._char() == "]":
                    self.index += 1
                    return Value(result^)
                continue
            if self._char() == "]":
                self.index += 1
                return Value(result^)
            self._error("expected ',' or ']' in array")
        return Value(result^)

    def _parse_inline_table(mut self) raises -> Value:
        self._expect("{")
        var result = Object()
        self._skip_spaces()
        if self._char() == "}":
            self.index += 1
            return Value(result^)
        while True:
            self._parse_inline_key_value(result)
            self._skip_spaces()
            if self._char() == ",":
                self.index += 1
                self._skip_spaces()
                if self._char() == "}":
                    self._error("trailing comma in inline table is unsupported")
                continue
            if self._char() == "}":
                self.index += 1
                return Value(result^)
            self._error("expected ',' or '}' in inline table")
        return Value(result^)

    def _parse_scalar_value(mut self) raises -> Value:
        self._skip_spaces()
        var ch = self._char()
        if self.text[byte=self.index:self.index + 3] == "\"\"\"" or self.text[byte=self.index:self.index + 3] == "'''":
            self._error("multiline strings are unsupported")
        if ch == "\"": return Value(self._parse_basic_string())
        if ch == "'": return Value(self._parse_literal_string())
        var start = self.index
        while not self._eof():
            ch = self._char()
            if ch == "," or ch == "]" or ch == "}" or ch == "#" or ch == " " or ch == "\t" or ch == "\n" or ch == "\r": break
            self.index += 1
        if self.index == start: self._error("expected value")
        var token = String(self.text[byte=start:self.index])
        if token.byte_length() == 10 and token[byte=4] == "-" and token[byte=7] == "-":
            self._error("date/time values are unsupported")
        if token == "true": return Value(True)
        if token == "false": return Value(False)
        if token == "inf" or token == "-inf" or token == "+inf" or token == "nan" or token == "+nan" or token == "-nan":
            self._error("special float values are unsupported")
        if token.find(":") >= 0: self._error("date/time values are unsupported")
        return self._parse_number(token)

    def _parse_value(mut self) raises -> Value:
        var frames = List[Value]()
        var kinds = List[Int]()
        var states = List[Int]()
        var pending = List[List[String]]()
        var first = True
        var after_comma = List[Bool]()
        while True:
            if first:
                first = False
                self._skip_spaces()
                var ch = self._char()
                if ch == "[":
                    self.index += 1
                    frames.append(Value(Array()))
                    kinds.append(1)
                    states.append(0)
                    pending.append(List[String]())
                    after_comma.append(False)
                elif ch == "{":
                    self.index += 1
                    frames.append(Value(Object()))
                    kinds.append(2)
                    states.append(0)
                    pending.append(List[String]())
                    after_comma.append(False)
                else:
                    return self._parse_scalar_value()
            if len(frames) == 0: self._error("empty container")
            var top_index = len(frames) - 1
            var top_kind = kinds[top_index]
            var top_state = states[top_index]
            if top_state == 0:
                if top_kind == 1:
                    self._skip_value_space()
                    if self._char() == "]":
                        self.index += 1
                        var completed = frames.pop()
                        _ = kinds.pop(); _ = states.pop(); _ = pending.pop(); _ = after_comma.pop()
                        if len(frames) == 0: return completed^
                        var parent_index = len(frames) - 1
                        var parent = frames[parent_index].copy()
                        if kinds[parent_index] == 1:
                            parent.array().append(completed^)
                        else:
                            _put_object(parent.object(), pending[parent_index], completed^, self.path)
                            pending[parent_index] = List[String]()
                            after_comma[parent_index] = False
                        frames[parent_index] = parent^
                        states[parent_index] = 1
                        continue
                    states[top_index] = 2
                    continue
                self._skip_spaces()
                if self._char() == "\n" or self._char() == "\r" or self._char() == "#":
                    self._error("expected ',' or '}' in inline table")
                if after_comma[top_index] and self._char() == "}":
                    self._error("trailing comma in inline table is unsupported")
                if self._char() == "}":
                    self.index += 1
                    var completed = frames.pop()
                    _ = kinds.pop(); _ = states.pop(); _ = pending.pop(); _ = after_comma.pop()
                    if len(frames) == 0: return completed^
                    var parent_index = len(frames) - 1
                    var parent = frames[parent_index].copy()
                    if kinds[parent_index] == 1:
                        parent.array().append(completed^)
                    else:
                        _put_object(parent.object(), pending[parent_index], completed^, self.path)
                        pending[parent_index] = List[String]()
                        after_comma[parent_index] = False
                    frames[parent_index] = parent^
                    states[parent_index] = 1
                    continue
                pending[top_index] = self._parse_key_path()
                self._skip_spaces(); self._expect("="); self._skip_spaces()
                after_comma[top_index] = False
                states[top_index] = 2
                continue
            if top_state == 2:
                self._skip_spaces()
                var ch = self._char()
                if ch == "[":
                    self.index += 1
                    frames.append(Value(Array()))
                    kinds.append(1); states.append(0); pending.append(List[String]()); after_comma.append(False)
                    continue
                if ch == "{":
                    self.index += 1
                    frames.append(Value(Object()))
                    kinds.append(2); states.append(0); pending.append(List[String]()); after_comma.append(False)
                    continue
                var scalar = self._parse_scalar_value()
                if top_kind == 1:
                    var parent = frames[top_index].copy()
                    parent.array().append(scalar^)
                    frames[top_index] = parent^
                else:
                    var parent = frames[top_index].copy()
                    _put_object(parent.object(), pending[top_index], scalar^, self.path)
                    pending[top_index] = List[String]()
                    after_comma[top_index] = False
                    frames[top_index] = parent^
                states[top_index] = 1
                continue
            if top_kind == 1:
                self._skip_value_space()
            else:
                self._skip_spaces()
            var delimiter = self._char()
            if delimiter == ",":
                self.index += 1
                states[top_index] = 0
                after_comma[top_index] = top_kind == 2
                continue
            if (top_kind == 1 and delimiter == "]") or (top_kind == 2 and delimiter == "}"):
                self.index += 1
                var completed = frames.pop()
                _ = kinds.pop(); _ = states.pop(); _ = pending.pop(); _ = after_comma.pop()
                if len(frames) == 0: return completed^
                var parent_index = len(frames) - 1
                var parent = frames[parent_index].copy()
                if kinds[parent_index] == 1:
                    parent.array().append(completed^)
                else:
                    _put_object(parent.object(), pending[parent_index], completed^, self.path)
                    pending[parent_index] = List[String]()
                frames[parent_index] = parent^
                states[parent_index] = 1
                continue
            if top_kind == 2:
                self._error("expected ',' or '}' in inline table")
            self._error("expected container delimiter")

    def _finish_statement(mut self) raises:
        self._skip_spaces()
        if self._char() == "#": self._skip_comment()
        if self._char() == "\n":
            self.index += 1
            return
        if not self._eof(): self._error("unexpected trailing input")

    def _table_path_text(self, parts: List[String]) -> String:
        var result = String("")
        var first = True
        for part in parts:
            if not first: result += "."
            result += part
            first = False
        return result

    def _parse_header(mut self, mut root: Value) raises:
        if self.text[byte=self.index:self.index + 2] == "[[":
            self.index += 2
            var parts = self._parse_key_path("]")
            self._expect("]]" )
            self._finish_statement()
            var item = Value(Object())
            _put_path(root, parts, item^, True, self.path)
            self.current = parts.copy()
            return
        self._expect("[")
        var parts = self._parse_key_path("]")
        self._expect("]")
        self._finish_statement()
        var name = self._table_path_text(parts)
        for prior in self.tables:
            if prior == name: self._error("table declared more than once: " + name)
        self.tables.append(name^)
        _ensure_path(root, parts, self.path)
        self.current = parts.copy()


    def parse(mut self) raises -> Value:
        var root = Value(Object())
        while True:
            self._skip_blank_lines()
            if self._eof(): break
            if self._char() == "[":
                self._parse_header(root)
                continue
            var key = self._parse_key_path()
            self._skip_spaces()
            self._expect("=")
            self._skip_spaces()
            var value = self._parse_value()
            var full = self.current.copy()
            for part in key: full.append(part.copy())
            _put_path(root, full, value^, False, self.path)
            self._finish_statement()
        return root^


def _put_object(mut object: Object, parts: List[String], var value: Value, path: String) raises:
    if len(parts) == 0: raise Error("toml.parse at " + path + ": empty key")
    var parents = List[Value]()
    var parent_keys = List[String]()
    var current = Value(object.copy())
    var last = len(parts) - 1
    for i in range(last):
        if not current.is_object(): raise Error("toml.parse at " + path + ": dotted key conflicts with scalar")
        var key = parts[i].copy()
        parents.append(current.copy())
        parent_keys.append(key.copy())
        var child = Value(Object())
        if key in current.object():
            child = current.object()[key].copy()
            if not child.is_object(): raise Error("toml.parse at " + path + ": dotted key conflicts with scalar")
        current = child^
    var final_key = parts[last].copy()
    if not current.is_object(): raise Error("toml.parse at " + path + ": dotted key conflicts with scalar")
    if final_key in current.object(): raise Error("toml.parse at " + path + ": duplicate key '" + final_key + "'")
    current[final_key] = value^
    for i in range(len(parents) - 1, -1, -1):
        var parent = parents[i].copy()
        parent[parent_keys[i]] = current^
        current = parent^
    object = current.object().copy()


def _put_path(mut node: Value, parts: List[String], var value: Value, append: Bool, path: String) raises:
    if len(parts) == 0: raise Error("toml.parse at " + path + ": empty table path")
    var parents = List[Value]()
    var parent_keys = List[String]()
    var parent_arrays = List[Bool]()
    var current = node.copy()
    var last = len(parts) - 1
    for i in range(last):
        if not current.is_object(): raise Error("toml.parse at " + path + ": table path is not an object")
        var key = parts[i].copy()
        var selected_array = False
        var child = Value(Object())
        if key in current.object():
            var existing = current.object()[key].copy()
            if existing.is_array():
                var array = existing.array().copy()
                if len(array) == 0: raise Error("toml.parse at " + path + ": empty table array")
                child = array[len(array) - 1].copy()
                selected_array = True
            elif not existing.is_object():
                raise Error("toml.parse at " + path + ": table path conflicts with scalar")
            else:
                child = existing^
        parents.append(current.copy())
        parent_keys.append(key.copy())
        parent_arrays.append(selected_array)
        current = child^
    if not current.is_object(): raise Error("toml.parse at " + path + ": table path is not an object")
    var final_key = parts[last].copy()
    if append:
        var array = Array()
        if final_key in current.object():
            var existing = current.object()[final_key].copy()
            if not existing.is_array(): raise Error("toml.parse at " + path + ": array-of-tables conflicts with scalar")
            array = existing.array().copy()
        array.append(value^)
        current[final_key] = Value(array^)
    else:
        if final_key in current.object(): raise Error("toml.parse at " + path + ": duplicate key '" + final_key + "'")
        current[final_key] = value^
    for i in range(len(parents) - 1, -1, -1):
        var parent = parents[i].copy()
        if parent_arrays[i]:
            var array = parent[parent_keys[i]].array().copy()
            array[len(array) - 1] = current^
            parent[parent_keys[i]] = Value(array^)
        else:
            parent[parent_keys[i]] = current^
        current = parent^
    node = current^


def _ensure_path(mut node: Value, parts: List[String], path: String) raises:
    if len(parts) == 0: return
    var parents = List[Value]()
    var parent_keys = List[String]()
    var parent_arrays = List[Bool]()
    var current = node.copy()
    for i in range(len(parts)):
        if not current.is_object(): raise Error("toml.parse at " + path + ": table path is not an object")
        var key = parts[i].copy()
        var selected_array = False
        var child = Value(Object())
        if key in current.object():
            var existing = current.object()[key].copy()
            if existing.is_array():
                var array = existing.array().copy()
                if len(array) == 0: raise Error("toml.parse at " + path + ": empty table array")
                child = array[len(array) - 1].copy()
                selected_array = True
            elif not existing.is_object():
                raise Error("toml.parse at " + path + ": table path conflicts with scalar")
            else:
                child = existing^
        parents.append(current.copy())
        parent_keys.append(key.copy())
        parent_arrays.append(selected_array)
        current = child^
    for i in range(len(parents) - 1, -1, -1):
        var parent = parents[i].copy()
        if parent_arrays[i]:
            var array = parent[parent_keys[i]].array().copy()
            array[len(array) - 1] = current^
            parent[parent_keys[i]] = Value(array^)
        else:
            parent[parent_keys[i]] = current^
        current = parent^
    node = current^


def parse_toml_value(text: String, path: String = "<memory>") raises -> Value:
    """Parse supported TOML into an EmberJson Value; reject unsupported syntax."""
    var parser = TomlParser(text, path)
    return parser.parse()


def parse_toml_json(text: String, path: String = "<memory>") raises -> String:
    """Parse TOML and return canonical JSON for the shared package validator."""
    var value = parse_toml_value(text, path)
    return canonical_json_text(to_string(value^))


def main():
    pass
