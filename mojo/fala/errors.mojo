"""Native validation errors for Fala runtime contracts."""

struct ValidationError:
    var code: String
    var path: String
    var message: String

    def __init__(out self, code: String, path: String, message: String):
        self.code = code
        self.path = path
        self.message = message

    def __str__(self) -> String:
        return self.message

    def is_ok(self) -> Bool:
        return self.code == ""

    def describe(self) -> String:
        return self.code + " at " + self.path + ": " + self.message
