from fala.native_cli_surface import dispatch_native_command

def main() raises:
    print(dispatch_native_command("schema impulse"))
    print(dispatch_native_command("db init :memory:"))
    print(dispatch_native_command("runs list"))
    print(dispatch_native_command("unknown command"))
