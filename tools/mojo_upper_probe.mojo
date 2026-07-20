"""Smoke probe for Mojo's standard-library Unicode String.upper operation."""

def main() raises:
    var source = "straße 世界 café"
    print(source.upper())
