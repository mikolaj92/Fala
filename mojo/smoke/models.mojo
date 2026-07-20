from fala.native_package import PackageManifest

def main() raises:
    var manifest = PackageManifest(id="demo", version="2", correlation_paths=[])
    print(manifest.id)
    print(manifest.version)
