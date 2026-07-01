from __future__ import annotations

import json
import os
from pathlib import Path


manifest = json.loads(Path(os.environ["FALA_STEP_MANIFEST"]).read_text())
value = float(manifest["input"].get("value", 0))
if value >= 90:
    state = "critical"
    threshold = 90
elif value >= 70:
    state = "warning"
    threshold = 70
else:
    state = "normal"
    threshold = 70

output = Path(os.environ["FALA_STEP_OUTPUT_DIR"])
output.mkdir(parents=True, exist_ok=True)
(output / "result.json").write_text(
    json.dumps({"state": state, "threshold": threshold}),
    encoding="utf-8",
)
