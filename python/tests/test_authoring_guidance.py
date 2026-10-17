"""Offline checks for recovered, loadable authoring guidance."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SKILL = ROOT / "skills/building-with-fala/SKILL.md"


def test_authoring_skill_frontmatter_and_checklist():
    content = SKILL.read_text(encoding="utf-8")
    header, body = content.removeprefix("---\n").split("\n---\n", 1)
    assert content.startswith("---\n")
    assert "name: building-with-fala" in header
    description = re.search(r"^description: (.+)$", header, re.MULTILINE).group(1)
    assert len(description) <= 60 and description.endswith(".")
    for field in ("version:", "author:", "license:", "platforms:", "metadata:"):
        assert field in header
    for heading in ("## When to Use", "## Procedure", "## Pitfalls", "## Verification"):
        assert heading in body
    assert "execution_id" in body and "child_path" in body
    assert "run_until_idle" in body and "embedded/library" in body


def test_authoring_links_and_prerequisites_exist():
    for path in (ROOT / "README.md", ROOT / "docs/AGENT_PROGRAMMING_MODEL.md", SKILL):
        content = path.read_text(encoding="utf-8")
        for target in re.findall(r"\]\(([^)]+)\)", content):
            if "://" not in target and not target.startswith("#"):
                assert (path.parent / target.split("#")[0]).exists(), target
    for target in re.findall(r"Read `([^`]+)`", SKILL.read_text(encoding="utf-8")):
        assert (ROOT / target).exists(), target


def test_readme_integrates_guidance_without_restoring_removed_agents_file():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "skills/building-with-fala/SKILL.md" in readme
    assert "docs/AGENT_PROGRAMMING_MODEL.md" in readme
    assert not (ROOT / "AGENTS.md").exists()
