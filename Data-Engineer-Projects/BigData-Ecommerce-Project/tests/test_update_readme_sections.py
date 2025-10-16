import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))
from scripts.update_readme_sections import (ONE_COMMAND_AND_CLEANUP,
                                            ROMANIA_EU_NOTES,
                                            patch_readme_text)


def test_inserts_after_publish_block():
    src = """# Title
**Publish sample events**
```bash
gcloud pubsub topics publish clicks --message='{}'
```

"""
    out = patch_readme_text(src)
    assert ONE_COMMAND_AND_CLEANUP.strip() in out


def test_replaces_old_region_block_and_no_dup_costs():
    src = """# T

## Romania region & GDPR notes

old stuff

## Costs & Cleanup

legacy stuff
"""
    out = patch_readme_text(src)
    assert "Romania/EU region notes" in out
    assert ROMANIA_EU_NOTES.strip() in out
    assert not re.search(r"^##\s+Costs\s*&\s*Cleanup\s*(?:\r?\n|$)", out, re.I | re.M)


def test_idempotent():
    src = """# T
**Publish sample events**

```bash
echo x
```

"""
    once = patch_readme_text(src)
    twice = patch_readme_text(once)
    assert once == twice
