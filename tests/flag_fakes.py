"""The API's feature flags, as the tests see them.

``music_safety`` reads each step's mode from its feature flag in the API.
Every test reads them from :data:`FLAGS` instead, emptied before each test
(``tests/conftest.py``), so a step whose flag a test does not set is off,
and no test reaches the API for its flags.
"""

from __future__ import annotations

#: Each flag's mode text by name, as ``music_safety._read_flags`` returns it.
FLAGS: dict[str, str | None] = {}
