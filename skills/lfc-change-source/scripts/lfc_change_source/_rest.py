"""lfc_rest (the tests repo's lfc-rest skill): installed (pip install its scripts), else beside this skill in a checkout.
Imported by the functions that call a workspace, so modules that don't (debezium, contract) load without it."""
from __future__ import annotations

import os
import sys

try:
    from lfc_rest import Rest, not_found, target_catalog  # noqa: F401
except ImportError:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "lfc-rest", "scripts"))
    from lfc_rest import Rest, not_found, target_catalog  # noqa: F401
