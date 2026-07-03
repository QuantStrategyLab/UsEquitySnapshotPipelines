"""Backward-compatible import path for IBIT smart DCA research helpers."""

from __future__ import annotations

import sys

from .research import ibit_smart_dca_research as _module

sys.modules[__name__] = _module
