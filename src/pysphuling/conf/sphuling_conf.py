"""Configuration management for Sphuling / PySphuling.

Supports 'sphuling.*' configuration properties with backwards-compatible
translation for 'spark.*' legacy keys.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple


class SphulingConf:
    """Configuration for a Sphuling application. Used to set various parameters
    such as cores, memory pool limits, batch sizes, and optimizer flags.
    """

    def __init__(self, loadDefaults: bool = True, _dict: Optional[Dict[str, str]] = None) -> None:
        self._conf: Dict[str, str] = {}
        if loadDefaults:
            self._set_defaults()
        if _dict:
            for k, v in _dict.items():
                self.set(k, v)

    def _set_defaults(self) -> None:
        # Defaults for local execution
        self._conf["sphuling.app.name"] = "SphulingApplication"
        self._conf["sphuling.master"] = "local[*]"
        self._conf["sphuling.sql.shuffle.partitions"] = "4"
        self._conf["sphuling.batch.size"] = "8192"
        self._conf["sphuling.repartition.joins"] = "true"
        self._conf["sphuling.repartition.aggregations"] = "true"
        self._conf["sphuling.scratch.spill.dir"] = "./.sphuling/spill"

    def _normalize_key(self, key: str) -> str:
        # Map legacy spark.* to sphuling.*
        if key.startswith("spark."):
            return "sphuling." + key[len("spark."):]
        return key

    def set(self, key: str, value: Any) -> SphulingConf:
        norm_key = self._normalize_key(key)
        self._conf[norm_key] = str(value)
        return self

    def get(self, key: str, defaultValue: Optional[str] = None) -> Optional[str]:
        norm_key = self._normalize_key(key)
        return self._conf.get(norm_key, defaultValue)

    def getAll(self) -> List[Tuple[str, str]]:
        return list(self._conf.items())

    def contains(self, key: str) -> bool:
        return self._normalize_key(key) in self._conf

    def setMaster(self, master: str) -> SphulingConf:
        return self.set("sphuling.master", master)

    def setAppName(self, value: str) -> SphulingConf:
        return self.set("sphuling.app.name", value)

    def to_dict(self) -> Dict[str, str]:
        return dict(self._conf)

    def __repr__(self) -> str:
        return f"<SphulingConf: {len(self._conf)} properties>"


# Drop-in alias for PySpark compatibility
SparkConf = SphulingConf

