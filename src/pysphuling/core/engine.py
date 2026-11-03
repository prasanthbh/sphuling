"""DataFusion Execution Engine wrapper for PySphuling."""

from __future__ import annotations
import os
from typing import Optional
import datafusion as df

from pysphuling.conf.sphuling_conf import SphulingConf
from pysphuling.core.resources import parse_cores, parse_memory_to_bytes


class SphulingEngine:
    """Manages the underlying Apache DataFusion SessionContext, SessionConfig,
    RuntimeEnvBuilder, and temporary disk spilling directory.
    """

    def __init__(self, conf: Optional[SphulingConf] = None) -> None:
        self.conf = conf or SphulingConf()
        self._ctx: Optional[df.SessionContext] = None
        self._init_context()

    def _init_context(self) -> None:
        # 1. Parse resource constraints
        master = self.conf.get("sphuling.master", "local[*]")
        cores_cfg = self.conf.get("sphuling.executor.cores")
        num_cores = parse_cores(master, cores_cfg)

        mem_cfg = self.conf.get("sphuling.executor.memory", "4g")
        mem_bytes = parse_memory_to_bytes(mem_cfg)

        spill_dir = self.conf.get("sphuling.scratch.spill.dir", "./.sphuling/spill")
        os.makedirs(spill_dir, exist_ok=True)

        batch_size = int(self.conf.get("sphuling.batch.size", "8192"))

        # 2. Configure DataFusion SessionConfig
        config_options = {
            "datafusion.execution.target_partitions": str(num_cores),
            "datafusion.execution.batch_size": str(batch_size),
            "datafusion.execution.parquet.pruning": "true",
            "datafusion.execution.parquet.pushdown_filters": "true",
            "datafusion.optimizer.repartition_joins": "true",
            "datafusion.optimizer.repartition_aggregations": "true",
            "datafusion.optimizer.join_reordering": "true",
            "datafusion.sql_parser.enable_ident_normalization": "true",
        }

        # Apply any explicit user config mappings
        for k, v in self.conf.getAll():
            if k.startswith("datafusion."):
                config_options[k] = str(v)

        session_cfg = df.SessionConfig(config_options)

        # 3. Configure RuntimeEnv with Memory Pool & Disk Spilling
        runtime_builder = (
            df.RuntimeEnvBuilder()
            .with_temp_file_path(os.path.abspath(spill_dir))
            .with_greedy_memory_pool(mem_bytes)
        )

        self._ctx = df.SessionContext(session_cfg, runtime_builder)
        self.num_cores = num_cores
        self.mem_bytes = mem_bytes
        self.spill_dir = spill_dir

    @property
    def ctx(self) -> df.SessionContext:
        if self._ctx is None:
            self._init_context()
        return self._ctx

    def stop(self) -> None:
        """Stops the engine and releases session resources."""
        self._ctx = None

