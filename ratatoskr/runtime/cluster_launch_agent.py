#!/usr/bin/env python3
"""Minimal PyFlink job submitted during ``ratatoskr test launch --cluster``."""

from __future__ import annotations

from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    (
        env.from_collection([1, 2, 3])
        .map(lambda value: value * 2, output_type=Types.INT())
        .print()
    )
    env.execute("Ratatoskr Launch Smoke")


if __name__ == "__main__":
    main()
