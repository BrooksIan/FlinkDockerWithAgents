#!/usr/bin/env python3
"""PyFlink DataStream smoke demo (runs inside TaskManager via ``ratatoskr demo datastream``)."""

from __future__ import annotations


def main() -> None:
    from pyflink.datastream import StreamExecutionEnvironment

    env = StreamExecutionEnvironment.get_execution_environment()
    (
        env.from_collection([1, 2, 3])
        .map(lambda value: value * 2, output_type="INT")
        .print()
    )
    env.execute("ratatoskr-datastream-demo")


if __name__ == "__main__":
    main()
