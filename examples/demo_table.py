#!/usr/bin/env python3
"""PyFlink Table API smoke demo (runs inside TaskManager via ``ratatoskr demo table``)."""

from __future__ import annotations


def main() -> None:
    from pyflink.table import EnvironmentSettings, TableEnvironment

    settings = EnvironmentSettings.in_batch_mode()
    table_env = TableEnvironment.create(settings)
    table_env.execute_sql(
        """
        CREATE TEMPORARY VIEW nums AS
        SELECT * FROM (VALUES (1), (2), (3)) AS T(n)
        """
    )
    table_env.execute_sql("SELECT n, n * 2 AS doubled FROM nums").print()


if __name__ == "__main__":
    main()
