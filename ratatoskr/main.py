"""Console entry for ``ratatoskr`` script."""

from __future__ import annotations

import sys


def main() -> None:
  if "--version" in sys.argv or "-V" in sys.argv:
    from ratatoskr import __version__

    print(f"ratatoskr {__version__}")
    raise SystemExit(0)

  from ratatoskr._entry import app

  app()


if __name__ == "__main__":
  main()
