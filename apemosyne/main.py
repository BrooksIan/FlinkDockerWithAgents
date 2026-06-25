"""Console entry for ``apemosyne`` script."""

from __future__ import annotations

import sys


def main() -> None:
  if "--version" in sys.argv or "-V" in sys.argv:
    from apemosyne import __version__

    print(f"apemosyne {__version__}")
    raise SystemExit(0)

  from apemosyne._entry import app

  app()


if __name__ == "__main__":
  main()
