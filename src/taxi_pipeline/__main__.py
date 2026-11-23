import argparse
import logging
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

from .database import WAREHOUSE_LOCK, connect
from .ingest import load_trips, load_zones


def build_analytics():
    project_dir = Path(__file__).resolve().parents[2] / "dbt"
    dbt_name = "dbt.exe" if os.name == "nt" else "dbt"
    sibling_dbt = Path(sys.executable).with_name(dbt_name)
    command = str(sibling_dbt) if sibling_dbt.is_file() else "dbt"
    with connect() as conn:
        conn.autocommit = True
        conn.execute("select pg_advisory_lock(%s)", (WAREHOUSE_LOCK,))
        subprocess.run(
            [command, "build", "--project-dir", str(project_dir), "--profiles-dir", str(project_dir)],
            check=True,
        )


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Load and model NYC TLC yellow taxi trips")
    commands = parser.add_subparsers(dest="command", required=True)
    trips = commands.add_parser("load-trips", help="Replace one source month")
    trips.add_argument("--year", type=int, required=True)
    trips.add_argument("--month", type=int, required=True)
    trips.add_argument("--file", type=Path, help="Use a local Parquet file in place of the TLC download")
    zones = commands.add_parser("load-zones", help="Replace the TLC taxi-zone lookup")
    zones.add_argument("--file", type=Path, help="Use a local CSV file in place of the TLC download")
    commands.add_parser("build", help="Build and test the reporting models")
    args = parser.parse_args(argv)
    try:
        if args.command == "load-trips":
            try:
                source_month = date(args.year, args.month, 1)
            except ValueError as error:
                parser.error(f"Invalid source month: {error}")
            load_trips(source_month, args.file)
        elif args.command == "load-zones":
            load_zones(args.file)
        else:
            build_analytics()
    except Exception as error:
        logging.error("%s failed: %s", args.command, error)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
