"""
CD to this directory to run these invoke commands.
"""

from __future__ import annotations

import pathlib
from typing import Optional

import invoke
import pandas as pd

TEST_ROOT = pathlib.Path(__file__).parent.parent.parent.resolve(strict=True)


@invoke.task(aliases=["gen-assets"])
def generate_asset_files(
    ctx: invoke.Context,
    source_csv_dir: Optional[str | pathlib.Path] = None,
    limit: int = 12,
    year: str = 2019,
):
    """Generate or re-generate various test asset files."""
    if not source_csv_dir:
        source_csv_dir = TEST_ROOT / "test_sets" / "taxi_yellow_tripdata_samples"

    source_csv_dir = pathlib.Path(source_csv_dir).resolve(strict=True)

    json_asset_dir = pathlib.Path.cwd() / "json_assets"
    json_asset_dir.mkdir(exist_ok=True)
    for i, csv_file in enumerate(source_csv_dir.glob(f"*{year}*.csv"), start=1):
        json_output_file = json_asset_dir / csv_file.with_suffix(".json").name
        print(i, json_output_file.name)
        df: pd.DataFrame = pd.read_csv(csv_file)
        # write in a format that is non-default - default orient is 'index'
        df.to_json(json_output_file, orient="records")
        if i == limit:
            break
