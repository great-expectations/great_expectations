from __future__ import annotations

import pathlib

import invoke
import pandas as pd

TEST_ROOT = pathlib.Path(__file__).parent.parent.parent.resolve(strict=True)


@invoke.task(aliases=["gen-assets"])
def generate_asset_files(
    ctx: invoke.Context, source_csv_dir: str | pathlib.Path = None, limit: int = 5
):
    """Generate or re-generate various test asset files."""
    if not source_csv_dir:
        source_csv_dir = TEST_ROOT / "test_sets" / "taxi_yellow_tripdata_samples"
    source_csv_dir = pathlib.Path(source_csv_dir).resolve(strict=True)

    json_asset_dir = pathlib.Path.cwd() / "json_assets"
    json_asset_dir.mkdir(exist_ok=True)
    for i, csv_file in enumerate(source_csv_dir.glob("*.csv"), start=1):
        json_output_file = json_asset_dir / csv_file.with_suffix(".json").name
        print(i, json_output_file.name)
        df = pd.read_csv(csv_file)
        df.to_json(json_output_file)
        if i == limit:
            break
