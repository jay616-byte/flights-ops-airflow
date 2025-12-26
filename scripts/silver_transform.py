import json
import pandas as pd
from pathlib import Path

def run_silver_transform(**context):
    execution_date = context["ds_nodash"]
    ti = context["ti"]

    # PULL bronze file path from XCom
    bronze_file = ti.xcom_pull(
        key="bronze_file",
        task_ids="bronze_ingestion"
    )

    if not bronze_file:
        raise ValueError("Bronze file path not found in XCom")

    silver_path = Path("/opt/airflow/data/silver")
    silver_path.mkdir(parents=True, exist_ok=True)

    # Load bronze JSON
    with open(bronze_file) as f:
        raw = json.load(f)

    # Convert OpenSky 'states' to DataFrame
    df_raw = pd.DataFrame(raw["states"])

    df_raw.columns = [
        "icao24", "callsign", "origin_country",
        "time_position", "last_contact",
        "longitude", "latitude", "baro_altitude",
        "on_ground", "velocity", "true_track",
        "vertical_rate", "sensors",
        "geo_altitude", "squawk",
        "spi", "position_source"
    ]

    # Business-relevant columns
    df = df_raw[
        [
            "icao24",
            "callsign",
            "origin_country",
            "longitude",
            "latitude",
            "velocity",
            "geo_altitude",
            "on_ground"
        ]
    ]

    output_file = silver_path / f"flights_silver_{execution_date}.csv"
    df.to_csv(output_file, index=False)

    # PUSH silver file path to XCom
    ti.xcom_push(
        key="silver_file",
        value=str(output_file)
    )

    print(f"Silver data written to {output_file}")
    return str(output_file)
