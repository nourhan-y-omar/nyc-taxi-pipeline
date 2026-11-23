import pyarrow as pa
import pytest

from conftest import TRIP_SCHEMA, DownloadResponse
from taxi_pipeline import ingest


def test_download_rejects_an_incomplete_response_and_accepts_retry(tmp_path, monkeypatch):
    payload = b"complete source file"
    destination = tmp_path / "taxi_zone_lookup.csv"
    url = "https://example.com/taxi_zone_lookup.csv"
    responses = iter(
        [DownloadResponse(payload[:5], len(payload)), DownloadResponse(payload, len(payload))]
    )

    def get_response(request, *, timeout):
        assert request.full_url == url
        assert timeout > 0
        return next(responses)

    monkeypatch.setattr(ingest, "urlopen", get_response)
    with pytest.raises(ValueError, match="Incomplete download"):
        ingest.download(url, destination)

    ingest.download(url, destination)
    assert destination.read_bytes() == payload


def test_trip_schema_accepts_older_optional_columns_and_case_variations():
    schema = pa.schema(
        [
            (field.name.swapcase(), field.type)
            for field in TRIP_SCHEMA
            if field.name not in {"Airport_fee", "congestion_surcharge"}
        ]
    )
    columns = ingest.trip_columns(schema)
    assert columns["vendor_id"] == "vENDORid"
    assert columns["pickup_at"] == "TPEP_PICKUP_DATETIME"
    assert "airport_fee" not in columns
    assert "congestion_surcharge" not in columns


@pytest.mark.parametrize(
    ("schema", "message"),
    [
        (
            pa.schema([field for field in TRIP_SCHEMA if field.name != "fare_amount"]),
            "Missing required Parquet columns: fare_amount",
        ),
        (
            TRIP_SCHEMA.set(
                TRIP_SCHEMA.get_field_index("fare_amount"), pa.field("fare_amount", pa.string())
            ),
            "Incompatible Parquet type for fare_amount: string",
        ),
        (
            TRIP_SCHEMA.append(pa.field("VendorId", pa.int64())),
            "ambiguous column names",
        ),
    ],
)
def test_trip_schema_rejects_broken_columns(schema, message):
    with pytest.raises(ValueError, match=message):
        ingest.trip_columns(schema)
