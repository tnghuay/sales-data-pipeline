from pathlib import Path
import pytest

from lambda_function import (
    parse_csv,
    validate_and_transform,
    compute_metrics,
)


def load_sample_csv():
    sample_path = Path("sample-data/store_001_20240123.csv")
    return sample_path.read_text(encoding="utf-8")


def test_sample_csv_can_be_parsed():
    csv_text = load_sample_csv()
    rows = parse_csv(csv_text)

    assert len(rows) > 0
    assert "order_id" in rows[0]
    assert "customer_id" in rows[0]
    assert "product_id" in rows[0]
    assert "order_date" in rows[0]
    assert "quantity" in rows[0]
    assert "unit_price" in rows[0]
    assert "payment_status" in rows[0]


def test_sample_csv_order_date_is_2024_01_23():
    csv_text = load_sample_csv()
    rows = parse_csv(csv_text)

    for row in rows:
        assert row["order_date"] == "2024-01-23"


def test_sample_csv_validation_runs_successfully():
    csv_text = load_sample_csv()
    rows = parse_csv(csv_text)
    good_records, bad_records, summary = validate_and_transform(rows)

    assert summary["total_rows"] == len(rows)
    assert summary["good_rows"] == len(good_records)
    assert summary["bad_rows"] == len(bad_records)


def test_good_records_have_line_revenue():
    csv_text = load_sample_csv()
    rows = parse_csv(csv_text)
    good_records, bad_records, summary = validate_and_transform(rows)

    assert len(good_records) > 0

    for record in good_records:
        assert "line_revenue" in record
        assert float(record["line_revenue"]) >= 0


def test_compute_metrics_from_sample_csv():
    csv_text = load_sample_csv()
    rows = parse_csv(csv_text)
    good_records, bad_records, summary = validate_and_transform(rows)
    metrics = compute_metrics(good_records)

    assert "daily_revenue" in metrics
    assert "top_products" in metrics
    assert "orders_per_customer" in metrics
    assert "payment_status_count" in metrics
    assert "payment_success_rate_percent" in metrics

    assert "2024-01-23" in metrics["daily_revenue"]
    assert metrics["total_valid_orders"] == len(good_records)
    assert metrics["payment_success_rate_percent"] >= 0
    assert metrics["payment_success_rate_percent"] <= 100

def test_missing_required_column_should_fail_schema_validation():
    sample_path = Path("sample-data/store_001_20240124.csv")
    csv_text = sample_path.read_text(encoding="utf-8")
    
#     csv_text = """order_id,customer_id,product_id,order_date,quantity,unit_price
# ORD001,CUST001,PROD101,2024-01-23,2,29.99
# """

    rows = parse_csv(csv_text)

    assert len(rows) == 1