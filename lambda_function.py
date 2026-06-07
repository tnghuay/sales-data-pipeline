import boto3
import csv
import json
import os
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from io import StringIO
from urllib.parse import unquote_plus


s3 = boto3.client("s3")
sns = boto3.client("sns")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")

REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "product_id",
    "order_date",
    "quantity",
    "unit_price",
    "payment_status",
]

VALID_PAYMENT_STATUSES = {"paid", "failed", "pending", "refunded"}


def lambda_handler(event, context):
    """
    Expected S3 input path:

    store-001/raw/store_001_20240123.csv

    This Lambda also detects invalid file format.
    If a non-CSV file is uploaded to raw/, it sends an SNS alert.
    """

    results = []

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        print(f"Received file: s3://{bucket}/{key}")

        if "/raw/" not in key:
            print(f"Skipped because file is not inside raw folder: {key}")
            continue

        if not key.lower().endswith(".csv"):
            message = f"""
Critical data quality issue detected.

Issue type: Invalid file format
Bucket: {bucket}
File: {key}

Expected format:
CSV file with .csv extension

Action required:
Please upload a valid CSV file to the raw folder.
"""

            send_sns_alert(
                subject="Critical data quality issue: invalid file format",
                message=message,
            )

            result = {
                "source_file": key,
                "status": "REJECTED",
                "reason": "Invalid file format. Expected .csv file.",
                "processed_at": datetime.utcnow().isoformat() + "Z",
            }

            results.append(result)

            print(json.dumps(result, indent=2))
            continue

        try:
            result = process_sales_file(bucket, key)
            results.append(result)

        except Exception as exc:
            message = f"""
Critical data quality issue detected.

Issue type: File processing failed
Bucket: {bucket}
File: {key}

Error:
{str(exc)}

Action required:
Please check the uploaded file schema, format, and content.
"""

            send_sns_alert(
                subject="Critical data quality issue: file processing failed",
                message=message,
            )

            raise

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Processing completed",
                "results": results,
            },
            default=str,
        ),
    }


def process_sales_file(bucket, key):
    csv_text = read_s3_text(bucket, key)
    rows = parse_csv(csv_text)

    good_records, bad_records, summary = validate_and_transform(rows)
    metrics = compute_metrics(good_records)

    base_filename = os.path.basename(key).replace(".csv", "")

    processed_key = build_output_key(
        source_key=key,
        output_folder="processed",
        filename=f"processed_{base_filename}.csv",
    )

    errors_key = build_output_key(
        source_key=key,
        output_folder="errors",
        filename=f"errors_{base_filename}.csv",
    )

    analytics_key = build_output_key(
        source_key=key,
        output_folder="analytics",
        filename=f"metrics_{base_filename}.json",
    )

    if good_records:
        put_csv(bucket, processed_key, good_records)

    if bad_records:
        put_csv(bucket, errors_key, bad_records)

    put_json(bucket, analytics_key, metrics)

    processing_result = {
        "source_file": key,
        "processed_file": processed_key if good_records else None,
        "errors_file": errors_key if bad_records else None,
        "analytics_file": analytics_key,
        "status": "COMPLETED",
        "processed_at": datetime.utcnow().isoformat() + "Z",
        "summary": summary,
    }

    print(json.dumps(processing_result, indent=2, default=str))

    return processing_result


def read_s3_text(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8")


def parse_csv(csv_text):
    csv_file = StringIO(csv_text)
    reader = csv.DictReader(csv_file)

    if reader.fieldnames is None:
        raise ValueError("CSV file has no header")

    normalized_headers = [normalize_column_name(col) for col in reader.fieldnames]

    missing_columns = set(REQUIRED_COLUMNS) - set(normalized_headers)

    if missing_columns:
        raise ValueError(f"Invalid schema. Missing columns: {sorted(missing_columns)}")

    rows = []

    for raw_row in reader:
        normalized_row = {}

        for original_col, value in raw_row.items():
            normalized_col = normalize_column_name(original_col)
            normalized_row[normalized_col] = value.strip() if value else ""

        rows.append(normalized_row)

    return rows


def normalize_column_name(column_name):
    """
    Allows both:
    payment_status
    payment-status
    """
    return column_name.strip().lower().replace("-", "_")


def validate_and_transform(rows):
    good_records = []
    bad_records = []
    seen_order_ids = set()

    summary = {
        "total_rows": len(rows),
        "good_rows": 0,
        "bad_rows": 0,
        "duplicate_rows": 0,
        "missing_value_rows": 0,
        "invalid_quantity_rows": 0,
        "invalid_price_rows": 0,
        "invalid_date_rows": 0,
        "invalid_payment_status_rows": 0,
    }

    for row_number, row in enumerate(rows, start=2):
        errors = []

        order_id = row.get("order_id", "")
        customer_id = row.get("customer_id", "")
        product_id = row.get("product_id", "")
        order_date = row.get("order_date", "")
        quantity_raw = row.get("quantity", "")
        unit_price_raw = row.get("unit_price", "")
        payment_status = row.get("payment_status", "").lower()

        required_values = {
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "order_date": order_date,
            "quantity": quantity_raw,
            "unit_price": unit_price_raw,
            "payment_status": payment_status,
        }

        missing_fields = [
            field
            for field, value in required_values.items()
            if value is None or value == ""
        ]

        if missing_fields:
            errors.append(f"Missing required fields: {', '.join(missing_fields)}")
            summary["missing_value_rows"] += 1

        if order_id:
            if order_id in seen_order_ids:
                errors.append("Duplicate order_id")
                summary["duplicate_rows"] += 1
            else:
                seen_order_ids.add(order_id)

        quantity = None
        unit_price = None

        try:
            quantity = int(quantity_raw)
            if quantity <= 0:
                errors.append("Quantity must be greater than 0")
                summary["invalid_quantity_rows"] += 1
        except ValueError:
            errors.append("Quantity must be an integer")
            summary["invalid_quantity_rows"] += 1

        try:
            unit_price = Decimal(unit_price_raw)
            if unit_price < 0:
                errors.append("Unit price must not be negative")
                summary["invalid_price_rows"] += 1
        except (InvalidOperation, ValueError):
            errors.append("Unit price must be a valid number")
            summary["invalid_price_rows"] += 1

        if not is_valid_date(order_date):
            errors.append("order_date must be in YYYY-MM-DD format")
            summary["invalid_date_rows"] += 1

        if payment_status not in VALID_PAYMENT_STATUSES:
            errors.append(
                f"payment_status must be one of: {', '.join(sorted(VALID_PAYMENT_STATUSES))}"
            )
            summary["invalid_payment_status_rows"] += 1

        if errors:
            bad_row = dict(row)
            bad_row["row_number"] = row_number
            bad_row["error_reason"] = " | ".join(errors)
            bad_records.append(bad_row)
            continue

        line_revenue = quantity * unit_price

        clean_record = {
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "order_date": order_date,
            "quantity": quantity,
            "unit_price": str(unit_price),
            "payment_status": payment_status,
            "line_revenue": str(line_revenue),
        }

        good_records.append(clean_record)

    summary["good_rows"] = len(good_records)
    summary["bad_rows"] = len(bad_records)

    return good_records, bad_records, summary


def is_valid_date(value):
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def compute_metrics(good_records):
    daily_revenue = {}
    product_revenue = {}
    product_quantity = {}
    orders_per_customer = {}
    payment_status_count = {}

    total_orders = len(good_records)
    paid_orders = 0

    for row in good_records:
        order_date = row["order_date"]
        product_id = row["product_id"]
        customer_id = row["customer_id"]
        payment_status = row["payment_status"]

        quantity = int(row["quantity"])
        line_revenue = Decimal(row["line_revenue"])

        orders_per_customer[customer_id] = orders_per_customer.get(customer_id, 0) + 1
        payment_status_count[payment_status] = payment_status_count.get(payment_status, 0) + 1

        product_quantity[product_id] = product_quantity.get(product_id, 0) + quantity

        if payment_status == "paid":
            paid_orders += 1

            daily_revenue[order_date] = (
                daily_revenue.get(order_date, Decimal("0")) + line_revenue
            )

            product_revenue[product_id] = (
                product_revenue.get(product_id, Decimal("0")) + line_revenue
            )

    top_products = sorted(
        [
            {
                "product_id": product_id,
                "quantity_sold": quantity_sold,
                "revenue": str(product_revenue.get(product_id, Decimal("0"))),
            }
            for product_id, quantity_sold in product_quantity.items()
        ],
        key=lambda item: item["quantity_sold"],
        reverse=True,
    )

    payment_success_rate = 0

    if total_orders > 0:
        payment_success_rate = round((paid_orders / total_orders) * 100, 2)

    return {
        "daily_revenue": {
            date: str(revenue)
            for date, revenue in daily_revenue.items()
        },
        "top_products": top_products[:10],
        "orders_per_customer": orders_per_customer,
        "payment_status_count": payment_status_count,
        "payment_success_rate_percent": payment_success_rate,
        "total_valid_orders": total_orders,
        "paid_orders": paid_orders,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }


def build_output_key(source_key, output_folder, filename):
    """
    Input:
    store-001/raw/store_001_20240122.csv

    Output:
    store-001/processed/processed_store_001_20240122.csv
    store-001/errors/errors_store_001_20240122.csv
    store-001/analytics/metrics_store_001_20240122.json
    """

    parts = source_key.split("/")

    if len(parts) < 3:
        raise ValueError(f"Unexpected S3 key structure: {source_key}")

    store_folder = parts[0]

    return f"{store_folder}/{output_folder}/{filename}"


def extract_date_from_key(source_key):
    """
    Extracts date from filename like:
    store_001_20240115.csv

    Returns:
    2024, 01, 15
    """

    filename = os.path.basename(source_key)

    match = re.search(r"(\d{8})", filename)

    if not match:
        current_date = datetime.utcnow()
        return (
            str(current_date.year),
            f"{current_date.month:02d}",
            f"{current_date.day:02d}",
        )

    date_string = match.group(1)

    year = date_string[0:4]
    month = date_string[4:6]
    day = date_string[6:8]

    return year, month, day


def put_csv(bucket, key, records):
    if not records:
        return

    csv_buffer = StringIO()
    fieldnames = list(records[0].keys())

    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(records)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )

    print(f"Wrote CSV to s3://{bucket}/{key}")


def put_json(bucket, key, data):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )

    print(f"Wrote JSON to s3://{bucket}/{key}")


def send_sns_alert(subject, message):
    if not SNS_TOPIC_ARN:
        print("SNS_TOPIC_ARN is not configured. Alert was not sent.")
        print(message)
        return

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=message,
    )

    print(f"SNS alert sent: {subject}")