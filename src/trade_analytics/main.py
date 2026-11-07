import httpx

from trade_analytics.client import ComtradeClient
from trade_analytics.extract import fetch_trade_data
from trade_analytics.validate import validate_trade_data
from trade_analytics.transform import transform_trade_data
from trade_analytics.aggregate import aggregate_trade_data
from trade_analytics.storage import save_trade_data


PARAMS = {
    "period": "202401",
    "reporterCode": "842",
    "flowCode": "M",
    "cmdCode": "854231",
    "partnerCode": "0",
}


def main() -> None:
    with httpx.Client(timeout=30.0) as http_client:
        client = ComtradeClient(http_client)
        trade_data = fetch_trade_data(client, PARAMS)

    validate_trade_data(trade_data, PARAMS)
    transformed_data = transform_trade_data(trade_data)
    summary = aggregate_trade_data(transformed_data)
    manifest = save_trade_data(transformed_data, summary)

    print("ETL completed successfully")
    print("World value:", summary["world_value"])
    print("Partner count:", summary["partner_count"])
    print("Manifest:", manifest)


if __name__ == "__main__":
    main()