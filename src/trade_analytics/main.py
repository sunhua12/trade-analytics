from trade_analytics.extract import PARAMS, fetch_trade_data
from trade_analytics.validate import validate_trade_data
from trade_analytics.transform import transform_trade_data
from trade_analytics.aggregate import aggregate_trade_data
from trade_analytics.storage import save_trade_data


def main() -> None:
    trade_data = fetch_trade_data(PARAMS)
    validate_trade_data(trade_data, PARAMS)
    transformed_data = transform_trade_data(trade_data)
    summary = aggregate_trade_data(transformed_data)
    manifest = save_trade_data(transformed_data, summary, )

    print("ETL completed successfully")
    print("World value:", summary["world_value"])
    print("Partner count:", summary["partner_count"])
    print("Manifest:", manifest)


if __name__ == "__main__":
    main()
