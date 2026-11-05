from trade_analytics.extract import fetch_trade_data, PARAMS
from trade_analytics.validate import validate_trade_data


def main():
    # Extract
    trade_data = fetch_trade_data(PARAMS)

    # Validate
    validate_trade_data(trade_data, PARAMS)


if __name__ == "__main__":
    main()