import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    ARKHAM_API_KEY,
    DUNE_API_KEY_WALLE,
)
from services.dune.table_api import TableApi
from services.arkham.portfolio_service import PortfolioService


def main():

    DUNE_TABLE_QUERY_ID = 6074774
    DUNE_TABLE_NAME_SPACE = "sparkdotfi"
    DUNE_TABLE_NAME = "dataset_whale_portfolio_arkham_api"
    DUNE_TABLE_DESCRIPTION = "Whale portfolio dataset from Arkham"
    SCHEMA: list[dict[str, str]] = [
        {"name": "chain", "type": "varchar"},  # Wallet chain column
        {"name": "address", "type": "varbinary"},  # Wallet address column
        {"name": "symbol", "type": "varchar"},  # Token symbol column
        {"name": "balance", "type": "varchar"},  # Token balance column
        {"name": "price", "type": "varchar"},  # Token price column
        {"name": "usd", "type": "varchar"},  # USD value column
    ]
    isPrevate = False
    duneServiceWalle = TableApi(DUNE_API_KEY_WALLE)

    isTableCreated = duneServiceWalle.createTable(
        DUNE_TABLE_NAME_SPACE,
        DUNE_TABLE_NAME,
        DUNE_TABLE_DESCRIPTION,
        SCHEMA,
        isPrevate,
    )
    if not isTableCreated:
        return

    address_params = duneServiceWalle.queryRowDataByTableId(
        DUNE_TABLE_QUERY_ID, "user_addr"
    )
    if not address_params:
        return

    portfolioService = PortfolioService(ARKHAM_API_KEY)
    file_path = portfolioService.export_portfolios(address_params)

    isClear = duneServiceWalle.clearTable(DUNE_TABLE_NAME_SPACE, DUNE_TABLE_NAME)
    if not isClear:
        return

    duneServiceWalle.insertCsvToTable(file_path, DUNE_TABLE_NAME_SPACE, DUNE_TABLE_NAME)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Program execution interrupted by user")
    except Exception as e:
        print(f"\n❌ Program execution error: {str(e)}")
        import traceback

        traceback.print_exc()


#
