from dune_client.client import DuneClient

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    ARKHAM_API_KEY,
    DUNE_API_KEY_WALLE,
)
from services.dune.table_api import TableApi
from services.arkham.arkham_api import ArkhamApi
from services.arkham.portfolio_service import export_Portfolios


def main():
    # Dune Table config
    duneService = TableApi(DUNE_API_KEY_WALLE)
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
    # duneService.deleteTable(DUNE_TABLE_NAME_SPACE, DUNE_TABLE_NAME)
    """
    Step 0: Create a new dune table
    """
    isTableCreated = duneService.createTable(
        DUNE_TABLE_NAME_SPACE,
        DUNE_TABLE_NAME,
        DUNE_TABLE_DESCRIPTION,
        SCHEMA,
        isPrevate,
    )
    if not isTableCreated:
        print("❌ Failed to create table from dune")
        return
    """
    Step 1: Fetching address data from Dune Analytics...
    """
    user_addresses = duneService.queryRowDataByTableId(DUNE_TABLE_QUERY_ID, "user_addr")
    if not user_addresses:
        print("❌ Failed to retrieve any address data, program terminated")
        return

    print(f"✅ Step 1:  Dune data retrieval completed")
    print(f"📊 Retrieved {len(user_addresses)} addresses\n")

    """
    Step 2: Getting address portfolio through Arkham Intelligence...
    """
    filename = export_Portfolios(user_addresses, ArkhamApi(ARKHAM_API_KEY))
    print(f"✅ Step 2:  Arkham Intelligence processing completed")
    print(f"📁 Output file: {filename}")

    """
    Step 3: Clear the existing contents of the data table
    """
    isClear = duneService.clearTable(DUNE_TABLE_NAME_SPACE, DUNE_TABLE_NAME)
    if not isClear:
        print("❌ Data table clearing failed. Please check if the data table exists.")
        return
    print(f"✅ Step 3:  Data cleanup successful！")

    """
    Step 4: Insert the data processed in Step 2 into the data table on the Dune platform.
    """
    isInsert = duneService.insertCsvToTable(
        filename, DUNE_TABLE_NAME_SPACE, DUNE_TABLE_NAME
    )
    if not isInsert:
        print("❌ Data insertion failed, please check and try again.")
        return
    print(
        f"✅ Step 4:  Data update successful! Please open the online data platform to check if the data has been updated."
    )


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
