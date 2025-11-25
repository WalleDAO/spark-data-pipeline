from dune_client.client import DuneClient

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    ARKHAM_API_KEY,
    DUNE_API_KEY,
    DUNE_TABLE_NAME_SPACE,
    DUNE_TABLE_NAME,
    DUNE_TABLE_ID,
)
from dune_service.clear_dune_table import clearDuneTable
from dune_service.query_dune_row_data_by_table_id import queryDuneRowDataByTableId
from dune_service.insert_dune_table import insertCsvToDuneTable
from arkham_service.export_arkham_portfolio import exportPortfoliosToCsv


def main():
    dune = DuneClient(DUNE_API_KEY)

    """
    Step 1: Fetching address data from Dune Analytics...
    """
    # user_addresses = address_params
    user_addresses = queryDuneRowDataByTableId(dune, DUNE_TABLE_ID, "user_addr")
    if not user_addresses:
        print("❌ Failed to retrieve any address data, program terminated")
        return

    print(f"✅ Step 1:  Dune data retrieval completed")
    print(f"📊 Retrieved {len(user_addresses)} addresses\n")

    """
    Step 2: Getting address portfolio through Arkham Intelligence...
    """
    filename = exportPortfoliosToCsv(user_addresses, ARKHAM_API_KEY)
    print(f"✅ Step 2:  Arkham Intelligence processing completed")
    print(f"📁 Output file: {filename}")

    """
    Step 3: Clear the existing contents of the data table
    """
    isClear = clearDuneTable(dune, DUNE_API_KEY, DUNE_TABLE_NAME_SPACE, DUNE_TABLE_NAME)
    if not isClear:
        print("❌ Data table clearing failed. Please check if the data table exists.")
        return
    print(f"✅ Step 3:  Data cleanup successful！")

    """
    Step 4: Insert the data processed in Step 2 into the data table on the Dune platform.
    """
    isInsert = insertCsvToDuneTable(
        dune, filename, DUNE_TABLE_NAME_SPACE, DUNE_TABLE_NAME
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
