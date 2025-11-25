import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    DUNE_API_KEY,
    DUNE_TABLE_NAME_SPACE,
    DUNE_TABLE_NAME,
    DUNE_TABLE_DESCRIPTION,
)
from dune_service.create_dune_table import createDuneTable


def main():

    print("Starting to create Dune table...")
    success = createDuneTable(
        api_key=DUNE_API_KEY,
        namespace=DUNE_TABLE_NAME_SPACE,
        table_name=DUNE_TABLE_NAME,
        description=DUNE_TABLE_DESCRIPTION,
    )

    if success:
        print("Table creation process completed!")
    else:
        print("Table creation failed!")


if __name__ == "__main__":
    main()
