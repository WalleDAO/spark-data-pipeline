import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DUNE_API_KEY_WALLE, ARKHAM_API_KEY
from services.dune.table_api import TableApi
from services.arkham.label_service import LabelService


def main():

    DUNE_TABLE_ID = 6074774
    DUNE_TABLE_NAME_SPACE = "sparkdotfi"
    DUNE_TABLE_NAME = "dataset_whale_labels_arkham_api"
    DUNE_TABLE_DESCRIPTION = "Whale labels dataset from Arkham"
    SCHEMA: list[dict[str, str]] = [
        {"name": "no", "type": "integer"},
        {"name": "address", "type": "varbinary"},
        {"name": "name", "type": "varchar"},
        {"name": "type", "type": "varchar"},
        {"name": "label", "type": "varchar"},
        {"name": "isuseraddress", "type": "boolean"},
        {"name": "website", "type": "varchar"},
        {"name": "twitter", "type": "varchar"},
        {"name": "crunchbase", "type": "varchar"},
        {"name": "linkedin", "type": "varchar"},
    ]
    duneServiceWalle = TableApi(DUNE_API_KEY_WALLE)
    isCreated = duneServiceWalle.createTable(
        DUNE_TABLE_NAME_SPACE, DUNE_TABLE_NAME, DUNE_TABLE_DESCRIPTION, SCHEMA
    )
    if not isCreated:
        return
    address_params = duneServiceWalle.queryRowDataByTableId(DUNE_TABLE_ID, "user_addr")

    isClear = duneServiceWalle.clearTable(DUNE_TABLE_NAME_SPACE, DUNE_TABLE_NAME)
    if not isClear:
        return

    labelService = LabelService(ARKHAM_API_KEY)
    file_path = labelService.export_labels(address_params)

    duneServiceWalle.insertCsvToTable(file_path, DUNE_TABLE_NAME_SPACE, DUNE_TABLE_NAME)


if __name__ == "__main__":
    main()
