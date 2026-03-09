import sys
import os
from datetime import datetime

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
        {"name": "update_date", "type": "date"},  
    ]
    
    duneServiceWalle = TableApi(DUNE_API_KEY_WALLE)
    
    # ====================
    # Table creation (only runs if table doesn't exist)
    # ====================
    print("📋 Checking if table exists...")
    try:
        isTableCreated = duneServiceWalle.createTable(
            DUNE_TABLE_NAME_SPACE, DUNE_TABLE_NAME, DUNE_TABLE_DESCRIPTION, SCHEMA
        )
        if isTableCreated:
            print("✅ Table created successfully")
        else:
            print("ℹ️  Table already exists, continuing...")
    except Exception as e:
        print(f"⚠️  Table check: {str(e)}")
        print("ℹ️  Assuming table exists, continuing...")

    # ====================
    # PRODUCTION MODE: Fetch addresses from Dune and call Arkham API
    # ====================
    print("📊 Fetching addresses from Dune...")
    address_params = duneServiceWalle.queryRowDataByTableId(DUNE_TABLE_ID, "user_addr")
    
    if not address_params:
        print("❌ No addresses found in Dune table")
        return
    
    print(f"✅ Found {len(address_params)} addresses")
    
    print("🔍 Fetching labels from Arkham API...")
    labelService = LabelService(ARKHAM_API_KEY)
    file_path = labelService.export_labels(address_params)
    
    if not file_path or not os.path.exists(file_path):
        print("❌ Failed to get labels from Arkham")
        return
    
    print(f"✅ Labels exported to: {file_path}")

    # Upload CSV to Dune table (append mode)
    print("📤 Uploading data to Dune...")
    duneServiceWalle.insertCsvToTable(file_path, DUNE_TABLE_NAME_SPACE, DUNE_TABLE_NAME)


if __name__ == "__main__":
    main()
