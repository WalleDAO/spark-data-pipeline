from dune_client.client import DuneClient
import requests
from config import DUNE_API_KEY,DUNE_TABLE_NAME_SPACE,DUNE_TABLE_NAME,DUNE_TABLE_DESCRIPTION

def createDuneTable(dune):
    # Args: dune: DuneClient object
    # Returns: bool: Returns True if creation successful, False if failed
    try:
        table = dune.create_table(
            namespace=DUNE_TABLE_NAME_SPACE,
            table_name=DUNE_TABLE_NAME,
            description=DUNE_TABLE_DESCRIPTION,
            schema=[
                {"name": "no", "type": "integer"},
                {"name": "address", "type": "varbinary"},
                {"name": "name", "type": "varchar"},
                {"name": "type", "type": "varchar"},
                {"name": "label", "type": "varchar"},
                {"name": "isuseraddress", "type": "boolean"},
                {"name": "website", "type": "varchar"},
                {"name": "twitter", "type": "varchar"},
                {"name": "crunchbase", "type": "varchar"},
                {"name": "linkedin", "type": "varchar"}
            ],
            is_private=False
        )
        
        # Use object attribute access instead of dictionary method
        if hasattr(table, 'already_existed') and table.already_existed:
            print(f"Table already exists: {table.full_name}")
            print(f"Status: {table.message}")
        else:
            print(f"Table created successfully: {table.full_name}")
            print(f"Status: {table.message}")
            print(f"Credits consumed: 10")
        
        print(f"Example query: {table.example_query}")
        return True
        
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code
        try:
            error_detail = e.response.json()
            print(f"Error code: {status_code}")
            print(f"Detailed error information: {error_detail}")
        except:
            print(f"Error code: {status_code}")
            print(f"Response content: {e.response.text}")
        return False
        
    except Exception as e:
        print(f"Unknown error: {type(e).__name__}")
        print(f"Error content: {str(e)}")
        return False

def main():
    # Initialize DuneClient
    dune = DuneClient(DUNE_API_KEY)
    
    print("Starting to create Dune table...")
    success = createDuneTable(dune)
    
    if success:
        print("Table creation process completed!")
    else:
        print("Table creation failed!")

if __name__ == "__main__":
    main()
