from dune_client.client import DuneClient
import requests
import os
from config import DUNE_TABLE_NAME_SPACE, DUNE_TABLE_NAME

def insertDataToDuneTable(dune, csv_file_path):
    """
    Insert CSV data into existing Dune table
    Args: 
        dune: DuneClient object
        csv_file_path: Path to CSV file
    Returns: 
        bool: True if successful, False if failed
    """
    
    # Check if CSV file exists
    if not os.path.exists(csv_file_path):
        print(f"❌ CSV file not found: {csv_file_path}")
        return False
    
    try:
        # Insert CSV data directly to Dune table
        with open(csv_file_path, "rb") as data:
            result = dune.insert_table(
                namespace=DUNE_TABLE_NAME_SPACE,
                table_name=DUNE_TABLE_NAME,
                data=data,
                content_type="text/csv"
            )
        
        # Handle InsertTableResult object
        if hasattr(result, 'rows_written') and hasattr(result, 'bytes_written'):
            print(f"✅ Data insertion successful")
            print(f"📋 Table: {DUNE_TABLE_NAME_SPACE}.{DUNE_TABLE_NAME}")
            print(f"📝 Rows written: {result.rows_written}")
            print(f"💾 Bytes written: {result.bytes_written}")
            return True
        else:
            # Handle unexpected response format
            print(f"⚠️ Unexpected response format:")
            print(f"🔍 Response: {result}")
            print(f"🔍 Response type: {type(result)}")
            print(f"🔍 Available attributes: {[attr for attr in dir(result) if not attr.startswith('_')]}")
            return False
        
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code
        try:
            error_detail = e.response.json()
            print(f"❌ HTTP Error {status_code}")
            
            # Handle different error status codes according to docs
            if status_code == 400:
                print(f"🔍 Bad request: {error_detail.get('error', 'Unknown error')}")
            elif status_code == 401:
                print(f"🔍 Unauthorized: {error_detail.get('error', 'Unknown error')}")
            elif status_code == 404:
                print(f"🔍 Table not found: {error_detail.get('error', 'Unknown error')}")
            elif status_code == 500:
                print(f"🔍 Internal server error: {error_detail.get('error', 'Unknown error')}")
            else:
                print(f"🔍 Error: {error_detail.get('error', 'Unknown error')}")
                
        except:
            print(f"❌ HTTP Error {status_code}")
            print(f"📄 Response: {e.response.text}")
        return False
        
    except FileNotFoundError:
        print(f"❌ CSV file not found: {csv_file_path}")
        return False
        
    except Exception as e:
        print(f"❌ Unexpected error: {type(e).__name__}")
        print(f"🔍 Error details: {str(e)}")
        return False
