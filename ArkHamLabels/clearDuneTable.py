from dune_client.client import DuneClient
import requests
from config import  DUNE_TABLE_NAME_SPACE, DUNE_TABLE_NAME
def clearDuneTable(dune,api_key):
    try:
        if hasattr(dune, 'clear_table'):
            result = dune.clear_table(namespace=DUNE_TABLE_NAME_SPACE, table_name=DUNE_TABLE_NAME)
            if hasattr(result, 'message'):
                print("Table cleared successfully")
                return True
            else:
                print(f"Unexpected response: {result}")
                return False
        else:
            return clearDuneTableDirectAPI(dune,api_key,DUNE_TABLE_NAME_SPACE, DUNE_TABLE_NAME)
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code
        try:
            error_detail = e.response.json()
            print(f"HTTP Error {status_code}: {error_detail.get('error', 'Unknown error')}")
        except:
            print(f"HTTP Error {status_code}: {e.response.text}")
        return False
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

def clearDuneTableDirectAPI(dune,api_key,namespace, table_name):
    try:
        url = f"https://api.dune.com/api/v1/table/{namespace}/{table_name}/clear"
        headers = {"X-DUNE-API-KEY": api_key}
        response = requests.post(url, headers=headers)
        
        if response.status_code == 200:
            print("Table cleared successfully")
            return True
        else:
            try:
                error_detail = response.json()
                print(f"HTTP Error {response.status_code}: {error_detail.get('error', 'Unknown error')}")
            except:
                print(f"HTTP Error {response.status_code}: {response.text}")
            return False
    except Exception as e:
        print(f"Error: {str(e)}")
        return False
