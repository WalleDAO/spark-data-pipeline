from dune_client.client import DuneClient
import requests


def clearDuneTable(dune, api_key, namespace, table_name):
    """
    Clear a Dune Analytics table using either the client method or direct API call.

    This function attempts to clear a table first using the client's clear_table method
    if available, otherwise falls back to a direct API call.

    Args:
        dune: DuneClient instance
        api_key: Dune API key for authentication
        namespace: Namespace of the table
        table_name: Name of the table to clear

    Returns:
        bool: True if table was cleared successfully, False otherwise
    """
    try:
        if hasattr(dune, "clear_table"):
            result = dune.clear_table(namespace, table_name)
            if hasattr(result, "message"):
                print("Table cleared successfully")
                return True
            else:
                print(f"Unexpected response: {result}")
                return False
        else:
            return clearDuneTableDirectAPI(dune, api_key, namespace, table_name)
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code
        try:
            error_detail = e.response.json()
            print(
                f"HTTP Error {status_code}: {error_detail.get('error', 'Unknown error')}"
            )
        except:
            print(f"HTTP Error {status_code}: {e.response.text}")
        return False
    except Exception as e:
        print(f"Error: {str(e)}")
        return False


def clearDuneTableDirectAPI(dune, api_key, namespace, table_name):
    """
    Clear a Dune Analytics table using direct API call.

    This function is used as a fallback when the DuneClient instance doesn't
    have the clear_table method available.

    Args:
        dune: DuneClient instance (not used in this function but kept for consistency)
        api_key: Dune API key for authentication
        namespace: Namespace of the table
        table_name: Name of the table to clear

    Returns:
        bool: True if table was cleared successfully, False otherwise
    """
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
                print(
                    f"HTTP Error {response.status_code}: {error_detail.get('error', 'Unknown error')}"
                )
            except:
                print(f"HTTP Error {response.status_code}: {response.text}")
            return False
    except Exception as e:
        print(f"Error: {str(e)}")
        return False
