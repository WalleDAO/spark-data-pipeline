from dune_client.client import DuneClient
import requests
import os


def insertCsvToDuneTable(dune, csv_file_path, namespace, table_name):
    """
    Insert CSV data into an existing Dune Analytics table.

    This function reads a CSV file and inserts its data into a specified Dune table.
    It handles various error scenarios including file not found, HTTP errors, and
    unexpected response formats.

    Args:
        dune: DuneClient object for API communication
        csv_file_path: File path to the CSV file to be inserted
        namespace: Namespace of the target table
        table_name: Name of the target table

    Returns:
        bool: True if data insertion was successful, False otherwise
    """

    if not os.path.exists(csv_file_path):
        print(f"File not found: {csv_file_path}")
        return False

    try:
        with open(csv_file_path, "rb") as data:
            result = dune.insert_table(
                namespace,
                table_name,
                data=data,
                content_type="text/csv",  # CSV format specification
            )

        if hasattr(result, "rows_written") and hasattr(result, "bytes_written"):
            print("Data insertion successful")
            print(f"Table: {namespace}.{table_name}")
            print(f"Rows written: {result.rows_written}")
            print(f"Bytes written: {result.bytes_written}")
            return True
        else:
            print("Unexpected response format:")
            print(f"Response: {result}")
            print(f"Response type: {type(result)}")
            print(
                f"Available attributes: {[attr for attr in dir(result) if not attr.startswith('_')]}"
            )
            return False

    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code
        try:
            error_detail = e.response.json()
            print(f"HTTP Error {status_code}")

            if status_code == 400:  # Bad request error code
                print(f"Bad request: {error_detail.get('error', 'Unknown error')}")
            elif status_code == 401:  # Unauthorized error code
                print(f"Unauthorized: {error_detail.get('error', 'Unknown error')}")
            elif status_code == 404:  # Not found error code
                print(f"Table not found: {error_detail.get('error', 'Unknown error')}")
            elif status_code == 500:  # Internal server error code
                print(
                    f"Internal server error: {error_detail.get('error', 'Unknown error')}"
                )
            else:
                print(f"Error: {error_detail.get('error', 'Unknown error')}")

        except:
            print(f"HTTP Error {status_code}")
            print(f"Response: {e.response.text}")
        return False

    except FileNotFoundError:
        print(f"CSV file not found: {csv_file_path}")
        return False

    except Exception as e:
        print(f"Unexpected error: {type(e).__name__}")
        print(f"Error details: {str(e)}")
        return False
