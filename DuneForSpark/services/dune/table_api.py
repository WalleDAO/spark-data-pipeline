from dune_client.client import DuneClient
import requests
import os
from typing import List


class TableApi:
    """
    Dune Analytics api class, encapsulating all Dune-related operations.
    """

    def __init__(self, api_key: str):
        """
        Initialize DuneService

        Args:
            api_key: Dune API key
        """
        self.api_key = api_key
        self.dune = DuneClient(api_key)

    def createTable(
        self, namespace, table_name, description, schema, is_private: str = False
    ):
        """
        Create a new table in Dune Analytics with predefined schema.

        This function initializes a DuneClient and creates a table with columns for
        storing cryptocurrency wallet data including address, symbol, balance, price,
        and USD value.

        Args:
            namespace: Namespace where the table will be created
            table_name: Name of the table to create
            description: Description of the table's purpose

        Returns:
            bool: True if table creation was successful or already exists, False otherwise
        """
        try:
            table = self.dune.create_table(
                namespace=namespace,
                table_name=table_name,
                schema=schema,
                description=description,
                is_private=is_private,
            )

            if hasattr(table, "already_existed") and table.already_existed:
                print(f"Table already exists: {table.full_name}")
            else:
                print(f"Table created successfully: {table.full_name}")
                print("Credits consumed: 10")

            print(f"Example query: {table.example_query}")
            return True

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code
            try:
                error_detail = e.response.json()
                print(f"Error code: {status_code}")
                print(f"Error details: {error_detail}")
            except:
                print(f"Error code: {status_code}")
                print(f"Response: {e.response.text}")
            return False

        except Exception as e:
            print(f"Unexpected error: {type(e).__name__}")
            print(f"Error message: {str(e)}")
            return False

    def deleteTable(self, namespace, table_name):
        """
        Delete a table from Dune Analytics.

        Args:
            namespace: Namespace where the table is located
            table_name: Name of the table to delete

        Returns:
            bool: True if deletion was successful, False otherwise
        """
        try:
            print(f"Deleting table: {namespace}.{table_name}")

            result = self.dune.delete_table(namespace=namespace, table_name=table_name)

            if result:
                if isinstance(result, dict) and "message" in result:
                    print(f"Table deleted successfully: {namespace}.{table_name}")
                    print(f"Server message: {result['message']}")
                    return True
                else:
                    print(f"Table deleted successfully: {namespace}.{table_name}")
                    print(f"Server response: {result}")
                    return True
            else:
                print(f"Table deletion failed: {namespace}.{table_name}")
                print("Reason: Table does not exist or server internal error")
                return False

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code  # HTTP status code from error response

            if status_code == 404:  # Not found error code
                print(f"Table does not exist: {namespace}.{table_name}")
                print(f"HTTP status code: {status_code}")
            elif status_code == 500:  # Internal server error code
                print("Server internal error")
                print(f"HTTP status code: {status_code}")
            else:
                print(f"HTTP error: {status_code}")

            try:
                error_detail = e.response.json()
                print(f"Error details: {error_detail}")
            except:
                print(f"Response content: {e.response.text}")

            return False

        except Exception as e:
            print(f"Unexpected error: {type(e).__name__}")
            print(f"Error message: {str(e)}")
            return False

    def clearTable(self, namespace, table_name):
        """
        Clear a Dune Analytics table.

        This function attempts to clear a table using the DuneClient method if available,
        otherwise falls back to a direct API call to the Dune Analytics endpoint.

        Args:
            namespace: Namespace of the table
            table_name: Name of the table to clear

        Returns:
            bool: True if table was cleared successfully, False otherwise
        """
        try:
            if hasattr(self.dune, "clear_table"):
                print(f"Clearing table: {namespace}.{table_name}")
                result = self.dune.clear_table(namespace, table_name)

                if result:
                    print("Table cleared successfully")
                    return True
                else:
                    print("Table clearing failed")
                    return False
            else:
                print(f"Clearing table: {namespace}.{table_name} (using direct API)")
                url = f"https://api.dune.com/api/v1/table/{namespace}/{table_name}/clear"  # Dune API endpoint for table clearing
                headers = {
                    "X-DUNE-API-KEY": self.api_key
                }  # Authentication header with API key
                response = requests.post(
                    url, headers=headers, timeout=30
                )  # Request timeout in seconds

                if response.status_code == 200:  # Success status code
                    print("Table cleared successfully")
                    return True
                else:
                    print(f"Table clearing failed - HTTP {response.status_code}")
                    return False

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error {e.response.status_code}: {e.response.text}")
            return False
        except Exception as e:
            print(f"Error: {str(e)}")
            return False

    def insertCsvToTable(self, csv_file_path, namespace, table_name):
        """
        Insert CSV data into a Dune Analytics table by directly calling the API.

        This function bypasses the DuneClient and makes a direct HTTP POST request
        to the Dune Analytics API endpoint to insert CSV file data into a specified table.

        Args:
            csv_file_path: File path to the CSV file to be inserted
            namespace: Namespace of the target table
            table_name: Name of the target table

        Returns:
            bool: True if data insertion was successful, False otherwise
        """
        if not os.path.exists(csv_file_path):
            print(f"File not found: {csv_file_path}")
            return False

        url = f"https://api.dune.com/api/v1/table/{namespace}/{table_name}/insert"  # Dune API endpoint for table insertion

        headers = {
            "X-Dune-API-Key": self.api_key,  # API authentication header
            "Content-Type": "text/csv",  # CSV content type specification
            "Accept": "application/json",  # Expected response format
            "Accept-Encoding": "identity",  # Encoding preference
        }

        file_size = os.path.getsize(csv_file_path)
        print(f"File size: {file_size / (1024*1024):.2f} MB")

        try:
            print(f"Uploading to: {url}")

            with open(csv_file_path, "rb") as f:
                response = requests.post(
                    url,
                    headers=headers,
                    data=f,
                    timeout=600,  # Request timeout in seconds (10 minutes)
                )

            if response.status_code == 200:  # Success status code
                result = response.json()
                print("Upload successful")
                print(f"Rows written: {result.get('rows_written', 'N/A')}")
                print(f"Bytes written: {result.get('bytes_written', 'N/A')}")
                return True
            else:
                print(f"Upload failed: {response.status_code}")
                print(f"Response: {response.text}")
                return False

        except requests.exceptions.ReadTimeout:
            print("Request timeout")
            return False
        except Exception as e:
            print(f"Error: {str(e)}")
            return False

    def queryRowDataByTableId(self, dune_table_id, row_name) -> List[str]:
        """
        Query and extract specific row data from a Dune Analytics table.

        This method retrieves the latest query results from a Dune table using the provided
        table ID and extracts values from a specified column across all rows in the result set.

        Args:
            dune_table_id: ID of the Dune table to query
            row_name: Name of the column to extract data from

        Returns:
            List[str]: List of extracted values from the specified column, or empty list if error occurs
        """
        try:
            query_result = self.dune.get_latest_result(dune_table_id)

            extracted_values = []  # Container for storing extracted column values

            if hasattr(query_result, "result") and hasattr(query_result.result, "rows"):
                for row in query_result.result.rows:
                    if row_name in row:
                        extracted_values.append(row[row_name])

            total_rows = (
                len(query_result.result.rows)
                if hasattr(query_result, "result")
                and hasattr(query_result.result, "rows")
                else 0
            )  # Total number of rows returned from query result

            print("\n=== Data Extraction Summary ===")
            print(f"Total rows: {total_rows}")
            print(f"Total values extracted: {len(extracted_values)}")

            return extracted_values

        except Exception as e:
            print(f"Error fetching data from Dune Analytics: {str(e)}")
            return []
