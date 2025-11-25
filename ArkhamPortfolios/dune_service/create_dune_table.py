from dune_client.client import DuneClient
import requests


def createDuneTable(api_key, namespace, table_name, description):
    """
    Create a new table in Dune Analytics with predefined schema.

    This function initializes a DuneClient and creates a table with columns for
    storing cryptocurrency wallet data including address, symbol, balance, price,
    and USD value.

    Args:
        api_key: Dune API key for authentication
        namespace: Namespace where the table will be created
        table_name: Name of the table to create
        description: Description of the table's purpose

    Returns:
        bool: True if table creation was successful or already exists, False otherwise
    """
    dune = DuneClient(api_key)
    try:
        table = dune.create_table(
            namespace=namespace,
            table_name=table_name,
            description=description,
            schema=[
                {"name": "address", "type": "varbinary"},  # Wallet address column
                {"name": "symbol", "type": "varchar"},  # Token symbol column
                {"name": "balance", "type": "varchar"},  # Token balance column
                {"name": "price", "type": "varchar"},  # Token price column
                {"name": "usd", "type": "varchar"},  # USD value column
            ],
            is_private=False,  # Table visibility setting
        )

        if hasattr(table, "already_existed") and table.already_existed:
            print(f"Table already exists: {table.full_name}")
            print(f"Status: {table.message}")
        else:
            print(f"Table created successfully: {table.full_name}")
            print(f"Status: {table.message}")
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
