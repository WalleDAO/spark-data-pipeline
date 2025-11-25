from dune_client.client import DuneClient
from typing import List


def queryDuneRowDataByTableId(dune, dune_table_id, row_name) -> List[str]:
    """
    Query and extract specific row data from a Dune Analytics table.

    This function retrieves the latest query results from a Dune table and extracts
    values from a specified column. If no column name is provided, it defaults to
    extracting user addresses.

    Args:
        dune: DuneClient object for API communication
        dune_table_id: ID of the Dune table to query
        row_name: Name of the column to extract data from

    Returns:
        List[str]: List of extracted values from the specified column, or empty list if error occurs
    """

    default_row_name = "user_addr"  # Default column name for user addresses
    if not row_name:
        row_name = default_row_name

    try:
        query_result = dune.get_latest_result(dune_table_id)

        extracted_values = []  # List to store extracted column values

        if hasattr(query_result, "result") and hasattr(query_result.result, "rows"):
            for row in query_result.result.rows:
                if row_name in row:
                    extracted_values.append(row[row_name])

        total_rows = (
            len(query_result.result.rows)
            if hasattr(query_result, "result") and hasattr(query_result.result, "rows")
            else 0
        )  # Total number of rows in result

        print("\n=== Data Extraction Summary ===")
        print(f"Total rows: {total_rows}")
        print(f"Total values extracted: {len(extracted_values)}")

        return extracted_values

    except Exception as e:
        print(f"Error fetching data from Dune Analytics: {str(e)}")
        return []
