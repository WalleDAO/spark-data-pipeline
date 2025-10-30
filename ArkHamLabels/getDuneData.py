from dune_client.client import DuneClient
from typing import List
from config import DUNE_TABLE_ID

def getDuneData(dune) -> List[str]:
    try:
        query_result = dune.get_latest_result(DUNE_TABLE_ID)

        # Extract all user_addr from the result
        user_addresses = []

        # Access the rows data
        if hasattr(query_result, 'result') and hasattr(query_result.result, 'rows'):
            for row in query_result.result.rows:
                if 'user_addr' in row:
                    user_addresses.append(row['user_addr'])
        
        print(f"\n=== user_addresses Summary ===")
        print(f"Total rows: {len(query_result.result.rows) if hasattr(query_result, 'result') and hasattr(query_result.result, 'rows') else 0}")
        print(f"Total user addresses extracted: {len(user_addresses)}")
        
        return user_addresses
        
    except Exception as e:
        print(f"Error fetching data from Dune Analytics: {str(e)}")
        return []
