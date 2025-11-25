import requests
import time
from arkham_service.wallet_portfolio_model import WalletPortfolio
import threading
import csv
import os
from datetime import datetime
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

ARKHAM_URL = "https://api.arkm.com"  # Base URL for Arkham API
MAX_WORKERS = 10  # Number of concurrent threads
REQUEST_DELAY = 0.05  # Request delay in seconds
BATCH_SIZE = 1000  # Number of addresses to process in each batch

lock = threading.Lock()
thread_local = threading.local()


def get_session():
    """
    Get thread-local session object to improve connection reuse efficiency.
    Returns a requests.Session object that is unique to the current thread.
    """
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session


def getArkhamPortfolio(api_key, address, time_param=None):
    """
    Retrieve portfolio data for a single address from Arkham API.

    Args:
        api_key: Arkham API key for authentication
        address: Blockchain address to query
        time_param: Optional timestamp in milliseconds for historical data

    Returns:
        WalletPortfolio object on success, None on failure
    """
    if time_param is None:
        time_param = int(time.time() * 1000)

    url = f"{ARKHAM_URL}/portfolio/address/{address}"

    params = {"time": time_param}
    headers = {"Accept": "application/json", "API-Key": api_key}

    try:
        time.sleep(REQUEST_DELAY)

        session = get_session()
        response = session.get(url, params=params, headers=headers, timeout=15)

        if response.status_code == 200:
            response_data = response.json()
            wallet_portfolio = WalletPortfolio.from_response(address, response_data)
            return wallet_portfolio

        elif response.status_code == 400:
            print(
                f"[Error 400] Address: {address} - Request parameter error: {response.text}"
            )
        elif response.status_code == 401:
            print(
                f"[Error 401] Address: {address} - Unauthorized access: {response.text}"
            )
        elif response.status_code == 429:
            print(
                f"[Error 429] Address: {address} - Too many requests, API rate limit: {response.text}"
            )
            time.sleep(2)
        elif response.status_code == 500:
            print(
                f"[Error 500] Address: {address} - Server internal error: {response.text}"
            )
        else:
            print(
                f"[Error {response.status_code}] Address: {address} - {response.text}"
            )

    except requests.exceptions.Timeout:
        print(f"[Timeout] Address: {address} - Request timeout")
    except requests.exceptions.ConnectionError:
        print(f"[Connection Error] Address: {address} - Connection failed")
    except requests.exceptions.RequestException as e:
        print(f"[Request Exception] Address: {address} - {str(e)}")
    except Exception as e:
        print(f"[Unknown Exception] Address: {address} - {str(e)}")

    return None


def process_single_address(
    address: str, api_key: str, time_param, index: int, total: int
) -> Tuple[bool, WalletPortfolio]:
    """
    Process a single address and return success status with portfolio data.

    Args:
        address: Blockchain address to query
        api_key: Arkham API key
        time_param: Timestamp for data retrieval
        index: Current processing index
        total: Total number of addresses

    Returns:
        Tuple containing success flag (bool) and WalletPortfolio object (or None)
    """
    try:
        wallet_portfolio = getArkhamPortfolio(api_key, address, time_param)

        if wallet_portfolio:
            print(f"[{index}/{total}] ✅ Success - {address}")
            return True, wallet_portfolio
        else:
            print(f"[{index}/{total}] ❌ Failed - {address}")
            return False, None

    except Exception as e:
        print(f"[{index}/{total}] ❌ Exception - {address}: {str(e)}")
        return False, None


def batch_process_addresses(
    addresses: List[str], api_key: str, time_param=None
) -> List[WalletPortfolio]:
    """
    Process multiple addresses in batches using thread pool for parallel execution.

    Args:
        addresses: List of blockchain addresses to query
        api_key: Arkham API key
        time_param: Optional timestamp for historical data

    Returns:
        List of successfully retrieved WalletPortfolio objects
    """
    results = []
    total_count = len(addresses)

    batches = [
        addresses[i : i + BATCH_SIZE] for i in range(0, len(addresses), BATCH_SIZE)
    ]

    for batch_index, batch in enumerate(batches):
        batch_results = []
        batch_size = len(batch)
        print(
            f"Processing batch {batch_index + 1}/{len(batches)}, containing {batch_size} addresses"
        )

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for i, addr in enumerate(batch, 1):
                global_index = batch_index * BATCH_SIZE + i
                future = executor.submit(
                    process_single_address,
                    addr,
                    api_key,
                    time_param,
                    global_index,
                    total_count,
                )
                futures.append(future)

            for future in as_completed(futures):
                try:
                    success, portfolio = future.result()
                    if success and portfolio:
                        batch_results.append(portfolio)
                except Exception as e:
                    print(f"Task execution failed: {str(e)}")

        results.extend(batch_results)

        if batch_index < len(batches) - 1:
            wait_time = 2
            print(f"Waiting {wait_time} seconds before processing next batch...")
            time.sleep(wait_time)

    return results


def export_to_csv(
    wallet_portfolios: List[WalletPortfolio], filename: str = None
) -> str:
    """
    Export wallet portfolio data to CSV file, focusing on Ethereum network tokens.

    Args:
        wallet_portfolios: List of WalletPortfolio objects to export
        filename: Optional custom filename for the CSV file

    Returns:
        Path to the created CSV file on success, None on failure
    """
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"ArcHam_portfolios_{timestamp}.csv"

    if not filename.endswith(".csv"):
        filename += ".csv"

    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    csvs_dir = os.path.join(parent_dir, "csvs")

    os.makedirs(csvs_dir, exist_ok=True)

    filepath = os.path.join(csvs_dir, filename)

    headers = [
        "address",
        "symbol",
        "balance",
        "price",
        "usd",
    ]

    row_count = 0
    try:
        with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)

            for wallet_portfolio in wallet_portfolios:
                if "ethereum" in wallet_portfolio.networks:
                    ethereum_network = wallet_portfolio.networks["ethereum"]
                    for token_id, token in ethereum_network.tokens.items():
                        writer.writerow(
                            [
                                wallet_portfolio.address,
                                token.symbol,
                                token.balance,
                                token.price,
                                token.usd,
                            ]
                        )
                        row_count += 1

        print(f"CSV file created: {filepath}")
        print(f"Total {row_count} rows written")
        return filepath

    except Exception as e:
        print(f"Error saving CSV file: {str(e)}")
        return None


def exportPortfoliosToCsv(
    address_params: List[str], api_key: str, time_param=None
) -> str:
    """
    Export portfolio data for multiple addresses to a CSV file.

    Args:
        address_params: List of addresses to query
        api_key: Arkham API key
        time_param: Optional timestamp for historical data (defaults to current time)

    Returns:
        Path to the created CSV file on success, None on failure
    """
    if not address_params:
        print("No address list provided")
        return None

    total_addresses = len(address_params)
    print(f"Starting to process {total_addresses} addresses...")

    start_time = time.time()

    results = batch_process_addresses(address_params, api_key, time_param)

    end_time = time.time()
    elapsed_time = end_time - start_time

    success_count = len(results)
    print(
        f"Processing complete: {success_count}/{total_addresses} addresses successfully retrieved"
    )
    print(f"Total time: {elapsed_time:.2f} seconds")

    if success_count == 0:
        print("No data successfully retrieved, canceling CSV export")
        return None

    filepath = export_to_csv(results)

    return filepath
