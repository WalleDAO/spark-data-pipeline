import time
from .portfolio_model import WalletPortfolio
from .arkham_api import ArkhamApi
import csv
import os
from datetime import datetime
from typing import List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

_MAX_WORKERS = 10  # Maximum number of concurrent worker threads
_BATCH_SIZE = 1000  # Number of addresses to process in each batch


def _process_single_address(
    arkham_api: ArkhamApi, address: str, time_param, index: int, total: int
) -> Tuple[bool, Optional[WalletPortfolio]]:
    """
    Process a single wallet address and retrieve its portfolio data.

    This function queries the Arkham API for portfolio information about a specific
    wallet address and handles both successful and failed requests, returning a tuple
    indicating success status and the portfolio data if available.

    Args:
        arkham_api: ArkhamApi instance for API communication
        address: Wallet address to process
        time_param: Timestamp parameter for historical data query
        index: Current index in the batch (for logging)
        total: Total number of addresses in the batch (for logging)

    Returns:
        Tuple[bool, Optional[WalletPortfolio]]: Success flag and portfolio object if successful
    """
    try:
        wallet_portfolio = arkham_api.get_portfolio(address, time_param)

        if wallet_portfolio:
            print(f"[{index}/{total}] Success - {address}")
            return True, wallet_portfolio
        else:
            print(f"[{index}/{total}] Failed - {address}")
            return False, None

    except Exception as e:
        print(f"[{index}/{total}] Exception - {address}: {str(e)}")
        return False, None


def _batch_process_addresses(
    addresses: List[str], arkham_api: ArkhamApi, time_param=None
) -> List[WalletPortfolio]:
    """
    Process multiple wallet addresses in batches using concurrent threads.

    This function divides the address list into batches and processes each batch
    concurrently using a thread pool. It includes inter-batch delays to avoid
    overwhelming the API and collects successful results.

    Args:
        addresses: List of wallet addresses to process
        arkham_api: ArkhamApi instance for API communication
        time_param: Optional timestamp parameter for historical data query

    Returns:
        List[WalletPortfolio]: List of successfully retrieved portfolio objects
    """
    results = []
    total_count = len(addresses)

    batches = [
        addresses[i : i + _BATCH_SIZE] for i in range(0, len(addresses), _BATCH_SIZE)
    ]

    for batch_index, batch in enumerate(batches):
        batch_results = []
        batch_size = len(batch)
        print(
            f"Processing batch {batch_index + 1}/{len(batches)}, containing {batch_size} addresses"
        )

        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
            futures = []
            for i, addr in enumerate(batch, 1):
                global_index = batch_index * _BATCH_SIZE + i
                future = executor.submit(
                    _process_single_address,
                    arkham_api,
                    addr,
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
            wait_time = 2  # Delay in seconds between batches
            print(f"Waiting {wait_time} seconds before processing next batch...")
            time.sleep(wait_time)

    return results


def _export_to_csv(
    wallet_portfolios: List[WalletPortfolio], filename: str = None
) -> Optional[str]:
    """
    Export wallet portfolio data to a CSV file.

    This function writes portfolio data from multiple wallets to a CSV file,
    organizing data by blockchain network and token. It automatically generates
    a timestamped filename if none is provided and creates the data directory
    if it does not exist. The function also converts chain names for display
    (e.g., 'arbitrum_one' to 'arbitrum').

    Args:
        wallet_portfolios: List of WalletPortfolio objects to export
        filename: Optional custom filename for the CSV file (without path)

    Returns:
        str: Full file path of the created CSV file, or None if export failed
    """
    if filename is None:
        timestamp = datetime.now().strftime(
            "%Y%m%d_%H%M%S"
        )  # Current timestamp for auto-generated filename
        filename = f"ArcHam_portfolios_{timestamp}.csv"

    if not filename.endswith(".csv"):
        filename += ".csv"

    data_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "data",  # Data directory relative to project root
    )

    os.makedirs(data_dir, exist_ok=True)

    filepath = os.path.join(data_dir, filename)

    headers = [
        "chain",  # Blockchain network name
        "address",  # Wallet address
        "symbol",  # Token symbol
        "balance",  # Token balance amount
        "price",  # Token price in USD
        "usd",  # Total value in USD
    ]
    chains = [
        "arbitrum_one",
        "ethereum",
        "base",
        "optimism",
    ]  # Supported blockchain networks
    row_count = 0
    try:
        with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)

            for wallet_portfolio in wallet_portfolios:
                for (
                    chain
                ) in chains:  # Iterate through chains to check wallet data availability
                    if chain in wallet_portfolio.networks:
                        network = wallet_portfolio.networks[chain]
                        display_chain = (
                            "arbitrum" if chain == "arbitrum_one" else chain
                        )  # Convert 'arbitrum_one' to 'arbitrum' for display
                        for token_id, token in network.tokens.items():
                            writer.writerow(
                                [
                                    display_chain,
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


def export_Portfolios(
    address_params: List[str],
    arkham_api: ArkhamApi,
    time_param: Optional[int] = None,
    filename: Optional[str] = None,
) -> Optional[str]:
    """
    Main function to export wallet portfolios for a batch of addresses.

    This function orchestrates the entire workflow: validates input addresses,
    processes them in batches using concurrent threads, measures execution time,
    and exports the results to a CSV file. It provides detailed logging of the
    processing progress and results.

    Args:
        address_params: List of wallet addresses to process and export
        arkham_api: ArkhamApi instance for API communication
        time_param: Optional timestamp parameter for historical data query
        filename: Optional custom filename for the CSV file

    Returns:
        str: Full file path of the created CSV file, or None if no data was retrieved
    """
    if not address_params:
        print("No address list provided")
        return None

    total_addresses = len(address_params)
    print(f"Starting to process {total_addresses} addresses...")

    start_time = time.time()  # Record start time for performance measurement

    results = _batch_process_addresses(address_params, arkham_api, time_param)

    end_time = time.time()  # Record end time for performance measurement
    elapsed_time = end_time - start_time

    success_count = len(results)
    print(
        f"Processing complete: {success_count}/{total_addresses} addresses successfully retrieved"
    )
    print(f"Total time: {elapsed_time:.2f} seconds")

    if success_count == 0:
        print("No data successfully retrieved, canceling CSV export")
        return None

    filepath = _export_to_csv(results, filename)

    return filepath


__all__ = ["export_Portfolios"]
