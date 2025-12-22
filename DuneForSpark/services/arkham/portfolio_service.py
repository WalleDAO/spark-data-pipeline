import csv
import os
from datetime import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from typing import List, Tuple, Optional
from dataclasses import dataclass

from .arkham_api import ArkhamApi
from .portfolio_model import WalletPortfolio


@dataclass
class PortfolioProcessResult:
    """Data class for portfolio processing results"""

    address: str
    wallet_portfolio: Optional[WalletPortfolio]
    is_success: bool
    error_message: Optional[str] = None


class PortfolioService:
    """
    Service for batch processing wallet portfolios from Arkham Intelligence API.

    This class provides functionality to query wallet portfolios for multiple addresses
    concurrently with retry mechanism, process the results, and export them to CSV format.
    It manages API interactions, thread-safe operations, and file export capabilities.
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.arkm.com",
        max_workers: int = 5,
        max_retries: int = 10,
        batch_delay: float = 2.0,
    ):
        """
        Initialize the PortfolioService.

        Args:
            api_key: API key for authentication with Arkham Intelligence
            base_url: Base URL for the Arkham API endpoint
            request_delay: Delay in seconds between consecutive API requests
            max_workers: Maximum number of concurrent threads for batch processing
            max_retries: Maximum number of retry attempts for failed addresses
            batch_delay: Delay in seconds between retry rounds
        """
        self.arkham_api = ArkhamApi(api_key, base_url)
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.batch_delay = batch_delay
        # Use fine-grained locks for better thread safety
        self.results_lock = threading.Lock()
        self.failed_lock = threading.Lock()

    def process_single_address(
        self, address: str, time_param: Optional[int], index: int, total: int
    ) -> PortfolioProcessResult:
        """
        Process a single wallet address and retrieve its portfolio data.

        This method queries the Arkham API for portfolio information about a specific
        wallet address and handles both successful and failed requests by returning
        PortfolioProcessResult objects with detailed status information.

        Args:
            address: Wallet address to process
            time_param: Optional timestamp parameter for historical data query
            index: Current index in the batch (for logging purposes)
            total: Total number of addresses in the batch (for logging purposes)

        Returns:
            PortfolioProcessResult: Contains portfolio data, success status, and error info
        """
        try:
            wallet_portfolio = self.arkham_api.get_portfolio(address, time_param)

            if wallet_portfolio:
                print(f"[{index}/{total}] ✅ Success - {address}")
                return PortfolioProcessResult(
                    address=address, wallet_portfolio=wallet_portfolio, is_success=True
                )
            else:
                print(f"[{index}/{total}] ❌ Failed - {address}")
                return PortfolioProcessResult(
                    address=address,
                    wallet_portfolio=None,
                    is_success=False,
                    error_message="API returned no portfolio data",
                )
        except Exception as e:
            print(f"[{index}/{total}] ❌ Error - {address}: {str(e)}")
            return PortfolioProcessResult(
                address=address,
                wallet_portfolio=None,
                is_success=False,
                error_message=str(e),
            )

    def process_batch_single_round(
        self, addresses: List[str], time_param: Optional[int], round_number: int
    ) -> Tuple[List[WalletPortfolio], List[str]]:
        """
        Process a batch of addresses in a single round.

        This method handles concurrent processing of addresses using ThreadPoolExecutor
        and collects results in a thread-safe manner.

        Args:
            addresses: List of addresses to process
            time_param: Optional timestamp parameter for historical data query
            round_number: Current round number for logging

        Returns:
            Tuple[List[WalletPortfolio], List[str]]: (successful results, failed addresses)
        """
        successful_portfolios: List[WalletPortfolio] = []
        failed_addresses: List[str] = []
        total_count = len(addresses)

        print(f"\n--- Round {round_number}: Processing {total_count} addresses ---")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_address = {}
            for i, addr in enumerate(addresses, 1):
                future = executor.submit(
                    self.process_single_address, addr, time_param, i, total_count
                )
                future_to_address[future] = addr

            # Collect results
            for future in as_completed(future_to_address):
                address = future_to_address[future]
                try:
                    result = future.result(
                        timeout=60
                    )  # Longer timeout for portfolio API

                    # Thread-safe result processing
                    if result.is_success and result.wallet_portfolio:
                        with self.results_lock:
                            successful_portfolios.append(result.wallet_portfolio)
                    else:
                        with self.failed_lock:
                            failed_addresses.append(address)

                except Exception as e:
                    print(f"Future execution failed for {address}: {str(e)}")
                    with self.failed_lock:
                        failed_addresses.append(address)

        # Output round statistics
        success_count = len(successful_portfolios)
        failed_count = len(failed_addresses)
        print(
            f"Round {round_number} completed: {success_count} successful, {failed_count} failed"
        )

        return successful_portfolios, failed_addresses

    def batch_process_addresses_concurrent(
        self, addresses: List[str], time_param: Optional[int] = None
    ) -> List[WalletPortfolio]:
        """
        Process multiple wallet addresses concurrently with retry mechanism.

        This method implements a retry loop that processes failed addresses up to
        max_retries times. It distributes address processing across multiple worker
        threads to improve performance and handles failures gracefully.

        Args:
            addresses: List of wallet addresses to process
            time_param: Optional timestamp parameter for historical data query

        Returns:
            List[WalletPortfolio]: List of portfolio objects for all successfully processed addresses
        """
        if not addresses:
            return []

        all_successful_portfolios: List[WalletPortfolio] = []
        current_addresses = addresses.copy()
        circle_count = 0

        print(
            f"Starting batch processing of {len(addresses)} addresses with up to {self.max_retries} retries..."
        )

        while current_addresses and circle_count < self.max_retries:
            circle_count += 1

            # Process current batch
            successful_portfolios, failed_addresses = self.process_batch_single_round(
                current_addresses, time_param, circle_count
            )

            # Accumulate successful results
            all_successful_portfolios.extend(successful_portfolios)

            # Check if retry is needed
            if failed_addresses:
                if circle_count < self.max_retries:
                    print(
                        f"Preparing to retry {len(failed_addresses)} failed addresses..."
                    )
                    current_addresses = failed_addresses
                    # Add delay before retry
                    time.sleep(self.batch_delay)
                else:
                    # Maximum retries reached
                    print(f"\n⚠️  Maximum retries ({self.max_retries}) reached!")
                    self._print_final_failed_addresses(failed_addresses)
                    break
            else:
                print(
                    f"✅ All addresses processed successfully after {circle_count} rounds!"
                )
                break

        return all_successful_portfolios

    def _print_final_failed_addresses(self, failed_addresses: List[str]) -> None:
        """
        Print final failed addresses that couldn't be processed.

        Args:
            failed_addresses: List of addresses that failed after all retries
        """
        if failed_addresses:
            print(
                f"\n❌ {len(failed_addresses)} addresses failed after {self.max_retries} retries:"
            )
            print("Failed addresses that may have persistent issues:")
            for i, addr in enumerate(failed_addresses, 1):
                print(f"  {i:3d}. {addr}")
            print()

    def export_to_csv(
        self, wallet_portfolios: List[WalletPortfolio], filename: str = None
    ) -> Optional[str]:
        """
        Export wallet portfolio data to a CSV file.

        This method writes portfolio data from multiple wallets to a CSV file,
        organizing data by blockchain network and token. It automatically generates
        a timestamped filename if none is provided and creates the data directory
        if it does not exist.

        Args:
            wallet_portfolios: List of WalletPortfolio objects to export
            filename: Optional custom filename for the CSV file (without path)

        Returns:
            str: Full file path of the created CSV file, or None if export failed
        """
        if not wallet_portfolios:
            print("No wallet portfolios to export")
            return None

        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"ArcHam_portfolios_{timestamp}.csv"

        if not filename.endswith(".csv"):
            filename += ".csv"

        # Create data directory
        data_dir = os.path.join(
            os.path.dirname(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            ),
            "data",
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

        # Supported blockchain networks
        chains = ["arbitrum_one", "ethereum", "base", "optimism"]
        row_count = 0

        try:
            with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers)

                for wallet_portfolio in wallet_portfolios:
                    for chain in chains:
                        if chain in wallet_portfolio.networks:
                            network = wallet_portfolio.networks[chain]
                            # Convert 'arbitrum_one' to 'arbitrum' for display
                            display_chain = (
                                "arbitrum" if chain == "arbitrum_one" else chain
                            )
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

            print(f"Total {row_count} rows written")
            return filepath

        except Exception as e:
            print(f"Error saving CSV file: {str(e)}")
            return None

    def export_portfolios(
        self,
        addresses: List[str],
        time_param: Optional[int] = None,
        filename: Optional[str] = None,
    ) -> Optional[str]:
        """
        Main method to export wallet portfolios for a batch of addresses.

        This method orchestrates the entire workflow: validates input addresses,
        processes them concurrently through the Arkham API with retry mechanism,
        measures execution time, and exports the results to a CSV file in the data directory.

        Args:
            addresses: List of wallet addresses to process and export
            time_param: Optional timestamp parameter for historical data query
            filename: Optional custom filename for the CSV file

        Returns:
            str: Full file path of the created CSV file, or None if export failed
        """
        if not addresses:
            print("No addresses provided")
            return None

        start_time = time.time()

        # Process addresses with retry mechanism
        wallet_portfolios = self.batch_process_addresses_concurrent(
            addresses, time_param
        )

        end_time = time.time()
        processing_time = end_time - start_time

        # Output final statistics
        print(f"\n{'='*60}")
        print(f"Portfolio Processing Summary:")
        print(f"  Total addresses: {len(addresses)}")
        print(f"  Successfully processed: {len(wallet_portfolios)}")
        print(f"  Failed addresses: {len(addresses) - len(wallet_portfolios)}")
        print(f"  Success rate: {len(wallet_portfolios)/len(addresses)*100:.1f}%")
        print(f"  Processing time: {processing_time:.2f} seconds")
        print(f"{'='*60}\n")

        # Export to CSV
        if wallet_portfolios:
            csv_path = self.export_to_csv(wallet_portfolios, filename)
            if csv_path:
                print(f"✅ CSV file created: {csv_path}")
                return csv_path
        else:
            print("❌ No successful results to export")

        return None
