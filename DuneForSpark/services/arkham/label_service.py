import csv
import os
from datetime import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from typing import List, Tuple, Optional
from dataclasses import dataclass

from .arkham_api import ArkhamApi
from .label_model import WalletLabel


@dataclass
class ProcessResult:
    """Data class for processing results"""

    address: str
    wallet_label: Optional[WalletLabel]
    is_success: bool
    error_message: Optional[str] = None


class LabelService:
    """
    Service for batch processing wallet labels from Arkham Intelligence API.

    This class provides functionality to query wallet labels for multiple addresses
    concurrently with retry mechanism, process the results, and export them to CSV format.
    It manages API interactions, thread-safe operations, and file export capabilities.
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.arkm.com",
        max_workers: int = 5,
        max_retries: int = 10,
    ):
        """
        Initialize the LabelService.

        Args:
            api_key: API key for authentication with Arkham Intelligence
            base_url: Base URL for the Arkham API endpoint
            request_delay: Delay in seconds between consecutive API requests
            max_workers: Maximum number of concurrent threads for batch processing
            max_retries: Maximum number of retry attempts for failed addresses
        """
        self.arkham_api = ArkhamApi(api_key, base_url)
        self.max_workers = max_workers
        self.max_retries = max_retries
        # Use fine-grained locks for better thread safety
        self.results_lock = threading.Lock()
        self.failed_lock = threading.Lock()

    def process_single_address(
        self, address: str, index: int, total: int
    ) -> ProcessResult:
        """
        Process a single wallet address and retrieve its label data.

        This method queries the Arkham API for label information about a specific
        wallet address and handles both successful and failed requests by returning
        ProcessResult objects with detailed status information.

        Args:
            address: Wallet address to process
            index: Current index in the batch (for logging purposes)
            total: Total number of addresses in the batch (for logging purposes)

        Returns:
            ProcessResult: Contains wallet label, success status, and error info
        """
        try:
            wallet_label = self.arkham_api.get_label(address)

            if wallet_label:
                print(f"[{index}/{total}] ✅ Success - {address}")
                return ProcessResult(
                    address=address, wallet_label=wallet_label, is_success=True
                )
            else:
                print(f"[{index}/{total}] ❌ Failed - {address}")
                return ProcessResult(
                    address=address,
                    wallet_label=None,
                    is_success=False,
                    error_message="API returned no data",
                )
        except Exception as e:
            print(f"[{index}/{total}] ❌ Error - {address}: {str(e)}")
            return ProcessResult(
                address=address,
                wallet_label=None,
                is_success=False,
                error_message=str(e),
            )

    def process_batch_single_round(
        self, addresses: List[str], round_number: int
    ) -> Tuple[List[WalletLabel], List[str]]:
        """
        Process a batch of addresses in a single round.

        This method handles concurrent processing of addresses using ThreadPoolExecutor
        and collects results in a thread-safe manner.

        Args:
            addresses: List of addresses to process
            round_number: Current round number for logging

        Returns:
            Tuple[List[WalletLabel], List[str]]: (successful results, failed addresses)
        """
        successful_labels: List[WalletLabel] = []
        failed_addresses: List[str] = []
        total_count = len(addresses)

        print(f"\n--- Round {round_number}: Processing {total_count} addresses ---")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_address = {}
            for i, addr in enumerate(addresses, 1):
                future = executor.submit(
                    self.process_single_address, addr, i, total_count
                )
                future_to_address[future] = addr

            # Collect results
            for future in as_completed(future_to_address):
                address = future_to_address[future]
                try:
                    result = future.result(timeout=30)  # Set timeout for safety

                    # Thread-safe result processing
                    if result.is_success and result.wallet_label:
                        with self.results_lock:
                            successful_labels.append(result.wallet_label)
                    else:
                        with self.failed_lock:
                            failed_addresses.append(address)

                except Exception as e:
                    print(f"Future execution failed for {address}: {str(e)}")
                    with self.failed_lock:
                        failed_addresses.append(address)

        # Output round statistics
        success_count = len(successful_labels)
        failed_count = len(failed_addresses)
        print(
            f"Round {round_number} completed: {success_count} successful, {failed_count} failed"
        )

        return successful_labels, failed_addresses

    def batch_process_addresses_concurrent(
        self, addresses: List[str]
    ) -> List[WalletLabel]:
        """
        Process multiple wallet addresses concurrently with retry mechanism.

        This method implements a retry loop that processes failed addresses up to
        max_retries times. It distributes address processing across multiple worker
        threads to improve performance and handles failures gracefully.

        Args:
            addresses: List of wallet addresses to process

        Returns:
            List[WalletLabel]: List of label objects for all successfully processed addresses
        """
        if not addresses:
            return []

        all_successful_labels: List[WalletLabel] = []
        current_addresses = addresses.copy()
        circle_count = 0

        print(
            f"Starting batch processing of {len(addresses)} addresses with up to {self.max_retries} retries..."
        )

        while current_addresses and circle_count < self.max_retries:
            circle_count += 1

            # Process current batch
            successful_labels, failed_addresses = self.process_batch_single_round(
                current_addresses, circle_count
            )

            # Accumulate successful results
            all_successful_labels.extend(successful_labels)

            # Check if retry is needed
            if failed_addresses:
                if circle_count < self.max_retries:
                    print(
                        f"Preparing to retry {len(failed_addresses)} failed addresses..."
                    )
                    current_addresses = failed_addresses
                    # Add delay before retry
                    time.sleep(2)
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

        return all_successful_labels

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
        self, wallet_labels: List[WalletLabel], filename: str = None
    ) -> Optional[str]:
        """
        Export wallet label data to a CSV file.

        This method writes wallet label information to a CSV file in the data directory.
        If no filename is provided, a timestamped filename is automatically generated.
        The method creates the data directory if it does not exist.

        Args:
            wallet_labels: List of WalletLabel objects to export
            filename: Optional custom filename for the CSV file (without path)

        Returns:
            str: Full file path of the created CSV file, or None if export failed
        """
        if not wallet_labels:
            print("No wallet labels to export")
            return None

        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"ArcHam_Intelligence_Report_{timestamp}.csv"

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
            "no",  # Sequential row number
            "address",  # Wallet address
            "name",  # Entity name
            "type",  # Entity type classification
            "label",  # Wallet label or tag
            "isuseraddress",  # Flag indicating if address is user-owned
            "website",  # Entity website URL
            "twitter",  # Entity Twitter handle
            "crunchbase",  # Entity Crunchbase profile URL
            "linkedin",  # Entity LinkedIn profile URL
        ]

        try:
            with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers)

                for i, wallet_label in enumerate(wallet_labels, 1):
                    writer.writerow(
                        [
                            i,
                            wallet_label.address,
                            wallet_label.name,
                            wallet_label.entity_type,
                            wallet_label.label,
                            wallet_label.is_user_address,
                            wallet_label.website,
                            wallet_label.twitter,
                            wallet_label.crunchbase,
                            wallet_label.linkedin,
                        ]
                    )

            return filepath
        except Exception as e:
            print(f"Error saving CSV file: {str(e)}")
            return None

    def export_labels(self, addresses: List[str]) -> Optional[str]:
        """
        Main method to export wallet labels for a batch of addresses.

        This method orchestrates the entire workflow: validates input addresses,
        processes them concurrently through the Arkham API with retry mechanism,
        measures execution time, and exports the results to a CSV file in the data directory.

        Args:
            addresses: List of wallet addresses to process and export

        Returns:
            str: Full file path of the created CSV file, or None if export failed
        """
        if not addresses:
            print("No addresses provided")
            return None

        start_time = time.time()

        # Process addresses with retry mechanism
        wallet_labels = self.batch_process_addresses_concurrent(addresses)

        end_time = time.time()
        processing_time = end_time - start_time

        # Output final statistics
        print(f"\n{'='*60}")
        print(f"Processing Summary:")
        print(f"  Total addresses: {len(addresses)}")
        print(f"  Successfully processed: {len(wallet_labels)}")
        print(f"  Failed addresses: {len(addresses) - len(wallet_labels)}")
        print(f"  Success rate: {len(wallet_labels)/len(addresses)*100:.1f}%")
        print(f"  Processing time: {processing_time:.2f} seconds")
        print(f"{'='*60}\n")

        # Export to CSV
        if wallet_labels:
            csv_path = self.export_to_csv(wallet_labels)
            if csv_path:
                print(f"✅ CSV file created: {csv_path}")
                return csv_path
        else:
            print("❌ No successful results to export")

        return None
