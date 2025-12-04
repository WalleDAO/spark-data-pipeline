import csv
import os
from datetime import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from typing import List, Tuple, Optional

from .arkham_api import ArkhamApi
from .label_model import WalletLabel


class LabelService:
    """
    Service for batch processing wallet labels from Arkham Intelligence API.

    This class provides functionality to query wallet labels for multiple addresses
    concurrently, process the results, and export them to CSV format. It manages
    API interactions, thread-safe operations, and file export capabilities.
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.arkm.com",  # Default Arkham API base URL
        request_delay: float = 0.05,  # Delay between requests in seconds
        max_workers: int = 5,  # Maximum number of concurrent worker threads
    ):
        """
        Initialize the LabelService.

        Args:
            api_key: API key for authentication with Arkham Intelligence
            base_url: Base URL for the Arkham API endpoint
            request_delay: Delay in seconds between consecutive API requests
            max_workers: Maximum number of concurrent threads for batch processing
        """
        self.arkham_api = ArkhamApi(api_key, base_url, request_delay)
        self.max_workers = max_workers
        self.lock = threading.Lock()  # Thread lock for thread-safe list operations

    def process_single_address(
        self, address: str, index: int, total: int
    ) -> WalletLabel:
        """
        Process a single wallet address and retrieve its label data.

        This method queries the Arkham API for label information about a specific
        wallet address and handles both successful and failed requests by returning
        either populated or empty WalletLabel objects.

        Args:
            address: Wallet address to process
            index: Current index in the batch (for logging purposes)
            total: Total number of addresses in the batch (for logging purposes)

        Returns:
            WalletLabel: Label object containing wallet data or empty object if failed
        """
        wallet_label = self.arkham_api.get_label(address)

        if wallet_label:
            print(f"[{index}/{total}] Success - {address}")
            return wallet_label
        else:
            print(f"[{index}/{total}] Failed - {address}")
            return WalletLabel.from_response(
                address, {}
            )  # Create empty WalletLabel object containing only address

    def batch_process_addresses_concurrent(
        self, addresses: List[str]
    ) -> List[WalletLabel]:
        """
        Process multiple wallet addresses concurrently using thread pool executor.

        This method distributes address processing across multiple worker threads
        to improve performance when querying large batches of addresses. Results
        are collected in a thread-safe manner using locks.

        Args:
            addresses: List of wallet addresses to process

        Returns:
            List[WalletLabel]: List of label objects for all processed addresses
        """
        wallet_labels: List[WalletLabel] = []
        total_count = len(addresses)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for i, addr in enumerate(addresses, 1):
                future = executor.submit(
                    self.process_single_address, addr, i, total_count
                )
                futures.append(future)

            for future in as_completed(futures):
                try:
                    result = future.result()
                    with self.lock:
                        wallet_labels.append(result)
                except Exception as e:
                    print(f"Task failed with error: {e}")

        return wallet_labels

    def export_to_csv(
        self, wallet_labels: List[WalletLabel], filename: str = None
    ) -> str:
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
        if filename is None:
            timestamp = datetime.now().strftime(
                "%Y%m%d_%H%M%S"
            )  # Current timestamp for auto-generated filename
            filename = f"ArcHam_Intelligence_Report_{timestamp}.csv"

        if not filename.endswith(".csv"):
            filename += ".csv"

        data_dir = os.path.join(
            os.path.dirname(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            ),
            "data",  # Data directory relative to project root
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
        processes them concurrently through the Arkham API, measures execution time,
        and exports the results to a CSV file in the data directory.

        Args:
            addresses: List of wallet addresses to process and export

        Returns:
            str: Full file path of the created CSV file, or None if export failed
        """
        if not addresses or len(addresses) == 0:
            print("No addresses provided")
            return [], ""

        start_time = time.time()  # Record start time for performance measurement
        print(f"Starting batch processing of {len(addresses)} addresses...")

        wallet_labels = self.batch_process_addresses_concurrent(addresses)

        end_time = time.time()  # Record end time for performance measurement
        print(f"Processing completed in {end_time - start_time:.2f} seconds")

        csv_path = self.export_to_csv(wallet_labels)
        if csv_path:
            print(f"CSV file created: {csv_path}")
            return csv_path

        return None
