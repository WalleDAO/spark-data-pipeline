import requests
import time
import threading
from typing import Optional, Dict, Any
from .portfolio_model import WalletPortfolio
from .label_model import WalletLabel


class ArkhamApi:
    """
    Client for interacting with the Arkham Intelligence API.

    This class provides methods to query wallet portfolio data and wallet labels
    from the Arkham Intelligence API. It manages API authentication, request delays,
    and thread-safe session management.
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.arkm.com",  # Default Arkham API base URL
        request_delay: float = 0.05,  # Delay between requests in seconds
    ):
        """
        Initialize the ArkhamApi client.

        Args:
            api_key: API key for authentication with Arkham Intelligence
            base_url: Base URL for the Arkham API endpoint
            request_delay: Delay in seconds between consecutive API requests
        """
        self.api_key = api_key
        self.base_url = base_url
        self.request_delay = request_delay
        self.thread_local = (
            threading.local()
        )  # Thread-local storage for session management

    def _get_session(self):
        """
        Get or create a thread-local requests session.

        This method ensures each thread has its own session object for thread-safe
        HTTP communication with the API.

        Returns:
            requests.Session: Thread-local session object
        """
        if not hasattr(self.thread_local, "session"):
            self.thread_local.session = requests.Session()
        return self.thread_local.session

    def get_portfolio(
        self, address: str, time_param: Optional[int] = None
    ) -> Optional[WalletPortfolio]:
        """
        Retrieve wallet portfolio data for a given address.

        This method queries the Arkham API to get portfolio information including
        assets, balances, and other portfolio details for a specified wallet address.

        Args:
            address: Wallet address to query
            time_param: Timestamp in milliseconds for historical data (defaults to current time)

        Returns:
            WalletPortfolio: Portfolio object containing wallet data, or None if request fails
        """
        if time_param is None:
            time_param = int(time.time() * 1000)  # Current timestamp in milliseconds

        url = f"{self.base_url}/portfolio/address/{address}"  # Portfolio endpoint URL

        params = {"time": time_param}  # Query parameters
        headers = {
            "Accept": "application/json",
            "API-Key": self.api_key,
        }  # Request headers with authentication

        try:
            time.sleep(self.request_delay)

            session = self._get_session()
            response = session.get(
                url, params=params, headers=headers, timeout=15
            )  # Request timeout in seconds

            if response.status_code == 200:  # Success status code
                response_data = response.json()
                wallet_portfolio = WalletPortfolio.from_response(address, response_data)
                return wallet_portfolio

            elif response.status_code == 400:  # Bad request error code
                print(
                    f"Error 400 Address: {address} - Request parameter error: {response.text}"
                )
            elif response.status_code == 401:  # Unauthorized error code
                print(
                    f"Error 401 Address: {address} - Unauthorized access: {response.text}"
                )
            elif response.status_code == 429:  # Rate limit error code
                print(
                    f"Error 429 Address: {address} - Too many requests, API rate limit: {response.text}"
                )
                time.sleep(2)  # Backoff delay for rate limiting
            elif response.status_code == 500:  # Internal server error code
                print(
                    f"Error 500 Address: {address} - Server internal error: {response.text}"
                )
            else:
                print(
                    f"Error {response.status_code} Address: {address} - {response.text}"
                )

        except requests.exceptions.Timeout:
            print(f"Timeout Address: {address} - Request timeout")
        except requests.exceptions.ConnectionError:
            print(f"Connection Error Address: {address} - Connection failed")
        except requests.exceptions.RequestException as e:
            print(f"Request Exception Address: {address} - {str(e)}")
        except Exception as e:
            print(f"Unknown Exception Address: {address} - {str(e)}")

        return None

    def get_label(self, address: str) -> Optional[WalletLabel]:
        """
        Retrieve wallet label and intelligence data for a given address.

        This method queries the Arkham API to get wallet labels, tags, and other
        intelligence information for a specified wallet address.

        Args:
            address: Wallet address to query

        Returns:
            WalletLabel: Label object containing wallet intelligence data, or None if request fails
        """
        url = f"{self.base_url}/intelligence/address/{address}/all"  # Intelligence endpoint URL
        headers = {
            "Accept": "application/json",
            "API-Key": self.api_key,
        }  # Request headers with authentication

        try:
            time.sleep(self.request_delay)

            session = self._get_session()
            response = session.get(
                url, headers=headers, timeout=15
            )  # Request timeout in seconds

            if response.status_code == 200:  # Success status code
                response_data = response.json()
                wallet_label = WalletLabel.from_response(address, response_data)
                return wallet_label

            elif response.status_code == 400:  # Bad request error code
                print(
                    f"Error 400 Address: {address} - Request parameter error: {response.text}"
                )
            elif response.status_code == 401:  # Unauthorized error code
                print(
                    f"Error 401 Address: {address} - Unauthorized access: {response.text}"
                )
            elif response.status_code == 429:  # Rate limit error code
                print(
                    f"Error 429 Address: {address} - Too many requests, API rate limit: {response.text}"
                )
                time.sleep(2)  # Backoff delay for rate limiting
            elif response.status_code == 500:  # Internal server error code
                print(
                    f"Error 500 Address: {address} - Server internal error: {response.text}"
                )
            else:
                print(
                    f"Error {response.status_code} Address: {address} - {response.text}"
                )

        except requests.exceptions.Timeout:
            print(f"Timeout Address: {address} - Request timeout")
        except requests.exceptions.ConnectionError:
            print(f"Connection Error Address: {address} - Connection failed")
        except requests.exceptions.RequestException as e:
            print(f"Request Exception Address: {address} - {str(e)}")
        except Exception as e:
            print(f"Unknown Exception Address: {address} - {str(e)}")

        return None
