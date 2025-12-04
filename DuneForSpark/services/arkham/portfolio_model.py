from typing import Dict
from dataclasses import dataclass


@dataclass
class Token:
    """
    Represents a cryptocurrency token with its properties.

    Attributes:
        id: Unique identifier for the token
        name: Full name of the token
        symbol: Trading symbol of the token
        balance: Amount of tokens held
        price: Current price of the token
        usd: Value in USD of the token balance
    """

    id: str
    name: str
    symbol: str
    balance: str
    price: str
    usd: str

    @classmethod
    def from_dict(cls, data: dict) -> "Token":
        """
        Create a Token instance from a dictionary.

        Args:
            data: Dictionary containing token data

        Returns:
            A new Token instance with properties populated from the dictionary
        """
        return cls(
            id=data["id"],
            name=data["name"],
            symbol=data["symbol"],
            balance=data["balance"],
            price=data["price"],
            usd=data["usd"],
        )


@dataclass
class Network:
    """
    Represents a blockchain network containing multiple tokens.

    Attributes:
        name: Name of the blockchain network (e.g., 'ethereum', 'bitcoin')
        tokens: Dictionary of tokens in this network, keyed by token ID
    """

    name: str
    tokens: Dict[str, Token]

    @classmethod
    def from_dict(cls, name: str, data: dict) -> "Network":
        """
        Create a Network instance from a dictionary.

        Args:
            name: Name of the network
            data: Dictionary containing network token data

        Returns:
            A new Network instance with tokens populated from the dictionary
        """
        tokens = {}
        for token_id, token_data in data.items():
            tokens[token_id] = Token.from_dict(token_data)
        return cls(name=name, tokens=tokens)


@dataclass
class WalletPortfolio:
    """
    Represents a wallet's complete portfolio across multiple blockchain networks.

    Attributes:
        address: The blockchain address of the wallet
        networks: Dictionary of networks in this portfolio, keyed by network name
    """

    address: str
    networks: Dict[str, Network]

    @classmethod
    def from_response(cls, address: str, response_data: dict) -> "WalletPortfolio":
        """
        Create a WalletPortfolio instance from API response data.

        Args:
            address: The blockchain address of the wallet
            response_data: Dictionary containing portfolio data from API response

        Returns:
            A new WalletPortfolio instance with networks populated from the response data
        """
        networks = {}
        for network_name, network_data in response_data.items():
            networks[network_name] = Network.from_dict(network_name, network_data)
        return cls(address=address, networks=networks)
