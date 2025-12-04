from typing import Dict, Optional
from dataclasses import dataclass


@dataclass
class ArkhamEntity:
    """
    Represents an Arkham Intelligence entity with organizational information.

    This dataclass stores metadata about an entity including its name, type,
    and various social/web presence links.
    """

    name: str
    type: str
    website: str
    twitter: str
    crunchbase: str
    linkedin: str

    @classmethod
    def from_dict(cls, data: dict) -> "ArkhamEntity":
        """
        Create an ArkhamEntity instance from a dictionary.

        Args:
            data: Dictionary containing entity information

        Returns:
            ArkhamEntity: New instance with data from dictionary
        """
        return cls(
            name=data.get("name", ""),  # Default to empty string if not present
            type=data.get("type", ""),  # Default to empty string if not present
            website=data.get("website", ""),  # Default to empty string if not present
            twitter=data.get("twitter", ""),  # Default to empty string if not present
            crunchbase=data.get(
                "crunchbase", ""
            ),  # Default to empty string if not present
            linkedin=data.get("linkedin", ""),  # Default to empty string if not present
        )


@dataclass
class ArkhamLabel:
    """
    Represents an Arkham Intelligence label for wallet classification.

    This dataclass stores a simple label name used to classify wallet addresses.
    """

    name: str

    @classmethod
    def from_dict(cls, data: dict) -> "ArkhamLabel":
        """
        Create an ArkhamLabel instance from a dictionary.

        Args:
            data: Dictionary containing label information

        Returns:
            ArkhamLabel: New instance with data from dictionary
        """
        return cls(name=data.get("name", ""))  # Default to empty string if not present


@dataclass
class ChainData:
    """
    Represents blockchain-specific data for a wallet address.

    This dataclass stores chain-specific information including the address,
    whether it's a user address, and associated Arkham entity and label data.
    """

    address: str
    is_user_address: bool
    arkham_entity: Optional[ArkhamEntity]
    arkham_label: Optional[ArkhamLabel]

    @classmethod
    def from_dict(cls, data: dict) -> "ChainData":
        """
        Create a ChainData instance from a dictionary.

        Args:
            data: Dictionary containing chain-specific data

        Returns:
            ChainData: New instance with data from dictionary
        """
        arkham_entity = None
        if "arkhamEntity" in data and data["arkhamEntity"]:
            arkham_entity = ArkhamEntity.from_dict(data["arkhamEntity"])

        arkham_label = None
        if "arkhamLabel" in data and data["arkhamLabel"]:
            arkham_label = ArkhamLabel.from_dict(data["arkhamLabel"])

        return cls(
            address=data.get("address", ""),  # Default to empty string if not present
            is_user_address=data.get(
                "isUserAddress", False
            ),  # Default to False if not present
            arkham_entity=arkham_entity,
            arkham_label=arkham_label,
        )


@dataclass
class WalletLabel:
    """
    Represents comprehensive wallet label and intelligence data across multiple blockchains.

    This dataclass aggregates wallet information from multiple chains and provides
    convenient access to entity and label data through properties, with support for
    chain priority-based selection of primary chain data.
    """

    address: str
    chains: Dict[str, ChainData]
    primary_chain_data: Optional[ChainData]

    @classmethod
    def from_response(cls, address: str, response_data: dict) -> "WalletLabel":
        """
        Create a WalletLabel instance from API response data.

        This method processes multi-chain response data and selects the primary
        chain based on a predefined priority list.

        Args:
            address: Wallet address
            response_data: Dictionary containing chain data from API response

        Returns:
            WalletLabel: New instance with parsed chain data and primary chain selected
        """
        chains = {}
        for chain_name, chain_data in response_data.items():
            if isinstance(chain_data, dict):
                chains[chain_name] = ChainData.from_dict(chain_data)

        priority_chains = [
            "ethereum",  # Highest priority blockchain
            "arbitrum_one",  # Second priority blockchain
            "polygon",  # Third priority blockchain
            "optimism",  # Fourth priority blockchain
            "base",  # Fifth priority blockchain
            "bsc",  # Sixth priority blockchain
        ]
        primary_chain_data = None

        for chain in priority_chains:
            if chain in chains:
                primary_chain_data = chains[chain]
                break

        if primary_chain_data is None and chains:
            primary_chain_data = next(
                iter(chains.values())
            )  # Select first available chain if no priority match

        return cls(
            address=address,
            chains=chains,
            primary_chain_data=primary_chain_data,
        )

    @property
    def name(self) -> str:
        """
        Get the entity name from primary chain data.

        Returns:
            str: Entity name or empty string if not available
        """
        if self.primary_chain_data and self.primary_chain_data.arkham_entity:
            return self.primary_chain_data.arkham_entity.name
        return ""

    @property
    def entity_type(self) -> str:
        """
        Get the entity type from primary chain data.

        Returns:
            str: Entity type or empty string if not available
        """
        if self.primary_chain_data and self.primary_chain_data.arkham_entity:
            return self.primary_chain_data.arkham_entity.type
        return ""

    @property
    def label(self) -> str:
        """
        Get the wallet label with fallback to other chains.

        This property first attempts to retrieve the label from the primary chain,
        then falls back to searching other chains if not found.

        Returns:
            str: Label name or empty string if not available
        """
        if self.primary_chain_data and self.primary_chain_data.arkham_label:
            return self.primary_chain_data.arkham_label.name

        for chain_data in self.chains.values():
            if chain_data.arkham_label:
                return chain_data.arkham_label.name

        return ""

    @property
    def website(self) -> str:
        """
        Get the entity website from primary chain data.

        Returns:
            str: Website URL or empty string if not available
        """
        if self.primary_chain_data and self.primary_chain_data.arkham_entity:
            return self.primary_chain_data.arkham_entity.website
        return ""

    @property
    def twitter(self) -> str:
        """
        Get the entity Twitter handle from primary chain data.

        Returns:
            str: Twitter handle or empty string if not available
        """
        if self.primary_chain_data and self.primary_chain_data.arkham_entity:
            return self.primary_chain_data.arkham_entity.twitter
        return ""

    @property
    def crunchbase(self) -> str:
        """
        Get the entity Crunchbase profile from primary chain data.

        Returns:
            str: Crunchbase profile or empty string if not available
        """
        if self.primary_chain_data and self.primary_chain_data.arkham_entity:
            return self.primary_chain_data.arkham_entity.crunchbase
        return ""

    @property
    def linkedin(self) -> str:
        """
        Get the entity LinkedIn profile from primary chain data.

        Returns:
            str: LinkedIn profile or empty string if not available
        """
        if self.primary_chain_data and self.primary_chain_data.arkham_entity:
            return self.primary_chain_data.arkham_entity.linkedin
        return ""

    @property
    def is_user_address(self) -> bool:
        """
        Check if the address is a user address from primary chain data.

        Returns:
            bool: True if address is a user address, False otherwise
        """
        if self.primary_chain_data:
            return self.primary_chain_data.is_user_address
        return False

    def to_dict(self) -> dict:
        """
        Convert WalletLabel instance to dictionary format.

        Returns:
            dict: Dictionary representation of wallet label with all relevant fields
        """
        return {
            "address": self.address,
            "name": self.name,
            "type": self.entity_type,
            "website": self.website,
            "twitter": self.twitter,
            "crunchbase": self.crunchbase,
            "linkedin": self.linkedin,
            "label": self.label,
            "isUserAddress": self.is_user_address,
        }
