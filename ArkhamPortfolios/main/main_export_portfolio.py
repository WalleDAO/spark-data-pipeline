import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import ARKHAM_API_KEY
from arkham_service.export_arkham_portfolio import exportPortfoliosToCsv


def main():
    address_params = [
        "0xDdE0d6e90bfB74f1dC8ea070cFd0c0180C03Ad16",
        "0x2CfB40d10ee666413cbf7e2D393b9D0C63091B30",
        "0x6fCe63859a859a0f30eD09B12F5010d790618ca4",
        "0xe4359594E7F6b0ababD9928e262282cb9f8a7bd7",
        "0x673a4e48ccdCc23BD6f3dfe426a3157CfCaC447a",
        "0xA2b16c27c0766A1Df18892F7b0413b4f5806ee4D",
        "0x2c7708a5a4AB1DA17489eF9B9C78B97bd175a6C7",
        "0xC6BaDCe2f5E10db90D74DBe023768259Ec4699c7",
    ]

    filepath = exportPortfoliosToCsv(address_params, ARKHAM_API_KEY, time_param=None)

    if filepath:
        print(f"Generated file: {filepath}")
    else:
        print("Process failed!")


if __name__ == "__main__":
    main()
