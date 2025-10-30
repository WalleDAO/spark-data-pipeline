
import requests
import json
from typing import List, Tuple
import csv
import os
from datetime import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from config import ARKHAM_URL, ARKHAM_API_KEY, MAX_WORKERS, REQUEST_DELAY

class ArcHamResponseItem:
    """ArcHam API response item object"""
    
    def __init__(self, 
                 address: str = "",
                 name: str = "",
                 type: str = "",
                 website: str = "",
                 twitter: str = "",
                 crunchbase: str = "",
                 linkedin: str = "",
                 label: str = "",
                 isUserAddress: bool = False):
        self.address = address
        self.name = name
        self.type = type
        self.website = website
        self.twitter = twitter
        self.crunchbase = crunchbase
        self.linkedin = linkedin
        self.label = label
        self.isUserAddress = isUserAddress
    
    def __repr__(self):
        return (f"ArcHamResponseItem("
                f"address='{self.address}', "
                f"name='{self.name}', "
                f"type='{self.type}', "
                f"label='{self.label}', "
                f"isUserAddress={self.isUserAddress})")
    
    def __str__(self):
        return f"Address: {self.address} | Name: {self.name} | Type: {self.type} | Label: {self.label}"
    
    def to_dict(self) -> dict:
        return {
            "address": self.address,
            "name": self.name,
            "type": self.type,
            "website": self.website,
            "twitter": self.twitter,
            "crunchbase": self.crunchbase,
            "linkedin": self.linkedin,
            "label": self.label,
            "isUserAddress": self.isUserAddress
        }

def getArkHamLabels(addrs: List[str]) -> Tuple[List[ArcHamResponseItem], str]:
    
    ArcHamResponses: List[ArcHamResponseItem] = []
    lock = threading.Lock()
    thread_local = threading.local()
    
    def get_session():
        """Get thread-local session for thread-safe HTTP requests"""
        if not hasattr(thread_local, 'session'):
            thread_local.session = requests.Session()
            thread_local.session.headers.update({
                "accept": "application/json",
                "API-Key": ARKHAM_API_KEY
            })
        return thread_local.session

    def parse_json_to_archam_item(data: dict) -> ArcHamResponseItem:
        """Convert API JSON data to ArcHamResponseItem object"""
        item = ArcHamResponseItem()
        
        if not isinstance(data, dict) or len(data) == 0:
            return item
        
        priority_chains = ["ethereum", "arbitrum_one", "polygon", "optimism", "base", "bsc"]
        
        selected_chain_data = None
        selected_chain = None
        
        for chain in priority_chains:
            if chain in data:
                selected_chain_data = data[chain]
                selected_chain = chain
                break
        
        if selected_chain_data is None:
            for chain_name, chain_data in data.items():
                selected_chain_data = chain_data
                selected_chain = chain_name
                break
        
        if selected_chain_data is None:
            return item
        
        item.address = selected_chain_data.get('address', '')
        item.isUserAddress = selected_chain_data.get('isUserAddress', False)
        
        if 'arkhamEntity' in selected_chain_data and selected_chain_data['arkhamEntity']:
            arkham_entity = selected_chain_data['arkhamEntity']
            item.name = arkham_entity.get('name', '')
            item.type = arkham_entity.get('type', '')
            item.website = arkham_entity.get('website', '')
            item.twitter = arkham_entity.get('twitter', '')
            item.crunchbase = arkham_entity.get('crunchbase', '')
            item.linkedin = arkham_entity.get('linkedin', '')
        
        if 'arkhamLabel' in selected_chain_data and selected_chain_data['arkhamLabel']:
            item.label = selected_chain_data['arkhamLabel'].get('name', '')
        else:
            for chain_name, chain_data in data.items():
                if chain_name != selected_chain and 'arkhamLabel' in chain_data and chain_data['arkhamLabel']:
                    item.label = chain_data['arkhamLabel'].get('name', '')
                    break
        
        return item

    def get_address_intelligence(address: str, index: int, total: int) -> dict:
        """Get address intelligence from Arkham API"""
        url = f"{ARKHAM_URL}/intelligence/address/{address}/all"
        
        try:
            time.sleep(REQUEST_DELAY)
            
            session = get_session()
            response = session.get(url, timeout=15)
            
            if response.status_code == 200:
                return {
                    "success": True,
                    "address": address,
                    "data": response.json(),
                    "index": index,
                    "total": total
                }
            else:
                return {
                    "success": False,
                    "address": address,
                    "error": f"HTTP {response.status_code}",
                    "index": index,
                    "total": total
                }
        
        except requests.exceptions.RequestException as e:
            return {
                "success": False,
                "address": address,
                "error": f"Request error: {str(e)}",
                "index": index,
                "total": total
            }

    def process_single_address(address: str, index: int, total: int):
        """Process single address and add to results"""
        nonlocal ArcHamResponses
        
        result = get_address_intelligence(address, index, total)
        
        if result["success"]:
            print(f"[{result['index']}/{result['total']}] ✅ Success - {result['address']}")
            archam_item = parse_json_to_archam_item(result["data"])
            
            with lock:
                ArcHamResponses.append(archam_item)
        else:
            print(f"[{result['index']}/{result['total']}] ❌ Failed - {result['address']}: {result['error']}")
            failed_item = ArcHamResponseItem(address=result['address'])
            
            with lock:
                ArcHamResponses.append(failed_item)

    def batch_process_addresses_concurrent(addresses: List[str]):
        """Batch process address list with concurrent execution"""
        nonlocal ArcHamResponses
        
        total_count = len(addresses)
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for i, addr in enumerate(addresses, 1):
                future = executor.submit(process_single_address, addr, i, total_count)
                futures.append(future)
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Task failed with error: {e}")

    def export_to_csv(filename: str = None) -> str:
        """Export ArcHamResponses data to CSV file"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"ArcHam_Intelligence_Report_{timestamp}.csv"
        
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        current_dir = os.path.dirname(os.path.abspath(__file__))
        filepath = os.path.join(current_dir, filename)
        
        headers = [
            "no", "address", "name", "type", "label", 
            "isuseraddress", "website", "twitter", "crunchbase", "linkedin"
        ]
        
        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers)
                
                for i, item in enumerate(ArcHamResponses, 1):
                    writer.writerow([
                        i,
                        item.address,
                        item.name,
                        item.type,
                        item.label,
                        item.isUserAddress,
                        item.website,
                        item.twitter,
                        item.crunchbase,
                        item.linkedin
                    ])
            
            return filepath
        except Exception as e:
            print(f"Error saving CSV file: {str(e)}")
            return None

    if not addrs or len(addrs) == 0:
        print("No addresses provided")
        return [], ""
    
    start_time = time.time()
    print(f"Starting batch processing of {len(addrs)} addresses...")
    
    batch_process_addresses_concurrent(addrs)
    
    end_time = time.time()
    print(f"Processing completed in {end_time - start_time:.2f} seconds")
    
    csv_path = export_to_csv()
    if csv_path:
        print(f"CSV file created: {csv_path}")
        return ArcHamResponses, csv_path
    
    return ArcHamResponses, ""