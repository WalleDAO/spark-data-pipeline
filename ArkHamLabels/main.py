from getDuneData import getDuneData
from getArkHamLabels import getArkHamLabels
from clearDuneTable import clearDuneTable
from insertDuneTable import insertDataToDuneTable
from dune_client.client import DuneClient
from config import DUNE_API_KEY

def main():
    dune = DuneClient(DUNE_API_KEY)
    ########################################################
    # Step 1: Fetching address data from Dune Analytics...
    user_addresses = getDuneData(dune)
    if not user_addresses:
        print("❌ Failed to retrieve any address data, program terminated")
        return
    
    print(f"✅ Step 1:  Dune data retrieval completed")
    print(f"📊 Retrieved {len(user_addresses)} addresses\n")
    ########################################################
    # Step 2: Getting address labels through Arkham Intelligence...
    arkham_results, filename = getArkHamLabels(user_addresses)
    print(f"✅ Step 2:  Arkham Intelligence processing completed")
    print(f"📁 Output file: {filename}")
    ########################################################
    # Step 3: Clear the existing contents of the data table
    isClear = clearDuneTable(dune,DUNE_API_KEY)
    if not isClear:
        print("❌ Data table clearing failed. Please check if the data table exists.")
        return
    print(f"✅ Step 3:  Data cleanup successful！")
    ######################################################## 
    # Step 4: Insert the data processed in Step 2 into the data table on the Dune platform.
    
    isInsert = insertDataToDuneTable(dune,filename)
    if not isInsert:
        print("❌ Data insertion failed, please check and try again.")
        return
    print(f"✅ Step 4:  Data update successful! Please open the online data platform to check if the data has been updated.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Program execution interrupted by user")
    except Exception as e:
        print(f"\n❌ Program execution error: {str(e)}")
        import traceback
        traceback.print_exc()
