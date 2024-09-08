import os
import requests
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Prompt the user for an Ethereum contract address or use the one from the .env file
contract_address = input("Enter the Ethereum contract address to analyze (or press Enter to use the one from .env): ") or os.getenv('ETH_ADDRESS')

# Other environment variables
ETHERSCAN_API_KEY = os.getenv('ETHERSCAN_API_KEY')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

# Set up the PostgreSQL connection
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

cursor = conn.cursor()

# Etherscan API endpoint for transaction history
url = f'https://api.etherscan.io/api?module=account&action=txlist&address={contract_address}&startblock=0&endblock=99999999&sort=asc&apikey={ETHERSCAN_API_KEY}'

# Fetch transaction data
response = requests.get(url)
data = response.json()

# Check if the API call was successful
if data['status'] == '1':
    transactions = data['result']
else:
    print("Error fetching data:", data['message'])
    transactions = []

# Insert data into PostgreSQL
for tx in transactions:
    cursor.execute("""
        INSERT INTO ethereum_transactions (
            tx_hash, block_number, timestamp, sender, receiver, value, gas, gas_price, is_error, tx_receipt_status, input_data, contract_address, cumulative_gas_used, gas_used, confirmations
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        tx['hash'],
        tx['blockNumber'],
        datetime.utcfromtimestamp(int(tx['timeStamp'])).strftime('%Y-%m-%d %H:%M:%S'),
        tx['from'],
        tx['to'],
        tx['value'],
        tx['gas'],
        tx['gasPrice'],
        tx['isError'],
        tx['txreceipt_status'],
        tx['input'],
        contract_address,  # Use the contract address here
        tx['cumulativeGasUsed'],
        tx['gasUsed'],
        tx['confirmations']
    ))

# Commit the transaction
conn.commit()

# Close the connection
cursor.close()
conn.close()

print("Data has been inserted into the PostgreSQL database")
