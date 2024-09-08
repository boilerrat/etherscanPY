import os
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Database connection details
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

# Query to fetch transaction timestamps and values
query = """
SELECT
    timestamp::date AS date,
    SUM(value::numeric / 1e18) AS total_ether
FROM
    ethereum_transactions
GROUP BY
    timestamp::date
ORDER BY
    date;
"""

# Execute the query and load the results into a pandas DataFrame
df = pd.read_sql_query(query, conn)

# Close the database connection
conn.close()

# Plot the data
plt.figure(figsize=(10, 6))
plt.plot(df['date'], df['total_ether'], marker='o', linestyle='-', color='b')
plt.title('Total Ether Value of Transactions Over Time')
plt.xlabel('Date')
plt.ylabel('Total Ether (ETH)')
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()

# Save the plot as an image file
plt.savefig('ether_transactions_over_time.png')

# Display the plot
plt.show()
