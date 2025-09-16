import pandas as pd
import random
import numpy as np
# Number of records
num_records = 500


start = pd.to_datetime("2023-01-01 00:00:00")
end   = pd.to_datetime("2023-12-31 23:59:59")

# Convert to unix timestamps
start_u = start.value // 10**9
end_u   = end.value // 10**9

# Generate synthetic fraud test dataset
data = []
for i in range(1, num_records + 1):
    Account_ID= str(f"Act{5000 +i}")
    Transaction_ID= str(f"T{200000 + i}")
    Transaction_Amount= np.random.randint(100, 100000) # INR amount
    Transaction_Frequency= np.random.randint(10, 30)
    Hist_Trans_Frequency = np.random.randint(1, 20)
    Transaction_Time= pd.to_datetime(np.random.randint(start_u, end_u), unit='s')
    Declined_Transactions= np.random.randint(0, 5)
    Txn_In_Foreign_Country= np.random.choice([0, 1],  p=[0.9, 0.1])

    data.append([
        Account_ID,
        Transaction_ID,
        Transaction_Amount,
        Transaction_Frequency,
        Hist_Trans_Frequency,
        Transaction_Time,
        Declined_Transactions,
        Txn_In_Foreign_Country
    ])

# Define columns
columns = [
    "Account_ID",
    "Transaction_ID",
    "Transaction_Amount",
    "Transaction_Frequency",
    "Hist_Trans_Frequency",
    "Transaction_Time",
    "Declined_Transactions",
    "Txn_In_Foreign_Country"
]

# Create DataFrame
df = pd.DataFrame(data, columns=columns)


df["Account_ID"]=df["Account_ID"].astype("string")
df["Transaction_ID"]=df["Transaction_ID"].astype("string")
df["Transaction_Amount"]=df["Transaction_Amount"].astype("long")


# Save to Excel
file_path = r"C:\Users\Shyam\PycharmProjects\PythonProject\RealTimeFraudDetectionUsingKafkaAndGenAI/Fraud_TestData.xlsx"
df.to_excel(file_path, index=False)

print(f"✅ Excel file with {num_records} records created: {file_path}")


print(df.info())



