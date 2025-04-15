from google.cloud import datastore
import pandas as pd

# Explicitly specify credentials file (optional if using environment variables)

client = datastore.Client()
print(f"Datasets in project {client.project}:")

# List datasets in the project
datasets = list(client.list_datasets())
if datasets:
    for dataset in datasets:
        print(f"Dataset: {dataset.dataset_id}")

        # List tables in the dataset
        tables = list(client.list_tables(dataset.dataset_id))
        if tables:
            for table in tables:
                print(f"  Table: {table.table_id}")
        else:
            print("  No tables found in this dataset.")
else:
    print("No datasets found.")
'''query = "SELECT * FROM `725980947361.risk.adjusted_invoices` LIMIT 1000"
query_job = client.query(query)
df = query_job.result().to_dataframe()

print(df.head())'''