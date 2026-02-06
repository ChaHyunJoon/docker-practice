# data pipeline: gets some input data, processes, and outputs the result (parquet file, postgre db, data warehouse)
import sys
import pandas as pd

print('arguments:', sys.argv)

month = int(sys.argv[1])

df = pd.DataFrame({"day": [1, 2], "num_passengers": [3, 4], "month": [month, month]})
print(df.head())

df.to_parquet(f"output_delay_{sys.argv[1]}.parquet")
print("hello pipeline, month:", month)