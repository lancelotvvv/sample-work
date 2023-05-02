import pandas as pd
import pyarrow.parquet as pq



table = pq.read_table("processed_data.parquet")

