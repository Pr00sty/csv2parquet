import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import numpy as np
import datetime
import dask.dataframe as ddf
import sys


data_file = '1000SalesRecords.csv'
separator_char = ','

with open(data_file, 'r') as file:
    headers = file.readlines(1)[0].strip().split(separator_char) # type: List
    headers = [i.strip().strip('"') for i in headers]
    # print('-------------\n')
    dict_of_lists = {i: [] for i in headers} # type: Dict[str, List]
    file_data = file.read().splitlines()
    for line in file_data:
        split_line = line.strip().split(separator_char)
        for col_name, val in zip(headers, split_line):
            dict_of_lists[col_name].append(val.strip())

df3 = pd.DataFrame(dict_of_lists)

table = pa.Table.from_pandas(df3, preserve_index=False)
pq.write_table(table, 'example_noindex.parquet')
