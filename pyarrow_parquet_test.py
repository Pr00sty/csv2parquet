from pandas import DataFrame
from pyarrow import parquet, Table

data_file = '1000SalesRecords.csv'
separator_char = ','

with open(data_file, 'r') as file:
    headers = [i.strip().strip('"') for i in file.readline().strip().split(separator_char)]
    dict_of_lists = {i: [] for i in headers}
    file_data = file.read().splitlines()
    for line in file_data:
        split_line = line.strip().split(separator_char)
        for col_name, val in zip(headers, split_line):
            dict_of_lists[col_name].append(val.strip())

df = DataFrame(dict_of_lists)

table = Table.from_pandas(df=df, preserve_index=False)
parquet.write_table(table, 'example_noindex.parquet')
