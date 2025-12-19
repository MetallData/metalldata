import pandas as pd
import numpy as np

data = {
    'name': ['Alice', 'Bob', None,],
    'height': [5.5, np.nan, None],
    'zipcode': ['12345 67890', None, '34567 89012'],
    'state': ['CA', 'NY', None],
    'job': ['Engineer', None, 'Teacher'],
}

df = pd.DataFrame(data)
df.to_parquet('missing_values.parquet', engine='pyarrow')
