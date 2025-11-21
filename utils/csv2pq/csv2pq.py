#!/usr/bin/env python3

import sys
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq


infn = sys.argv[1]
base = Path(infn).stem
outfn = base + ".parquet"

# Load the CSV file into a Pandas DataFrame
df = pd.read_csv(infn)

# Write the DataFrame to a Parquet file
df.to_parquet(outfn, engine='pyarrow')

