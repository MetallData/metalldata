#!/usr/bin/env python3

import sys
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
from mpi4py import MPI


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

chunks = None

if rank == 0:
    infn = sys.argv[1]
    base = Path(infn).stem
    
    # Load the CSV file into a Pandas DataFrame
    df = pd.read_csv(infn)
    
    # Split the dataframe among processes
    chunks = [df[i::size] for i in range(size)]

# Broadcast base to all processes
base = comm.bcast(base, root=0)

# Scatter chunks to all processes
chunk = comm.scatter(chunks, root=0)

# Each process writes its own parquet file
outfn = f"{base}_{rank}.parquet"
chunk.to_parquet(outfn, engine='pyarrow')
print(f"Rank {rank} wrote {outfn}")



# #!/usr/bin/env python3

# import sys
# from pathlib import Path
# import pandas as pd
# import pyarrow.parquet as pq


# infn = sys.argv[1]
# base = Path(infn).stem
# outfn = base + ".parquet"

# # Load the CSV file into a Pandas DataFrame
# df = pd.read_csv(infn)

# # Write the DataFrame to a Parquet file
# df.to_parquet(outfn, engine='pyarrow')


