## TaintedTX

We include Bitcoin sample database in the sampledata folder that contain the complete first 2 years and only the first day of the year from year 2012 onwards. We also include a dummy database in testdatabase that we build for our early testing of the library. 

To test the library tool online visit: https://mybinder.org/v2/gh/tintiron/taintedtx/HEAD

## Example Usage 
# Example 1
```
import taintedtx
import pandas as pd
tt = taintedtx.TaintedTx(path='sampledata/')  # choose sampledata database
tt.prepare_data(limitoption='2010to2011')  # prepare dataframe of transactions in 2010 and 2011
```

# Example 2
```
import taintedtx
import pandas as pd
tt = taintedtx.TaintedTx(path='sampledata/')
```tt.prepare_data(tx=['d31c0cc622bcc1f1c1f9a4a68650dab1ec2bc959d65b472aca48ee5155798113', '03b7fc88d6eff344a80e022b9c4f15776bf75a5e686dc6339975d9ab8ea7099'], limitoption=np.timedelta64(10,'D'))  # search for the transaction and prepare dataframe for 10 days starting from the earliest transaction in the list, this will genereate resultdf with tx_index that we can use for taint analysis function```
tt.tx_taint_search(resultdf.index)  # perform taint analysis on the transaction
```

# Example 3
