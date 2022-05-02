Example usage of TaintedTX library

Example 1
---------------------------------------
>>> import taintedtx
>>> tt = taintedtx.TaintedTX(path='sampledata/')  # choose sampledata database
>>> tt.prepare_data(limitoption='2010to2011')  # prepare dataframe of transactions in 2010 and 2011

Example 2
---------------------------------------
>>> import taintedtx
>>> tt = taintedtx.TaintedTX(path='sampledata/')
>>> search_tx = ['txhash1', 'txhash2']
>>> tt.prepare_data(tx=search_tx,limitoption=np.timedelta64(10,'D'))  # search for the transaction and prepare dataframe for 10 days starting from the earliest transaction in the list, this will return result data frame with tx index that we can use for taint analysis function
>>> tt.tx_taint_search(tt.result['tx_index'])  # perform taint analysis on the transaction
>>> tt.policy_tx_taint('fifo')  # Apply FIFO strategies distribution

Example 3
---------------------------------------
>>> import taintedtx
>>> tt = taintedtx.TaintedTX(path='sampledata/')
>>> search_adr = ['adrhash1']
>>> tt.prepare_data(adr=search_tx)  # search for address and prepare dataframe of the whole blockchain data, return result data frame that contains every transaction outputs received by the addresses
>>> tt.adr_taint_search(tt.resultdf.index, depth=100)  # perform address taint analysis on the transaction for 100 depth search

