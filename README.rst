TaintedTX
=======================================
TaintedTX is a python library for Bitcoin-like cryptocurrencies tracking analysis. The tool currently uses Pandas data frame library as its primary method of operation.

We include Bitcoin sample database in the sampledata folder that contains the complete first 2 years and only the first day for the years 2018 and 2019. We also include a dummy database in testdatabase that we build for our early testing of the library. 

Note that it is possible to create transaction data frames directly from blocksci without having to save them into files for reading.

The technical documentation can be found in the docs/_build/html/index.html

To test the library online, visit: https://mybinder.org/v2/gh/tintiron/taintedtx/HEAD
The binder may take a while to load (roughly 5 to 10 minutes).

Feel free to report bugs/suggestions in issues or send an email to tt28@hw.ac.uk

Notable Functions
=======================================

- Transaction taint analysis with various strategies which are, Dirty-first, Poison, Haircut and In-Out order variant combination such as FIFO (First In First Out), LIFO (Last In First Out), TIHO (Taint In Highest Out), CISO (Clean In Smallest Out).

    +-------------------------+
    | In-Out strategy varients|
    +============+============+
    |      In    |     Out    |
    +------------+------------+
    |  First(FI) |  First(FO) |
    +------------+------------+
    |  Last(LI)  |  Last(LO)  |
    +------------+------------+
    |  Taint(LI) | Highest(HO)|
    +------------+------------+
    |  Clean(CI) |Smallest(SO)|
    +------------+------------+
    | Highest(HO)|            |
    +------------+------------+
    |Smallest(SO)|            |
    +------------+------------+
- Address taint analysis for tracking zero-tainted coins from centralised mixer services.
- Address clustering heuristics namely, multi-input sharing and multi-output sharing.
- Transaction behaviour analysis of the tainted results.
- Address and transaction profiling and scaping scripts from various sources such as CoinJoin, Lightning Network, Mixer.

Example Usage 
=======================================

Example 1
---------------------------------------
>>> import taintedtx
>>> tt = taintedtx.TaintedTX(path='sampledata/')  # choose sampledata database
>>> tt.prepare_data(limit_option='2009to2010')  # prepare dataframe of transactions in 2009 and 2010

Example 2
---------------------------------------
>>> import taintedtx
>>> tt = taintedtx.TaintedTX(path='sampledata/')
>>> search_tx = ['txhash1', 'txhash2']
>>> tt.prepare_data(tx=search_tx, limit_option=np.timedelta64(10, 'D'))  # search for the transaction and prepare dataframe for 10 days starting from the earliest transaction in the list, this will return result data frame with tx index that we can use for taint analysis function
>>> taint_df = tt.tx_taint_search(tt.result['tx_index'])  # perform taint analysis on the transaction
>>> tt.policy_tx_taint(taint_df, 'fifo')  # Apply FIFO strategies distribution

Example 3
---------------------------------------
>>> import taintedtx
>>> tt = taintedtx.TaintedTX(path='sampledata/')
>>> search_adr = ['adrhash1']
>>> tt.prepare_data(adr=search_tx)  # search for address and prepare dataframe of the whole blockchain data, return result data frame that contains every transaction outputs received by the addresses
>>> tt.adr_taint_search(tt.result["adr_index"], depth_limit=100)  # perform address taint analysis on the address for 100 depth search

Future improvement/idea list
=======================================
- Switch to dask dataframe for performance.
- Add option to indicate data file between monthly and yearly.
- Add automated data frame building from BlockSci similar to the current reading from files. 
- More taint analysis strategy variants: Service/unknown out (prioritise distribution to identified (service) address first or last), closest/furthest out (prioritise based on output value compared to tainted value.).
