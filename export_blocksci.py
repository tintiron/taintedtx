"""This is the snippet of codes we use to export blockchain data from blocksci"""
import blocksci
import pandas as pd
import numpy as np
chain = blocksci.Blockchain("ADDPATH")
converter = blocksci.CurrencyConverter()
path = ""

txoutput_filename = tx_output.csv
txinput_filename = tx_input.csv
txheight_filename = tx_height.csv
txhash_filename = tx_hash.csv
adrhash_filename = adr_hash.csv
txfee_filename = tx_fee.csv

def write_file(df, filename):
  if filename[-3:] == '.h5':
    df.to_hdf(os.path.join(path, filename), key="df")
  elif filename[-4:] == '.pkl':
    df.to_pickle(os.path.join(path, filename))
  elif filename[-4:] == '.csv':
    df.to_csv(os.path.join(path, filename))
  

"""block info"""
blocks = chain.range("2009", "2022")
i=0
l=len(blocks)
miner=[]
while i < l:
  newminer=blocks[i].miner()
	miner.append(newminer)
	i = i+1		
df3 = pd.DataFrame({"miner":miner})
i=1
l=len(blocks)
feeperbyte=[]
while i < l:
	example_block_height = i
	average = np.mean(chain[example_block_height].txes.fee_per_byte())
	feeperbyte.append(average)
	i = i+1	
df4 = pd.DataFrame({"feeperbyte":feeperbyte})

time=blocks.time
hash=blocks.hash
df = pd.DataFrame({"hash":hash})
dftest = pd.DataFrame({"time":time})
df = df.join(dftest)
blockhash = df['hash'].values.tolist()
blockhashlist=[]
for item in blockhash:
	item = item.decode()
	item = item.replace("b'", "")
	item = item.replace("'", "")
	blockhashlist.append(item)
df2 = pd.DataFrame({"blockhash":blockhashlist})
df = df.drop(columns=['hash'])
df = pd.concat([df, df2], axis=1)
df.index.names = ['blockindex']
df.columns = ['time','blockhash']
df['feeperbyte'] = df4['feeperbyte']
df['miner'] = df3['miner']
write_file(df, 'blocks.csv')

"""tx hash and height"""
blocks = chain.range("2009", "2022")
txhash=blocks.txes.hash
txindex=blocks.txes.index
df = pd.DataFrame({"tx_hash":txhash}, index = txindex)
txhash = df['tx_hash'].values.tolist()
txhashlist=[]
for item in txhash:
	item = item.decode()
	item = item.replace("b'", "")
	item = item.replace("'", "")
	txhashlist.append(item)
df = pd.DataFrame({"tx_hash":txhashlist}, index = txindex)
df.index = df.index.set_names("tx_index")
write_file(df, tx_hash.csv')

rangeyear = ["2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021"]
rangemonth = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"]
for thisyear in rangeyear:
	nextyear = str(int(thisyear+1))
	blocks = chain.range(thisyear, nextyear)
	txheight = blocks.txes.block_height
	txindex=blocks.txes.index
	df = pd.DataFrame({"block_index":height}, index = txindex)
	df.index = df.index.set_names("tx_index")
	write_file(df, year+"/"+'tx_height.csv')
  

"""Tx fee"""
blocks = chain.range("2009", "2022")
feebyte = blocks.txes.fee_per_byte()
feeval = blocks.txes.fee
df = pd.DataFrame({"feeperbyte":feebyte, "feevalue":feeval}, index = txindex)
df.index = df.index.set_names("tx_index")
write_file(df, path+'tx_fee.csv')

"""output/input and address info"""

rangeyear = ["2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021"]
rangemonth = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"]
for thisyear in rangeyear:
    inputdf = pd.DataFrame()
    outputdf = pd.DataFrame()
    addressdf = pd.DataFrame()
    for thismonth in rangemonth:
        nextyear = thisyear
        nextmonth = str(int(thismonth) + 1)
        if thismonth == "12":
            nextyear = str(int(thisyear) + 1)
            nextmonth = "1"
        blocks = chain.range(thismonth + "-" + "1" + "-" + thisyear, nextmonth + "-" + "1" + "-" + nextyear)
        print(thismonth + "-" + "1" + "-" + thisyear, nextmonth + "-" + "1" + "-" + nextyear)
        # Output
        addtype = blocks.outputs.address_type
        df2 = pd.DataFrame({"type": addtype})
        df2 = df2.replace(blocksci.address_type.nonstandard, 0)
        df2 = df2.replace(blocksci.address_type.pubkey, 1)
        df2 = df2.replace(blocksci.address_type.pubkeyhash, 2)
        df2 = df2.replace(blocksci.address_type.multisig_pubkey, 3)
        df2 = df2.replace(blocksci.address_type.scripthash, 4)
        df2 = df2.replace(blocksci.address_type.nulldata, 5)
        df2 = df2.replace(blocksci.address_type.multisig, 6)
        df2 = df2.replace(blocksci.address_type.witness_pubkeyhash, 7)
        df2 = df2.replace(blocksci.address_type.witness_scripthash, 8)
        df2 = df2.replace(blocksci.address_type.witness_unknown, 9)
        df2 = df2['type'].values
        outputadd = blocks.outputs
        addresslist = []
        addhashlist = []
        for i in outputadd:
            addresslist.append(i.address.address_num)
            addtype = str(i.address.type)
            if addtype == "Nonstandard" or addtype == "Unknown Address Type" or addtype == "Pay to witness unknown":
                addhash = "Nonstandard"
                addhashlist.append(addhash)
            elif addtype == "Null data":
                addhash = "Null"
                addhashlist.append(addhash)
            elif addtype == "Multisig":
                addhash = i.address.addresses[0].address_string
                addhashlist.append(addhash)
            else:
                addhash = i.address.address_string
                addhashlist.append(addhash)
        df = pd.DataFrame({"addressindex": addresslist, "addresstype": df2})
        df['address'] = df['addressindex'].map(str) + df['addresstype'].map(str)
        df = df.drop(columns=['addresstype'])
        df['address'] = df['address'].astype(int)
        outputspent = blocks.outputs
	outputinsideindex = blocks.outputs.index
        spentlist = []
        for tx in outputspent:
            spentlist.append(tx.spending_tx_index)
        outputtxindex = blocks.outputs.tx_index
        outputval = blocks.outputs.value
        df = pd.DataFrame({"outputindex": outputinsideindex, "address": df['address'].tolist(), "outputvalue": outputval, "spentindex": spentlist}, index=outputtxindex)
        df['outputindex'] = df.index.map(str) + df['outputindex'].map(str)
	df['outputindex'] = df['outputindex'].astype(int)
	df.index = df.index.rename("txindex")
        outputdf = outputdf.append(df)
        # Address
        df = pd.DataFrame({"addresshash": addhashlist, "addresstype": df2}, index=addresslist)
        df = df.drop_duplicates(subset=['addresshash'])
        df = df.reset_index()
        df.columns = ['addindexlist', 'addresshash', 'addresstype']
        df['addressindex'] = df['addindexlist'].map(str) + df['addresstype'].map(str)
        df = df.drop(columns=['addindexlist', 'addresstype'])
        df = df.set_index('addressindex')
        addressdf = addressdf.append(df)
        # Input
        addtype = blocks.inputs.address_type
        df2 = pd.DataFrame({"type": addtype})
        df2 = df2.replace(blocksci.address_type.nonstandard, 0)
        df2 = df2.replace(blocksci.address_type.pubkey, 1)
        df2 = df2.replace(blocksci.address_type.pubkeyhash, 2)
        df2 = df2.replace(blocksci.address_type.multisig_pubkey, 3)
        df2 = df2.replace(blocksci.address_type.scripthash, 4)
        df2 = df2.replace(blocksci.address_type.nulldata, 5)
        df2 = df2.replace(blocksci.address_type.multisig, 6)
        df2 = df2.replace(blocksci.address_type.witness_pubkeyhash, 7)
        df2 = df2.replace(blocksci.address_type.witness_scripthash, 8)
        df2 = df2.replace(blocksci.address_type.witness_unknown, 9)
        df2 = df2['type'].values
        inputadd = blocks.inputs
        addresslist = []
        for i in inputadd:
            address = i.address.address_num
            addresslist.append(address)
        df = pd.DataFrame({"addressindex": addresslist, "addresstype": df2})
        df['addressindex'] = df['addressindex'].map(str) + df['addresstype'].map(str)
        df = df.drop(columns=['addresstype'])
        df['addressindex'] = df['addressindex'].astype(int)
	indexlist = []
        for thisinput in inputadd:
            spend = thisinput.spent_output
            index = str(spend.tx_index) + str(spend.index)
            indexlist.append(index)
        inputval = blocks.inputs.value
        inputtxindex = blocks.inputs.tx_index
        inputspent = blocks.inputs.spent_tx_index
        df = pd.DataFrame({"outputindex": indexlist, "address": df['addressindex'].tolist(), "inputvalue": inputval, "spentindex": inputspent}, index=inputtxindex)
	df['outputindex'] = df['outputindex'].astype(int)
        df.index = df.index.rename("txindex")
        inputdf = inputdf.append(df)
    outputdf.to_pickle(thisyear + "/" + "output.pkl")
    inputdf.to_pickle(thisyear + "/" + "input.pkl")
    addressdf.to_pickle(thisyear + "/" + "address.pkl")
	

"""address hash"""
# remove duplicate and change index to int
yearlist = (2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021) #     
found = []
for year in yearlist:
	print(year)
	try:
		address = pd.read_hdf(str(year)+"/address.h5")
	except:
		address = pd.read_pickle(str(year)+"/address.pkl")
	address.index = address.index.astype(int)
	address = address[~address.index.duplicated(keep='first')]
	address = address[~address.index.isin(found)]
	address = address.sort_index()
	found += list(set(address.index.tolist()))
	address.to_pickle(str(year)+"/address.pkl")

** coinjoin
txindexlist = []
joinmarketlist = []  # joinmarket list
# anycoinjoin = []  # other potential coinjoin
coinjoindf = pd.DataFrame(columns=["joinmarket"]) # "anycoinjoin" , anycoinjoin
for tx in blocks.txes:
	txindex = tx.index
	print(txindex)
	joinmarket = blocksci.heuristics.is_coinjoin(tx)
	txindexlist.append(txindex)
	joinmarketlist.append(joinmarket)

coinjoindf = pd.DataFrame({"joinmarket":joinmarketlist}, index = txindexlist)
coinjoindf.to_pickle("coinjointx.pkl")

**haircutfee
import pandas as pd
rangeyear = ["2020","2021"] #"2009","2010","2011","2012","2013","2014","2015","2016","2017" ,"2018","2019","2020","2021"
rangemonth = ["1","2","3","4","5","6","7","8","9","10","11","12"]
block = pd.read_hdf('fulldatabases/blocks.h5')
# year = "2009"
for year in rangeyear:
    dfbtcinputfee = pd.DataFrame()
    txheight = pd.read_hdf('fulldatabases/' + year + '/txheight.h5')
    fulldfbtcinput = pd.read_hdf('fulldatabases/' + year + '/input.h5')
    fulldfbtcoutput = pd.read_hdf('fulldatabases/' + year + '/output.h5')
    for month in rangemonth:
        nextyear = year
        nextmonth = str(int(month)+1)
        if month == "12":
            nextyear = str(int(year)+1)
            nextmonth = "1"
        blockmonth = block[(block['time'] >= year+"-"+month) & (block['time'] < nextyear+"-"+nextmonth)]     
        firsttx = txheight[txheight['blockindex'] == blockmonth.index[0]].index[0]
        lasttx = txheight[txheight['blockindex'] == blockmonth.index[-1]].index[-1]
        dfbtcinput = fulldfbtcinput[fulldfbtcinput.index >= firsttx]
        dfbtcinput = dfbtcinput[dfbtcinput.index <= lasttx]
        dfbtcoutput = fulldfbtcoutput[fulldfbtcoutput.index >= firsttx]
        dfbtcoutput = dfbtcoutput[dfbtcoutput.index <= lasttx]
        dftxfee = pd.DataFrame(index=dfbtcinput.drop(columns=['address', 'spentindex']).groupby('txindex').sum().index)
        dftxfee['txfee'] = dfbtcinput.drop(columns='address').groupby('txindex').sum()['inputvalue'] - \
                    dfbtcoutput.drop(columns=['address', 'spentindex']).groupby('txindex').sum()['outputvalue']
        feelist = dfbtcinput['inputvalue'] / dfbtcinput.drop(columns=['address', 'spentindex']).groupby('txindex').sum()['inputvalue'] * \
              dftxfee['txfee']
        dfbtcinput['fee'] = feelist.values
        dfbtcinput['fee'] = dfbtcinput['fee'].fillna(value=0)
        dfbtcinput['txfee'] = dfbtcinput['inputvalue'] - dfbtcinput['fee']
        dfbtcinput = dfbtcinput.drop(columns='fee')
        dfbtcinput['txfee'] = dfbtcinput['txfee'].fillna(value=0)
        dfbtcinput['txfee'] = dfbtcinput['txfee'].values.round()
        sumfix = dfbtcinput.drop(columns=['spentindex', 'address', 'inputvalue']).groupby('txindex').sum()
        sumfix2 = dfbtcoutput.drop(columns=['spentindex', 'address']).groupby('txindex').sum()
        sumfix['feefix'] = sumfix['txfee'] - sumfix2['outputvalue']
        sumfix = sumfix.fillna(0)
        for aa in sumfix[sumfix['feefix'] != 0].index.tolist():
            try:
                dfbtcinput.at[aa, 'txfee'][0] = dfbtcinput.at[aa, 'txfee'][0] - sumfix.loc[aa, 'feefix']
            except:
                dfbtcinput.at[aa, 'txfee'] = dfbtcinput.at[aa, 'txfee'] - sumfix.loc[aa, 'feefix']
        dfbtcinput['txfee'] = dfbtcinput['txfee'].astype(int)
        dfbtcinput = dfbtcinput.drop(columns=['spentindex','inputvalue','address'])
        dfbtcinputfee = dfbtcinputfee.append(dfbtcinput)
    dfbtcinputfee.to_csv(year+'haircutfee.csv')

