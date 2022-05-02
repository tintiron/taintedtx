"""This is the snippet of codes we use to export blockchain data from blocksci"""
import os.path

import blocksci
import numpy as np
import pandas as pd

chain = blocksci.Blockchain('ADDPATH')
converter = blocksci.CurrencyConverter()
path = ""

block_file_name = 'block.csv'
tx_output_file_name = 'tx_output.csv'
tx_input_file_name = 'tx_input.csv'
tx_height_file_name = 'tx_height.csv'
tx_hash_file_name = 'tx_hash.csv'
adr_hash_file_name = 'adr_hash.csv'
txfee_file_name = 'tx_fee.csv'
coinjoin_file_name = 'coinjoin_tx.csv'
tx_range_file_name = 'tx_range.csv'

year_range = ['2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022']
month_range = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']


def write_file(this_df, file_name):
    if file_name[-3:] == '.h5':
        this_df.to_hdf(os.path.join(path, file_name), key='df')
    elif file_name[-4:] == '.pkl':
        this_df.to_pickle(os.path.join(path, file_name))
    elif file_name[-4:] == '.csv':
        this_df.to_csv(os.path.join(path, file_name))


def read_option(file_name, file_path='', csv_index_option=None):
    this_df = pd.DataFrame()
    if file_name[-3:] == '.h5':
        this_df = pd.read_hdf(os.path.join(file_path, file_name))
    elif file_name[-4:] == '.pkl':
        this_df = pd.read_pickle(os.path.join(file_path, file_name))
    elif file_name[-4:] == '.csv':
        if csv_index_option is not None:
            this_df = pd.read_csv(os.path.join(file_path, file_name), index_col=csv_index_option)
        else:
            this_df = pd.read_csv(os.path.join(file_path, file_name), index_col=0)
    return this_df


"""block info"""
blocks = chain.range(year_range[0], year_range[-1])
i = 0
l = len(blocks)
miner = []
while i < l:
    new_miner = blocks[i].miner()
    miner.append(new_miner)
    i = i + 1
df3 = pd.DataFrame({'miner': miner})
i = 1
l = len(blocks)
fee_rate = []
while i < l:
    example_block_height = i
    average = np.mean(chain[example_block_height].txes.fee_per_byte())
    fee_rate.append(average)
    i += 1
df4 = pd.DataFrame({'fee_rate': fee_rate})

time = blocks.time
block_hash = blocks.hash
df = pd.DataFrame({'hash': block_hash})
df2 = pd.DataFrame({'time': time})
df = df.join(df2)
block_hash_df = df['hash'].values.tolist()
block_hash_list = []
for item in block_hash_df:
    item = item.decode()
    item = item.replace("b'", '')
    item = item.replace("'", '')
    block_hash_list.append(item)
df2 = pd.DataFrame({'block_hash': block_hash_list})
df = df.drop(columns=['hash'])
df = pd.concat([df, df2], axis=1)
df.index.names = ['block_index']
df.columns = ['time', 'block_hash']
df['fee_rate'] = df4['fee_rate']
df['miner'] = df3['miner']
write_file(df, block_file_name)

del df, df2, df3, df4, block_hash_list, block_hash_df, fee_rate

"""tx hash and height"""
tx_range_df = pd.DataFrame(columns=['first_tx', 'last_tx'])  # tx range file for indexing in data loading
tx_range_df.index = tx_range_df.index.set_names('year')
for this_year in year_range:
    # tx height
    next_year = str(int(this_year) + 1)
    blocks = chain.range(this_year, next_year)
    tx_height = blocks.txes.block_height
    tx_index = blocks.txes.index
    df = pd.DataFrame({'block_index': tx_height}, index=tx_index)
    df.index = df.index.set_names('tx_index')
    write_file(df, this_year + '/' + tx_height_file_name)

    # tx hash
    tx_hash = blocks.txes.hash
    df = pd.DataFrame({'tx_hash': tx_hash}, index=tx_index)
    tx_hash = df['tx_hash'].values.tolist()
    tx_hash_list = []
    for item in tx_hash:
        item = item.decode()
        item = item.replace("b'", '')
        item = item.replace("'", '')
        tx_hash_list.append(item)
    df = pd.DataFrame({'tx_hash': tx_hash_list}, index=tx_index)
    df.index = df.index.set_names('tx_index')
    write_file(df, this_year + '/' + tx_hash_file_name)
    tx_range_df.loc[int(this_year)] = [df.index[0], df.index[-1]]
write_file(tx_range_df, tx_range_file_name)

del df, tx_range_df, tx_hash, tx_hash_list

"""Tx fee"""
blocks = chain.range(year_range[0], year_range[-1])
tx_index = blocks.txes.index
fee_byte = blocks.txes.fee_per_byte()
fee_val = blocks.txes.fee
df = pd.DataFrame({'fee_rate': fee_byte, 'fee_value': fee_val}, index=tx_index)
df.index = df.index.set_names('tx_index')
write_file(df, txfee_file_name)

del fee_val, fee_byte

"""output/input and address info"""
for this_year in year_range:
    input_df = pd.DataFrame()
    output_df = pd.DataFrame()
    address_df = pd.DataFrame()
    for this_month in month_range:
        next_year = this_year
        next_month = str(int(this_month) + 1)
        if this_month == '12':
            next_year = str(int(this_year) + 1)
            next_month = '1'
        blocks = chain.range(this_month + '-' + '1' + '-' + this_year, next_month + '-' + '1' + '-' + next_year)
        print(this_month + '-' + '1' + '-' + this_year, next_month + '-' + '1' + '-' + next_year)
        # Output
        address_type = blocks.outputs.address_type
        df2 = pd.DataFrame({'type': address_type})
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
        output_add = blocks.outputs
        address_list = []
        address_hash_list = []
        for i in output_add:
            address_list.append(i.address.address_num)
            address_type = str(i.address.type)
            if address_type == 'Nonstandard' or address_type == 'Unknown Address Type' or address_type == 'Pay to witness unknown':
                address_hash = 'Nonstandard'
                address_hash_list.append(address_hash)
            elif address_type == 'Null data':
                address_hash = 'Null'
                address_hash_list.append(address_hash)
            elif address_type == 'Multisig':
                address_hash = i.address.addresses[0].address_string
                address_hash_list.append(address_hash)
            else:
                address_hash = i.address.address_string
                address_hash_list.append(address_hash)
        df = pd.DataFrame({'adr_index': address_list, 'adr_type': df2})
        df['adr_index'] = df['adr_index'].map(str) + df['adr_type'].map(str)
        df = df.drop(columns=['adr_type'])
        df['adr_index'] = df['adr_index'].astype(int)
        output_spent = blocks.outputs
        output_inside_index = blocks.outputs.index
        spent_list = []
        for tx in output_spent:
            spent_list.append(tx.spending_tx_index)
        output_tx_index = blocks.outputs.tx_index
        output_value = blocks.outputs.value
        df = pd.DataFrame({'output_index': output_inside_index, 'adr_index': df['adr_index'].tolist(), 'output_value': output_value,
                           'spent_index': spent_list}, index=output_tx_index)
        df['output_index'] = df.index.map(str) + df['output_index'].map(str)
        df['output_index'] = df['output_index'].astype(int)
        df.index = df.index.rename('tx_index')
        output_df = output_df.append(df)
        # Address
        df = pd.DataFrame({'adr_hash': address_hash_list, 'adr_type': df2}, index=address_list)
        df = df.drop_duplicates(subset=['adr_hash'])
        df = df.reset_index()
        df.columns = ['adr_index', 'adr_hash', 'adr_type']
        df['adr_index'] = df['adr_index'].map(str) + df['adr_type'].map(str)
        df = df.drop(columns=['adr_index', 'adr_type'])
        df = df.set_index('adr_index')
        address_df = address_df.append(df)
        # Input
        address_type = blocks.inputs.address_type
        df2 = pd.DataFrame({'type': address_type})
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
        input_add = blocks.inputs
        address_list = []
        for i in input_add:
            address = i.address.address_num
            address_list.append(address)
        df = pd.DataFrame({'adr_index': address_list, 'adr_type': df2})
        df['adr_index'] = df['adr_index'].map(str) + df['adr_type'].map(str)
        df = df.drop(columns=['adr_type'])
        df['adr_index'] = df['adr_index'].astype(int)
        index_list = []
        for this_input in input_add:
            spend = this_input.spent_output
            index = str(spend.tx_index) + str(spend.index)
            index_list.append(index)
        input_value = blocks.inputs.value
        input_tx_index = blocks.inputs.tx_index
        input_spent = blocks.inputs.spent_tx_index
        df = pd.DataFrame({'output_index': index_list, 'adr_index': df['adr_index'].tolist(), 'input_value': input_value, 'spent_index': input_spent},
                          index=input_tx_index)
        df['output_index'] = df['output_index'].astype(int)
        df.index = df.index.rename('tx_index')
        input_df = input_df.append(df)

    write_file(output_df, this_year + '/' + tx_output_file_name)
    write_file(input_df, this_year + '/' + tx_input_file_name)
    write_file(address_df, this_year + '/' + adr_hash_file_name)

del df, df2, input_df, output_df, address_df, index_list, address_list

"""address hash"""
# For after finish the above, remove duplicate and change index to int
found = []
for year in year_range:
    address = read_option(adr_hash_file_name, year)
    address.index = address.index.astype(int)
    address = address[~address.index.duplicated(keep='first')]
    address = address[~address.index.isin(found)]
    address = address.sort_index()
    found += list(set(address.index.tolist()))
    write_file(address, year + '/' + adr_hash_file_name)

del address

"""coinjoin"""
tx_index_list = []
joinmarket_list = []  # joinmarket list
coinjoin_df = pd.DataFrame(columns=['joinmarket'])
for tx in blocks.txes:
    tx_index = tx.index
    print(tx_index)
    joinmarket = blocksci.heuristics.is_coinjoin(tx)
    tx_index_list.append(tx_index)
    joinmarket_list.append(joinmarket)

coinjoin_df = pd.DataFrame({'joinmarket': joinmarket_list}, index=tx_index_list)
write_file(coinjoin_df, coinjoin_file_name)
