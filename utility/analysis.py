import logging
import numpy as np
import pandas as pd

from utility import utility_function

logging.getLogger().setLevel(logging.INFO)
pd.options.mode.chained_assignment = None  # default='warn'

read_option = utility_function.read_option


def tx_analysis(taintedtx, case_name, poison_df=None, tx_tainted=None, service_adr=None, coinjoin_tx=None, mixer_adr=None,
                mixer_tx=None, known_user=None, tx_fee=None, no_reuse=None, first_tx=None, avg_tx_fee=None, lightning_tx=None):
    """
    Perform transaction analysis on the provided transactions.

    :param taintedtx: taintedtx for linking to TaintedTX object.
    :param case_name: Case name.
    :param poison_df: DataFrame containing all possible tainted transactions for potential PETs classification.
    :param tx_tainted: DataFrame containing clustered adr for adding more.
    :param service_adr: DataFrame containing service addresses.
    :param coinjoin_tx: DataFrame containing coinjoin transactions.
    :param mixer_adr: DataFrame containing mixer addresses.
    :param mixer_tx: DataFrame containing mixer transactions.
    :param known_user: DataFrame containing known user address.
    :param tx_fee: DataFrame containing transaction fee.
    :param no_reuse: DataFrame containing address never reused (receive output more than one transaction).
    :param first_tx: DataFrame containing addresses' first transaction for checking fresh address.
    :param avg_tx_fee: DataFrame containing average transaction fee either per day, block or any time interval.
    :param lightning_tx: DataFrame containing lightning network transactions.

    :return: DataFrame with transaction and criteria as columns and add average criteria value to taintedtx.evaluate
    """
    if 'tx_index' in tx_tainted.columns:
        tx_tainted = tx_tainted.set_index('tx_index')

    tx_input = taintedtx.tx_input
    if len(tx_input) == 0:
        tx_input = pd.DataFrame()
        for year in taintedtx.yearlist:
            temp_tx_input = read_option(taintedtx.input_filename, taintedtx.path + str(year) + '/')
            temp_tx_input = temp_tx_input[temp_tx_input['tx_index'].isin(tx_tainted.index)]
            tx_input = tx_input.append(temp_tx_input)
        del temp_tx_input

    tx_height = taintedtx.tx_height
    if len(taintedtx.tx_height) == 0:
        tx_height = pd.DataFrame()

        for index, txrange in enumerate(taintedtx.tx_range_list):
            if txrange[1] >= tx_tainted.index[0]:
                temp_tx_height = read_option(taintedtx.txheight_filename, taintedtx.path + str(taintedtx.yearlist[index]) + '/')
                tx_height = tx_height.append(temp_tx_height)
            if tx_tainted.index[-1] <= txrange[1]:
                break
        del temp_tx_height

    block = read_option(taintedtx.block_filename, taintedtx.path)

    new_tx_tainted = pd.DataFrame(
        columns=['total_address', 'reuse_adr', 'fresh_reuse_adr', 'fresh_adr',
                 'service_adr', 'total_tx', 'pets', 'known_user',
                 'tx_fee', 'fee_rate'],
        index=list(sorted(set(tx_tainted.index.tolist()))))

    input_count = tx_input.drop(columns=['spent_index']).groupby(['tx_index', 'adr_index']).sum()
    input_count = input_count.groupby(['tx_index']).count()
    output_count = tx_tainted.drop(columns=['spent_index']).groupby(['tx_index', 'adr_index']).sum()
    output_count = output_count.groupby(['tx_index']).count()
    output_count['adr_per_tx'] = output_count['output_value'] + input_count['input_value']
    output_count.adr_per_tx.fillna(output_count.output_value, inplace=True)  # fill nan from coinbase tx
    if service_adr is None:  # no service adr profile dataframe
        service_adr = pd.DataFrame()

    # Frequency
    frequency = []
    last_tx_list = []
    first_tx_list = []
    start_tx = tx_tainted.index[0]
    start_block = tx_height[tx_height.index == start_tx]['block_index'].values[0]
    start_time = block[block.index == start_block]['time'].values
    for i in range(1, 15 + 1):
        end_time = start_time + np.timedelta64(i, 'D')
        start_txday = \
            tx_height[tx_height['block_index'] == block['time'].loc[block['time'].dt.date == np.datetime64(end_time[0], 'D')].index[0]].index[0]
        end_txday = \
            tx_height[tx_height['block_index'] == block['time'].loc[block['time'].dt.date == np.datetime64(end_time[0], 'D')].index[-1]].index[-1]
        last_tx_list.append(end_txday)
        if i != 1:
            first_tx_list.append(start_txday)
        else:
            first_tx_list.append(start_tx)
    for this_index, last_tx in enumerate(last_tx_list):
        first_tx = first_tx_list[this_index]
        df = tx_tainted[tx_tainted.index >= first_tx]
        df = df[df.index <= last_tx]
        frequency.append(len(list(set(df.index))))
    if len(frequency) > 0:
        frequency = sum(frequency) / len(frequency)
    else:
        frequency = 0

    # Service adr
    service_adr = tx_tainted[tx_tainted['adr_index'].isin(service_adr.index)]
    service_tx = len(list(set(service_adr.index)))
    service_adr = len(list(set(service_adr['adr_index'])))

    # Reuse adr (not count service)
    reuse_df = np.nan
    if no_reuse is not None:
        no_reuse = no_reuse[no_reuse.index.isin(tx_tainted['adr_index'])]
        reuse_df = tx_tainted[~tx_tainted['adr_index'].isin(no_reuse.index)].drop(
            columns=['spent_index', 'taint_value', 'clean_value'])
        if service_adr is not None:
            reuse_df = reuse_df[~reuse_df['adr_index'].isin(service_adr.index)]
        if mixer_adr is not None:
            reuse_df = reuse_df[~reuse_df['adr_index'].isin(mixer_adr.index)]
        reuse_df = reuse_df.groupby(['tx_index', 'adr_index']).count()
        reuse_df = reuse_df.reset_index().set_index('tx_index')

    # Fresh adr (not count service)
    fresh_df = np.nan
    if first_tx is not None:
        first_tx = first_tx[first_tx.index.isin(tx_tainted['adr_index'])]
        fresh_df = tx_tainted.reset_index().set_index('adr_index')
        if service_adr is not None:
            fresh_df = fresh_df[~fresh_df.index.isin(service_adr.index)]
        if mixer_adr is not None:
            fresh_df = fresh_df[~fresh_df.index.isin(mixer_adr.index)]
        fresh_df = fresh_df[~fresh_df.index.duplicated(keep='first')].drop(columns=['spent_index', 'taint_value', 'clean_value', 'output_value'])
        fresh_df = fresh_df.rename(columns={'tx_index': 'taint_tx'})
        fresh_df = fresh_df.join(first_tx)
        fresh_df = fresh_df[fresh_df['taint_tx'] == fresh_df['tx_index']]

    # fresh and reuse
    fresh_reuse = np.nan
    if no_reuse is not None and first_tx is not None:
        fresh_reuse = fresh_df[fresh_df.index.isin(reuse_df['adr_index'])]

        # remove fresh and reuse from reuse/fresh df
        reuse_df = reuse_df[~reuse_df['adr_index'].isin(fresh_reuse.index)]
        fresh_df = fresh_df[~fresh_df.index.isin(fresh_reuse.index)]
        fresh_reuse = len(list(set(fresh_reuse.index.tolist())))

    if no_reuse is not None:
        temp_df = new_tx_tainted[new_tx_tainted.index.isin(reuse_df.index)]
        temp_df2 = reuse_df[reuse_df.index.isin(new_tx_tainted.index)].groupby('tx_index').count()['adr_index']
        temp_df['reuse_adr'] = temp_df2
        new_tx_tainted = new_tx_tainted[~new_tx_tainted.index.isin(temp_df.index)]
        new_tx_tainted = new_tx_tainted.append(temp_df)
        new_tx_tainted = new_tx_tainted.sort_index()
        reuse_df = len(list(set(reuse_df['adr_index'].tolist())))

    if first_tx is not None:
        temp_df = new_tx_tainted[new_tx_tainted.index.isin(fresh_df['tx_index'])]
        temp_df2 = fresh_df[fresh_df['tx_index'].isin(new_tx_tainted.index)].reset_index().groupby('tx_index').count()
        temp_df['fresh_adr'] = temp_df2['adr_index']
        new_tx_tainted = new_tx_tainted[~new_tx_tainted.index.isin(temp_df.index)]
        new_tx_tainted = new_tx_tainted.append(temp_df)
        new_tx_tainted = new_tx_tainted.sort_index()
        fresh_df = len(list(set(fresh_df.index)))

    total_address = len(list(set(tx_tainted['adr_index'].tolist())))  # total adr

    # Unidentified pets
    df = input_count[input_count['input_value'] > 1]
    pet_tx = tx_tainted[tx_tainted.index.isin(df.index)]
    pet_tx = tx_input[tx_input.index.isin(pet_tx.index)]
    if poison_df is not None:
        df = pet_tx[~pet_tx['spent_index'].isin(poison_df.index)]  # find tx that not already existed in FULL tainted list
    else:
        df = pet_tx[~pet_tx['spent_index'].isin(tx_tainted.index)]
    pet_tx = pet_tx[pet_tx.index.isin(df.index)]  # get tx with completely clean coins in tx_tainted
    df = pet_tx[pet_tx['adr_index'].isin(service_adr.index)]  # remove tx with service
    pet_tx = pet_tx[~pet_tx.index.isin(df.index)]

    # known user adr
    known_user_found = np.nan
    if known_user is not None:
        known_user_found = tx_tainted[tx_tainted['adr_index'].isin(known_user.index)]
        known_user_found = new_tx_tainted[new_tx_tainted.index.isin(known_user_found.index)]
        if len(known_user_found) > 0:
            known_user_found['known_user'] = True
            new_tx_tainted = new_tx_tainted[~new_tx_tainted.index.isin(known_user.index)]
            new_tx_tainted['known_user'] = False
            new_tx_tainted = new_tx_tainted.append(known_user_found)
            new_tx_tainted = new_tx_tainted.sort_index()
        #             pet_tx = pet_tx[~pet_tx.index.isin(known_user_found.index)]
        known_user_found = len(list(set(known_user_found.index.tolist())))

    # CoinJoin tx
    coinjoin_found = np.nan
    if coinjoin_tx is not None:
        coinjoin_found = coinjoin_tx[coinjoin_tx.index.isin(new_tx_tainted.index)]
        if len(coinjoin_found) > 0:
            new_tx_tainted2 = new_tx_tainted[new_tx_tainted.index.isin(coinjoin_found.index)]
            new_tx_tainted2['pets'] = coinjoin_found['entity']
            new_tx_tainted = new_tx_tainted[~new_tx_tainted.index.isin(coinjoin_found.index)]
            new_tx_tainted = new_tx_tainted.append(new_tx_tainted2)
            new_tx_tainted = new_tx_tainted.sort_index()
        pet_tx = pet_tx[~pet_tx.index.isin(coinjoin_found.index)]  # remove known coinjoin_tx from unidentified
        coinjoin_found = len(list(set(coinjoin_found.index.tolist())))

    # Mixer tx
    mixer_adr_found = np.nan
    mixer_tx_found = np.nan
    if mixer_adr is not None:
        mixer_adr_found = tx_tainted[tx_tainted['adr_index'].isin(mixer_adr.index)].drop(
            columns=['output_value', 'spent_index'])  # find tx with mixer adr
        if len(mixer_adr_found) > 0:
            mixer_adr_found = tx_tainted[tx_tainted['adr_index'].isin(mixer_adr.index)].drop(
                columns=['output_value', 'spent_index'])  # find tx with mixer adr
            mixer_adr_found = mixer_adr_found.reset_index().set_index('adr_index')
            mixer_adr_found = mixer_adr_found.merge(mixer_adr, how='left', left_index=True, right_index=True)
            mixer_adr_found = mixer_adr_found.drop_duplicates(subset='tx_index').set_index('tx_index')
            mixer_adr_found = mixer_adr_found.rename(columns={'entity': 'pets'})
        if len(mixer_adr_found) > 0:
            new_tx_tainted = new_tx_tainted[~new_tx_tainted.index.isin(mixer_adr_found.index)]
            new_tx_tainted = new_tx_tainted.append(mixer_adr_found)
            new_tx_tainted = new_tx_tainted.sort_index()
        pet_tx = pet_tx[~pet_tx.index.isin(mixer_adr_found.index)]  # remove known mixer from unidentified
        mixer_adr_found = len(list(set(mixer_adr_found.index.tolist())))

        new_tx_tainted2 = mixer_tx[mixer_tx.index.isin(new_tx_tainted.index)]
        new_tx_tainted2.rename(columns={'entity': 'pets'})
        if len(new_tx_tainted2) > 0:
            new_tx_tainted = new_tx_tainted[~new_tx_tainted.index.isin(new_tx_tainted2.index)]
            new_tx_tainted = new_tx_tainted.append(new_tx_tainted2)
            new_tx_tainted = new_tx_tainted.sort_index()
        mixer_tx_found = len(list(set(new_tx_tainted2.index.tolist())))

    # lightning_tx network tx
    lightning_count = np.nan
    if lightning_tx is not None:
        df1 = lightning_tx[lightning_tx.index.isin(new_tx_tainted.index)]
        if len(df1) > 0:
            new_tx_tainted2 = new_tx_tainted[~new_tx_tainted.index.isin(df1.index)]
            new_tx_tainted2['pets'] = 'lightning_tx'  # df1['type']
            new_tx_tainted = new_tx_tainted.append(new_tx_tainted2)
            pet_tx = pet_tx[~pet_tx.index.isin(df1.index)]  # remove identified lightning_tx from unidentified
        lightning_count = len(list(set(df1.index)))

    pets_found = list(set(pet_tx.index.tolist()))
    temp_df = new_tx_tainted[new_tx_tainted.index.isin(pets_found)]
    temp_df['pets'] = True
    new_tx_tainted = new_tx_tainted[~new_tx_tainted.index.isin(pets_found)]
    new_tx_tainted['pets'] = False
    new_tx_tainted = new_tx_tainted.append(temp_df)
    new_tx_tainted = new_tx_tainted.sort_index()
    pets_found = len(pets_found)

    total_tx = len(list(set(tx_tainted.index.tolist())))

    # adr number per tx
    new_tx_tainted['adr_per_tx'] = output_count['adr_per_tx']
    output_count = output_count[output_count.index.isin(tx_tainted.index)]
    tx_size = output_count['adr_per_tx'].tolist()
    if len(tx_size) > 0:  # average adr per tx
        tx_size_list = sum(tx_size) / len(tx_size)
    else:
        tx_size_list = np.nan

    # fee rate
    fee_byte_list = np.nan
    if tx_fee is not None:
        tx_fee = tx_fee[tx_fee.index.isin(tx_tainted.index)]
        new_tx_tainted['fee_value'] = tx_fee['fee_value']
        new_tx_tainted['fee_rate'] = tx_fee['fee_rate']

        df2 = tx_height[tx_height.index.isin(new_tx_tainted.index)]
        df2 = df2.reset_index().set_index('block_index')
        df2 = pd.merge(df2, avg_tx_fee[avg_tx_fee.index.isin(df2.index)], how='left', left_index=True, right_index=True)
        df2 = df2.set_index('tx_index')
        print(len(new_tx_tainted), len(df2))
        new_tx_tainted['fee_dif'] = new_tx_tainted['fee_rate'] - df2['fee_rate']

        fee_byte_list = new_tx_tainted['fee_dif'].tolist()
        if len(fee_byte_list) > 0:
            fee_byte_list = sum(fee_byte_list) / len(fee_byte_list)

    a_series = pd.Series(
        [case_name, frequency, total_address, reuse_df, fresh_reuse, fresh_df, service_adr, service_tx, total_tx, pets_found, coinjoin_found, mixer_adr,
         mixer_adr_found, mixer_tx_found, known_user_found, tx_size_list, fee_byte_list, lightning_count], index=taintedtx.evaluate.columns)
    taintedtx.evaluate = taintedtx.evaluate.append(a_series, ignore_index=True)
    return new_tx_tainted
