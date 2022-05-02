import logging
import numpy as np
import pandas as pd
from utility import utility_function

logging.getLogger().setLevel(logging.INFO)
pd.options.mode.chained_assignment = None  # default='warn'

read_option = utility_function.read_option


def find_control(taintedtx, target_list, search_type, value, limit_option, input_count, output_count, value_range=0.1, case_name=None):
    """
    Find transaction control groups.

    :param taintedtx: TaintedTX object.
    :param target_list: List of targeted addresss or transactions.
    :param search_type: 'tx' for transaction search and 'adr' for address search.
    :param value: Transaction value criterion in smallest unit.
    :param limit_option: day number limit in np.datetime64 e.g., np.timedelta64(10, 'D')
    :param input_count: Transaction input number criterion.
    :param output_count: Transaction output number criterion.
    :param value_range: Range of acceptable transaction value.
    :param case_name: Name of the case used for saving into file.

    :return: DataFrame with list of potential transaction control groups.
    """

    if search_type == 'tx':
        taintedtx.prepare_data(tx=target_list, limit_option=[np.timedelta64(1, 'D'), limit_option])
    elif search_type == 'adr':
        taintedtx.prepare_data(adr=target_list, limit_option=[np.timedelta64(1, 'D'), limit_option])
    block = read_option(taintedtx.block_filename, taintedtx.path)
    logging.info('find control')
    start_tx = taintedtx.result['tx_index'].values[0]
    tx_input = taintedtx.tx_input[taintedtx.tx_input.index < start_tx]
    tx_output = taintedtx.tx_output[taintedtx.tx_output.index < start_tx]
    start_block = taintedtx.tx_height[taintedtx.tx_height.index == start_tx]['block_index'].values
    start_time = block[block.index == start_block[0]]['time'].values

    after_time = start_time - np.timedelta64(1, 'D')  # remove 1 more day
    after_tx = \
        taintedtx.tx_height[
            taintedtx.tx_height['block_index'] == block['time'].loc[block['time'].dt.date == np.datetime64(after_time[0], 'D')].index[
                -1]].index[-1]
    tx_input = tx_input[tx_input.index <= after_tx]
    tx_output = tx_output[tx_output.index <= after_tx]

    # input number criteria
    control = tx_input.groupby('tx_index').count()
    control = control[control['input_value'] == input_count]

    # output number criteria
    control2 = tx_output.groupby('tx_index').count()
    control2 = control2[control2['output_value'] == output_count]
    control = control[control.index.isin(control2.index)]
    control = tx_input[tx_input.index.isin(control.index)]

    # value range criteria
    control = control[control['input_value'] >= value * (1 - (value_range / 2))]
    control = control[control['input_value'] <= value * (1 + (value_range / 2))]

    control = control[~control.index.isin(control['spent_index'])]  # remove direct transaction in the same chain

    # sort to get closest value
    df3 = tx_input[tx_input.index.isin(control.index)]
    df3 = df3.drop(columns=['spent_index', 'adr_index'])
    df3 = df3.groupby('tx_index').sum()
    df2 = df3.iloc[(df3['input_value'] - (value * 100000000)).abs().argsort()]
    df = pd.DataFrame()
    for year in taintedtx.yearlist:
        tx_hash = read_option(taintedtx.txhash_filename, taintedtx.path + str(year) + '/')
        temp_hash = tx_hash[tx_hash.index.isin(df3.index)]
        df = df.append(temp_hash)
        if len(df) == len(df3):
            break
    df = df.reindex(df2.index)

    if case_name is not None:
        df.to_csv(case_name + 'ctl.csv')

    return df
