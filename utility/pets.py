from bs4 import BeautifulSoup
import requests
import random
import time
import logging
import pandas as pd

logging.getLogger().setLevel(logging.INFO)
pd.options.mode.chained_assignment = None  # default='warn'

chipmixer_start_tx_index = 217892955


def coinjoin_find(wasabi_pattern, tx_input, tx_output):
    """
    Profiling potential transactions involving coinjoin.

    :param wasabi_pattern: list of 5 patterns [criteria type ('value' or 'proportion'), tx value, output, input, denomination output,change output].
    :param tx_input: tx_input data.
    :param tx_output: tx_output data.

    :return: Dataframe with transactions classified as mixer transactions.
    """
    import taintedtx

    adr_witness_pubkeyhash = taintedtx.adr_witness_pubkeyhash

    coinjoin_tx = pd.DataFrame()

    # wasabi coinjoin_tx
    tx_input['adr_type'] = tx_input['adr_index'] % 10
    tx_output['adr_type'] = tx_output['adr_index'] % 10

    tx_input = tx_input[~tx_input.index.isin(tx_input[tx_input['adr_type'] != adr_witness_pubkeyhash].index)]  # general 5, address type must be this type only
    tx_output = tx_output[~tx_output.index.isin(tx_output[tx_output['adr_type'] != adr_witness_pubkeyhash].index)]  # general 5, address type must be this type only
    tx_input = tx_input[tx_input.index.isin(tx_output.index)]
    tx_output = tx_output[tx_output.index.isin(tx_input.index)]

    # tx value
    tx_value = tx_output.groupby('tx_index').sum()
    if wasabi_pattern[1] is not None:
        tx_value = tx_value[tx_value['output_value'] >= wasabi_pattern[1]]  # Total TX value
    output_wasabi = tx_output[tx_output.index.isin(tx_value.index)]
    input_wasabi = tx_input[tx_input.index.isin(tx_value.index)]

    # tx shape
    input_count = input_wasabi.groupby('tx_index').count()
    output_count = output_wasabi.groupby('tx_index').count()
    output_count = output_count[output_count.index.isin(input_count.index)]

    input_count = input_count[input_count['adr_index'] < output_count['adr_index']]  # general 1, input number always lower than output
    output_count = output_count[output_count.index.isin(input_count.index)]

    if wasabi_pattern[0] == "value":
        if wasabi_pattern[2] is not None:
            output_count = output_count[output_count['adr_index'] >= wasabi_pattern[2]]  # number of output
        if wasabi_pattern[3] is not None:
            input_count = input_count[input_count['adr_index'] >= wasabi_pattern[3]]  # number of input

    elif wasabi_pattern[0] == "proportion":
        input_count = input_count[['adr_index']]
        input_count = input_count.rename(columns={'adr_index': 'input_adr'})
        output_count = output_count[['adr_index']]
        output_count = output_count.merge(input_count, left_index=True, right_index=True)
        output_count['input_adr'] = output_count['input_adr'] * 100 / output_count['adr_index']
        output_count = output_count[output_count['input_adr'] >= wasabi_pattern[3]]  # proportion of input
        if wasabi_pattern[2] is not None:
            output_count = output_count[output_count['adr_index'] >= wasabi_pattern[2]]  # number of output

    output_wasabi = output_wasabi[output_wasabi.index.isin(input_count.index)]
    input_wasabi = input_wasabi[input_wasabi.index.isin(input_count.index)]
    output_wasabi = output_wasabi[output_wasabi.index.isin(output_count.index)]
    input_wasabi = input_wasabi[input_wasabi.index.isin(output_count.index)]

    # check for input count vs most frequent denominator outputs
    output_wasabi2 = output_wasabi.groupby(['tx_index', 'output_value']).count().drop(columns=['adr_index'])
    input_wasabi2 = input_wasabi.groupby(['tx_index']).count()
    output_wasabi2 = output_wasabi2.sort_values('spent_index').reset_index()
    output_wasabi2 = output_wasabi2.drop_duplicates(keep='last', subset='tx_index').set_index('tx_index')
    output_wasabi2 = output_wasabi2.merge(input_wasabi2[['input_value']], left_index=True, right_index=True)
    output_wasabi2 = output_wasabi2[output_wasabi2['spent_index'] > output_wasabi2['input_value']]
    output_wasabi = output_wasabi[~output_wasabi.index.isin(output_wasabi2.index)]

    output_wasabi = output_wasabi[
        ~output_wasabi.index.isin(output_wasabi2.index)]  # general 1, input number always greater than most frequent output
    input_wasabi = input_wasabi[input_wasabi.index.isin(output_wasabi.index)]

    # change output
    change_check = output_wasabi.groupby(['tx_index', 'output_value']).count().reset_index()
    change_check = change_check[change_check['adr_index'] == 1]
    change_check2 = output_wasabi.reset_index()
    change_check2 = pd.merge(change_check2, change_check.drop(columns=['adr_index', 'spent_index']),
                             left_on=['tx_index', 'output_value'], right_on=['tx_index', 'output_value'])
    change_check2 = change_check2.groupby('tx_index').count()

    if wasabi_pattern[0] == "value":
        if wasabi_pattern[5] is not None:
            change_check2 = change_check2[change_check2['adr_index'] >= wasabi_pattern[5]]  # Number of change output

    elif wasabi_pattern[0] == "proportion":
        change_check2 = change_check2.rename(columns={'adr_index': 'change'})
        change_check2 = change_check2.merge(output_count, how="left", left_index=True, right_index=True)
        change_check2['change'] = change_check2['change'] * 100 / change_check2['adr_index']
        if wasabi_pattern[5] is not None:
            change_check2 = change_check2[change_check2['change'] >= wasabi_pattern[5]]  # Proportion of change output

    output_wasabi = output_wasabi[output_wasabi.index.isin(change_check2.index)]
    input_wasabi = input_wasabi[input_wasabi.index.isin(change_check2.index)]

    # anonymity set
    ano_check = output_wasabi.groupby(['tx_index', 'output_value']).count().reset_index()
    ano_check = ano_check[ano_check['output_value'] > 9500000]  # general 2, total anoymity value must be higher than 0.95 BTC
    ano_check = ano_check[ano_check['adr_index'] > 1]  # must have at least 2 denomination output
    ano_check2 = output_wasabi.reset_index()
    ano_check2 = pd.merge(ano_check2, ano_check.drop(columns=['adr_index', 'spent_index']), how="left",
                          left_on=['tx_index', 'output_value'], right_on=['tx_index', 'output_value'])
    ano_check2 = ano_check2[~ano_check2['adr_type_y'].isnull()]
    ano_check2 = ano_check2.reset_index()
    ano_check2 = ano_check2.rename(columns={'index': 'order_check'})

    for index in list(set(ano_check2['tx_index'])):  # general 3, anonymity set must be in continuous order
        for this_value in list(set(ano_check2[ano_check2['tx_index'] == index]['output_value'].values)):
            df = ano_check2[(ano_check2['tx_index'] == index) & (ano_check2['output_value'] == this_value)]
            df = df[['order_check']]
            seq = pd.RangeIndex(df.order_check.min(), df.order_check.max())
            if len(seq[~seq.isin(df.order_check)].values) > 0:
                remove = ano_check2[(ano_check2['tx_index'] == index) &
                                    (ano_check2['output_value'] == this_value)]
                ano_check2 = ano_check2[~ano_check2.index.isin(remove.index)]

    ano_check2 = ano_check2.set_index('tx_index')
    for index in list(set(ano_check2.index)):  # general 4, anonymity set must sort lowest to highest
        if ano_check2[ano_check2.index == index]['output_value'].is_monotonic:
            pass
        else:
            ano_check2 = ano_check2[ano_check2.index != index]
    ano_check2 = ano_check2.groupby('tx_index').count()

    if wasabi_pattern[0] == "value":
        if wasabi_pattern[4] is not None:
            ano_check2 = ano_check2[ano_check2['adr_index'] >= wasabi_pattern[4]]  # Number of denomination output

    elif wasabi_pattern[0] == "proportion":
        ano_check2 = ano_check2.rename(columns={'adr_index': 'deno'})
        ano_check2 = ano_check2.merge(output_count, how="left", left_index=True, right_index=True)
        ano_check2['deno'] = ano_check2['deno'] * 100 / ano_check2['adr_index']
        if wasabi_pattern[4] is not None:
            ano_check2 = ano_check2[ano_check2['deno'] >= wasabi_pattern[4]]  # Proportion of denomination output

    output_wasabi = output_wasabi[output_wasabi.index.isin(ano_check2.index)]
    input_wasabi = input_wasabi[input_wasabi.index.isin(ano_check2.index)]

    wasabi_tx = output_wasabi.index.tolist()
    wasabi_tx = pd.DataFrame(index=wasabi_tx)
    wasabi_tx['type'] = 'wasabi'
    coinjoin_tx = coinjoin_tx.append(wasabi_tx)

    # whirlpool coinjoin_tx
    coin_list = [1000000, 5000000, 50000000]
    whirlpool_input = tx_input.groupby('tx_index').count()
    whirlpool_input = whirlpool_input[whirlpool_input['adr_index'] == 5]  # 5 input only
    whirlpool_input = tx_input[tx_input.index.isin(whirlpool_input.index)]
    whirlpool_input = whirlpool_input[['input_value']]

    whirlpool_output = tx_output.groupby('tx_index').count()
    whirlpool_output = whirlpool_output[whirlpool_output['adr_index'] == 5]  # 5 output only
    whirlpool_output = tx_output[tx_output.index.isin(whirlpool_output.index)]
    whirlpool_output = whirlpool_output[['output_value']]

    whirlpool_input = whirlpool_input[whirlpool_input.index.isin(whirlpool_output.index)]
    whirlpool_output = whirlpool_output[whirlpool_output.index.isin(whirlpool_input.index)]

    coin_check = whirlpool_output[~whirlpool_output['output_value'].isin(coin_list)]
    whirlpool_output = whirlpool_output[~whirlpool_output.index.isin(coin_check.index)]  # remove tx with output outside of coin_list
    whirlpool_input = whirlpool_input[whirlpool_input.index.isin(whirlpool_output.index)]
    for tx in list(set(whirlpool_output.index.tolist())):
        output_check = whirlpool_output[whirlpool_output.index.isin([tx])]
        a = output_check.values
        if (a[0] == a).all(0):  # all outputs have exact same value
            input_check = whirlpool_input[whirlpool_input.index.isin([tx])]
            found_coin = a[0][0]
            if len(input_check[
                       input_check['input_value'] >= found_coin]) != 5:  # remove any tx with input that has input value lower than output value
                whirlpool_output = whirlpool_output[~whirlpool_output.index.isin([tx])]
        else:  # not whirlpool, remove
            whirlpool_output = whirlpool_output[~whirlpool_output.index.isin([tx])]

    whirlpool_tx = list(sorted(set(whirlpool_output.index.tolist())))
    whirlpool_tx = pd.DataFrame(index=whirlpool_tx)
    whirlpool_tx['type'] = 'whirlpool'
    coinjoin_tx = coinjoin_tx.append(whirlpool_tx)
    return coinjoin_tx


def coinjoin_scrape(wait_time=random.randint(25, 50)):
    """
    Scraping current unconfirmed Wasabi CoinJoin transaction hash from Wasabi CoinJoin API
    :param wait_time: random.randint() with second value such as 10,30 for between 10 to 30 seconds
    :return:
    """
    df = pd.DataFrame(columns=["tx_hash"])
    print(df)
    while True:
        r = requests.get('https://wasabiwallet.io/api/v4/btc/chaumiancoinjoin/unconfirmed-coinjoins')
        soup = BeautifulSoup(r.text, 'html.parser')
        if str(soup) != [] and str(soup) not in df['tx_hash'].values:
            df = df.append({"tx_hash": str(soup)}, ignore_index=True)
            df.to_csv("scrape_coinjointx.csv")
        time.sleep(wait_time)
    return df


def mixer_find(tx_input, tx_output) -> pd.DataFrame:
    """
    Profiling potential transactions involving with mixer services.

    :param tx_input: tx_input data.
    :param tx_output: tx_output data.

    :return: Dataframe with transactions classified as mixer transactions.
    """

    mixer_tx = pd.DataFrame()
    output_chipmixer = tx_output.groupby('tx_index').count()
    output_chipmixer = output_chipmixer[output_chipmixer['adr_index'] > 3]  # no less than 4 output, can be higher
    output_chipmixer = tx_output[tx_output.index.isin(output_chipmixer.index)].groupby(
        ['tx_index', 'output_value']).count().reset_index().set_index(
        'tx_index')
    remove = output_chipmixer[
        (output_chipmixer['output_value'] < 100000) & (output_chipmixer['adr_index'] > 1)]  # minimum chip is 0.001 BTC with 1 exception
    output_chipmixer = output_chipmixer.groupby('tx_index').count()
    output_chipmixer = output_chipmixer[output_chipmixer['output_value'] <= 2]  # no output value more than 2 value

    output_chipmixer = tx_output[tx_output.index.isin(output_chipmixer.index)]
    output_chipmixer = output_chipmixer[~output_chipmixer.index.isin(remove.index)]
    output_chipmixer = output_chipmixer.groupby(['tx_index', 'output_value']).count().reset_index().set_index('tx_index')
    output_chipmixer['check'] = output_chipmixer['output_value'] % 100000  # must round at this digit
    remove = output_chipmixer[(output_chipmixer['adr_index'] > 1) & (output_chipmixer['check'] != 0)]  # no transaction with chip not round
    output_chipmixer = tx_output[tx_output.index.isin(output_chipmixer.index)]
    output_chipmixer = output_chipmixer[~output_chipmixer.index.isin(remove.index)]

    tx_chipmixer = list(sorted(set(output_chipmixer.index.tolist())))
    tx_chipmixer = pd.DataFrame(index=[tx_chipmixer])
    tx_chipmixer = tx_chipmixer[tx_chipmixer.index >= chipmixer_start_tx_index]  # chipmixer was announce around May 2017, remove false positive tx before that
    tx_chipmixer['type'] = 'chipmixer'
    mixer_tx = mixer_tx.append(tx_chipmixer)
    return mixer_tx
