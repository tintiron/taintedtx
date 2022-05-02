import logging
import pandas as pd

import requests
from bs4 import BeautifulSoup
import random
import time

logging.getLogger().setLevel(logging.INFO)
pd.options.mode.chained_assignment = None  # default='warn'

lncapup_tx = 525360980  # transaction index of the first transaction on the day the network capacity was updated.


def lightning_check(tx_input, tx_output, tx_tainted=None, service_adr=None, value_limit=None):
    """
    Check if the transactions are potentially lightning_tx transactions

    :param tx_input: Dataframe containing tx input data.
    :param tx_output: Dataframe containing tx output data.
    :param tx_tainted: Dataframe for lightning_tx checking.
    :param service_adr: Dataframe containing service unrelated to lightning_tx network.
    :param value_limit: Optional channel value limit that replace the default.

    :return: Dataframe with public lightning_tx classification.
    """
    import taintedtx
    lightning_adr_type = taintedtx.lightning_adr_type

    if tx_tainted is not None:
        found_tx = tx_output[tx_output.index.isin(tx_tainted.index)]
    else:
        found_tx = tx_output

    found_tx['adr_type'] = found_tx['adr_index'] % 10
    found_tx = found_tx[found_tx['adr_type'] == lightning_adr_type]  # only witness script hash multi-sig adr output

    if value_limit is None:
        found_tx = found_tx[found_tx['output_value'] <= 16700000]
        found_tx = found_tx[(found_tx['output_value'] > 4200000) & (found_tx.index < lncapup_tx)]
    else:
        found_tx = found_tx[found_tx['output_value'] <= value_limit]

    if service_adr is not None:
        found_input = tx_input[tx_input.index.isin(found_tx.index)]
        found_input = found_input[~found_input['adr_index'].isin(service_adr.index)]
        found_tx = found_tx[~found_tx.index.isin(found_input.index)]  # no service in input
        found_tx = found_tx[~found_tx['adr_index'].isin(service_adr.index)]  # no service in output

    close_tx = tx_output[tx_output.index.isin(found_tx['spent_index'])]
    close_input = tx_input[tx_input.index.isin(found_tx.index)]
    close_input = close_input.groupby('tx_index').count()
    close_input = close_input[close_input['adr_index'] == 1]  # only one input
    close_tx = close_tx[close_tx.index.isin(close_input.index)]
    close_tx = close_tx.groupby('tx_index').count()
    close_tx = close_tx[close_tx['adr_index'] <= 2]  # no more than two output

    found_tx = found_tx[found_tx.index.isin(close_tx.index)]  # complete list of lightning_tx tx
    lightning_tx = tx_tainted[tx_tainted.index.isin(found_tx.index)]

    return lightning_tx


def pub_lightning_check(tx_tainted, user_agent_list):
    """
    Check if found lightning_tx transactions are public or not

    :param tx_tainted: Dataframe for public checking, require transaction hash value.
    :param user_agent_list: List of user agents to improve google automated search prevention.

    :return: Dataframe with public lightning_tx classification.
    """

    search_wait_time = random.randint(5, 120)
    error_wait_time = random.randint(500, 3000)

    for run, tx in enumerate(tx_tainted.index):
        try:
            this_tx_hash = tx_tainted.loc[tx, 'tx_hash']
            this_tx_hash = f'"{this_tx_hash}"'
            time.sleep(search_wait_time)  # seem like may need longer time wait
            user_agent = random.choice(user_agent_list)
            requests.headers = {'User-Agent': user_agent, 'referer': 'https://www.google.com/'}
            url = 'https://www.google.com/search?q="lightning_tx"+' + this_tx_hash + '&oq="lightning_tx"+' + this_tx_hash
            source_code = requests.get(url, headers=requests.headers)

            while source_code.status_code != 200:
                print(tx, 'Error')
                time.sleep(error_wait_time)
                url = 'https://www.google.com/search?q="lightning_tx"+' + this_tx_hash + '&oq="lightning_tx"+' + this_tx_hash
                source_code = requests.get(url, headers=requests.headers)

            if source_code.status_code == 200:
                soup = BeautifulSoup(source_code.text, "html.parser")
                found = soup.find('div', id={'result-stats'})
                if found is not None:
                    tx_tainted.loc[tx, 'public'] = True
                else:
                    tx_tainted.loc[tx, 'public'] = False
        except:
            pass
        if run % 500 == 0:
            tx_tainted.to_csv('pub_lighting.csv')
    return tx_tainted
