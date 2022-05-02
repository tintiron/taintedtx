import logging

import pandas as pd

logging.getLogger().setLevel(logging.INFO)
pd.options.mode.chained_assignment = None  # default='warn'


def clustering(cluster, check, done, depth, df, coinjoin_tx, exclude):
    """
    Address clustering process

    :param cluster: DataFrame containing clustered adr for adding more.
    :param check: List of adr to perform clustering.
    :param done: List of adr already clustered.
    :param depth: Number of the current loop.
    :param df: Either tx_input or tx_output
    :param coinjoin_tx: coinjoin transaction dataframe, can be empty.
    :param exclude: List of addresses to exclude from clustering.

    :return: DataFrame containing addresses belonging to the same cluster.
    """

    done = list(set(done + check))  # add adr searching in this loop
    check = df[df['adr_index'].isin(check)]  # find tx
    check = check[~check['adr_index'].isin(exclude)]

    if coinjoin_tx is not None:
        check = check[~check.index.isin(coinjoin_tx.index)]

    check = df[df.index.isin(check.index)]  # find adr
    check = check[~check['adr_index'].isin(cluster.index)].drop_duplicates(subset='adr_index')[
        'adr_index'].tolist()  # remove adr already in result

    check2 = pd.DataFrame(index=check)
    check2['depth'] = depth  # add adr index to cluster
    cluster = cluster.append(check2)
    check = list(filter(lambda i: i not in done, check))  # remove adr already searched from next check
    return cluster, check, done


def adr_cluster(target, heuristic, tx_input, tx_output, depth_limit=-1, coinjoin_tx=None, exclude=()):
    """
    A simple input sharing clustering on a group of starting adr (the group input imply that all addresses in the list belong to the same entity)

    :param target: List or string of address index to perform clustering.
    :param heuristic: Either 'multi-input' or 'multi-output' to select clustering heuristic to perform.
    :param tx_input: DataFrame containing tx input data.
    :param tx_output: DataFrame containing tx output data.
    :param depth_limit: Number for how many clustering loop to be performed.
    :param coinjoin_tx: Optional coinjoin transaction dataframe for preventing the coinjoin transaction clustering.
    :param exclude: List of addresses to exclude from clustering.

    :return: DataFrame containing addresses belonging to the same cluster.

    tx_input and tx_output recommends trimming the data first and remove unnecessary data first to improve speed performance using something similar as the below script snippet
    tx_input = tx_input.drop(columns=['input_value','spent_index']).set_index('tx_index')  # remove unused column
    tx_input = tx_input.reset_index().drop_duplicates().set_index('tx_index')  # remove duplicate adr in same tx
    tx_input2 = tx_input.groupby('tx_index').count() # check for a single input tx
    tx_input2 = tx_input2[tx_input2['adr_index'] > 1]
    tx_input = tx_input[tx_input.index.isin(tx_input2.index)]  # remove single input
    """

    cluster = pd.DataFrame(columns=['depth'])
    try:
        done = []
        if heuristic == "multi-input":  # input clustering
            check = target
            if type(check) != list:
                check = [target]
            depth = 1
            while len(check) > 0 and depth != depth_limit:
                cluster, check, done = clustering(cluster, check, done, depth, tx_input, coinjoin_tx, exclude)
                depth += 1

        elif heuristic == "multi-output":  # output clustering
            check = target
            if type(check) != list:
                check = [target]
            depth = 1
            while len(check) > 0 and depth != depth_limit:
                cluster, check, done = clustering(cluster, check, done, depth, tx_output, coinjoin_tx, exclude)
                depth += 1

    except KeyboardInterrupt:  # return current results when interrupt
        return cluster
    return cluster
