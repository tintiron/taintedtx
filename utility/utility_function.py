import logging
import os.path
import pandas as pd

logging.getLogger().setLevel(logging.INFO)
pd.options.mode.chained_assignment = None  # default='warn'


def read_option(file_name, path='', csv_index=None):
    """
    Read dataframe file depending on the file extension
    :param file_name: Filename with file extension e.g., df.csv.
    :param path: Path string to file's folder.
    :param csv_index: Optional assigned index column for csv read.

    :return: Dataframe read from the file.
    """

    df = pd.DataFrame()
    if file_name[-3:] == '.h5':
        df = pd.read_hdf(os.path.join(path, file_name))
    elif file_name[-4:] == '.pkl':
        df = pd.read_pickle(os.path.join(path, file_name))
    elif file_name[-4:] == '.csv':
        if csv_index is not None:
            df = pd.read_csv(os.path.join(path, file_name), index_col=csv_index)
        else:
            df = pd.read_csv(os.path.join(path, file_name), index_col=0)
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'])
    return df


def get_inout(tx_df, search_df, search_with):
    """Search input or output from database using the addresses or tx_index

    :param tx_df: Either input_tx or output_tx dataframe.
    :param search_df: Dataframe containing tx_index to search for related input or output.
    :param search_with: String of either 'tx_index' for transaction search or 'adr_index' for adr search.

    :return: Dataframe containing inputs or outputs belonging to the assigned transactions in search_df.
    """

    result_df = pd.DataFrame()
    if search_with == 'tx_index':
        search = search_df.index
        if 'tx_index' in search_df:
            search = search_df['tx_index']
        if 'tx_index' in tx_df.columns:
            result_df = tx_df[tx_df['tx_index'].isin(search)]
        else:
            result_df = tx_df[tx_df.index.isin(search)]
    elif search_with == 'adr_index':
        result_df = tx_df[tx_df['adr_index'].isin(search_df['adr_index'])]
    return result_df


def remove_service(tx_tainted, service_adr):
    """
    Remove transaction chain starting from the next transactions of outputs that received by service adr found in service_adr

    :param tx_tainted: Dataframe for the removal process.
    :param service_adr: Dataframe containing service adr.

    :return: Dataframe excluding transactions after the service outputs.
    """

    reset = False
    if 'tx_index' in tx_tainted.columns:
        reset = True
        tx_tainted = tx_tainted.reset_index().set_index('tx_index')

    original = tx_tainted[~tx_tainted.index.isin(tx_tainted['spent_index'])]
    for tx_index in original.index:
        original = original.append(
            pd.DataFrame([tx_index], index=[tx_index], columns=['spent_index']))  # add fake previous spent_index of the first tx to prevent removal
    original = original[original['taint_value'].isna()]
    tx_tainted = tx_tainted.append(original).sort_index()

    service_remove = tx_tainted[tx_tainted['adr_index'].isin(service_adr.index)]
    tx_tainted = tx_tainted[~tx_tainted.index.isin(service_remove['spent_index'])]
    while len(tx_tainted[~tx_tainted.index.isin(tx_tainted['spent_index'])]) > 0:
        tx_tainted = tx_tainted[tx_tainted.index.isin(tx_tainted['spent_index'])]  # remove tx without previous tx
    tx_tainted = tx_tainted[~tx_tainted['adr_index'].isna()]  # remove fake
    tx_tainted.index.names = ['tx_index']
    if reset:
        tx_tainted = tx_tainted.reset_index().set_index('output_index')
    return tx_tainted


def remove_txchain(tx_tainted, remove):
    """
    Remove transaction chain after the transaction provided in the remove argument

    :param tx_tainted: Dataframe for the removal process.
    :param remove: Array like variable that contain list of tx_index for spending transactions removal.

    :return: Dataframe excluding transactions after the assigned transactions in remove.
    """

    if 'tx_index' in tx_tainted.columns:
        tx_tainted = tx_tainted.reset_index().set_index('tx_index')
    next_remove = tx_tainted[tx_tainted['spent_index'].isin(remove)]
    next_remove2 = next_remove.groupby(['spent_index', 'depth']).count().reset_index().set_index('spent_index')
    next_remove2 = next_remove2[next_remove2.index.duplicated()]  # do not remove if next transaction is used in transaction with previous depth
    next_remove = next_remove[~next_remove['spent_index'].isin(next_remove2.index)]['spent_index']

    not_check = []
    not_check += next_remove2.index.tolist()

    tx_tainted = tx_tainted[~tx_tainted.index.isin(next_remove)]
    next_remove = tx_tainted[(tx_tainted.index.isin(remove) & (~tx_tainted.index.isin(not_check)))]['spent_index']

    while len(next_remove) > 0:
        remove = next_remove
        next_remove = tx_tainted[tx_tainted['spent_index'].isin(remove)]
        next_remove2 = next_remove.groupby(['spent_index', 'depth']).count().reset_index().set_index('spent_index')
        next_remove2 = next_remove2[next_remove2.index.duplicated()]  # do not remove if next transaction is used in transaction with previous depth
        next_remove = next_remove[~next_remove['spent_index'].isin(next_remove2.index)]['spent_index']

        not_check += next_remove2.index.tolist()
        tx_tainted = tx_tainted[~tx_tainted.index.isin(next_remove)]
        next_remove = tx_tainted[(tx_tainted.index.isin(remove) & (~tx_tainted.index.isin(not_check)))]['spent_index']
    return tx_tainted


def first_spending_check(tx_tainted, case_name=0, service_adr=None, mixer_adr=None, mixer_tx=None, coinjoin_tx=None, lightning_tx=None):
    """
    Find the total output value of the outputs that reach entities or pets for the first time.

    :param tx_tainted: Dataframe for the checking process.
    :param case_name: Optional case name for the index column.
    :param service_adr: Dataframe containing service adr.
    :param mixer_adr: Dataframe containing mixer adr.
    :param mixer_tx: Dataframe containing mixer transaction.
    :param coinjoin_tx: Dataframe containing coinjoin transaction.
    :param lightning_tx: Dataframe containing lightning network transaction.

    :return: Dataframe excluding transactions after the assigned transactions in remove.
    """

    new_tx_tainted = pd.DataFrame(columns=['start_taint', 'service', 'mixer_adr', 'mixer_tx', 'coinjoin_tx', 'lightning_tx'])
    if 'tx_index' in tx_tainted.columns:
        tx_tainted = tx_tainted.reset_index().set_index('tx_index')

    found_service = pd.DataFrame()
    found_mixer_adr = pd.DataFrame()
    found_mixer_tx = pd.DataFrame()
    found_coinjoin = pd.DataFrame()
    found_lightning = pd.DataFrame()

    start_taint_value = tx_tainted[~tx_tainted.index.isin(tx_tainted['spent_index'])]['taint_value'].sum()

    # service adr
    if service_adr is not None:
        found_service = tx_tainted[tx_tainted['adr_index'].isin(service_adr.index)]
    # mixer adr
    if mixer_adr is not None:
        found_mixer_adr = tx_tainted[tx_tainted['adr_index'].isin(mixer_adr.index)]
    # mixer tx
    if mixer_tx is not None:
        found_mixer_tx = tx_tainted[tx_tainted.index.isin(mixer_tx.index)]
    # coinjoin_tx tx
    if coinjoin_tx is not None:
        found_coinjoin = tx_tainted[tx_tainted.index.isin(coinjoin_tx.index)]
    # lightning_tx tx
    if lightning_tx is not None:
        found_lightning = tx_tainted[tx_tainted.index.isin(lightning_tx.index)]

    for remove_stuff in [found_service, found_mixer_adr, found_mixer_tx, found_coinjoin, found_lightning]:
        if len(remove_stuff) > 0:
            tx_tainted = remove_txchain(tx_tainted, remove_stuff['spent_index'])  # keep only first spending instance

    found_service = tx_tainted[tx_tainted['adr_index'].isin(found_service['adr_index'])]['taint_value'].sum()
    found_mixer_adr = tx_tainted[tx_tainted['adr_index'].isin(found_mixer_adr['adr_index'])]['taint_value'].sum()
    found_mixer_tx = tx_tainted[tx_tainted.index.isin(found_mixer_tx.index)]['taint_value'].sum()
    found_coinjoin = tx_tainted[tx_tainted.index.isin(found_coinjoin.index)]['taint_value'].sum()
    found_lightning = tx_tainted[tx_tainted.index.isin(found_lightning.index)]['taint_value'].sum()
    new_tx_tainted.loc[case_name] = [start_taint_value, found_service, found_mixer_adr, found_mixer_tx, found_coinjoin, found_lightning]

    return new_tx_tainted
