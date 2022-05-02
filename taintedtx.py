import logging
import os.path
import re
from pathlib import Path

import numpy as np
import pandas as pd
from utility import utility_function

logging.getLogger().setLevel(logging.INFO)
pd.options.mode.chained_assignment = None  # default='warn'

read_option = utility_function.read_option
get_inout = utility_function.get_inout
remove_txchain = utility_function.remove_txchain
remove_service = utility_function.remove_service
first_spending_check = utility_function.first_spending_check

# Address type index (based on blocksci 0.7 address type)
adr_nonstandard = 0
adr_pubkey = 1
adr_pubkeyhash = 2
adr_multisig_pubkey = 3
adr_scripthash = 4
adr_multisig = 5
adr_nulldata = 6
adr_witness_pubkeyhash = 7
adr_witness_scripthash = 8
adr_witness_unknown = 9

lightning_adr_type = adr_witness_scripthash

# blockchain data file name
input_filename = 'tx_input.csv'
output_filename = 'tx_output.csv'
block_filename = 'block.csv'
tx_height_filename = 'tx_height.csv'
tx_hash_filename = 'tx_hash.csv'
address_filename = 'adr_hash.csv'
tx_range_filename = 'tx_range.csv'


class TaintedTX(object):
    def __init__(self, path=''):
        """
        Starting main class for cryptocurrency tracking.

        :param path: string of folder path for specifying directory of blockchain databases folder e.g., 'fulldatabases/'
        """

        self.path = path
        if len(self.path) > 0:
            if self.path[-1] != '/':
                self.path += '/'

            year_folder = Path(self.path)
            subdirectories = [x for x in year_folder.iterdir() if x.is_dir()]

            self.year_list = [int(str(item).split('/')[-1]) for item in subdirectories if
                              str(item).split('/')[-1].isdigit()]  # get list of year folder
            self.tx_range_list = []  # list of tx index in each year for faster data reading
            tx_range = read_option(tx_range_filename, self.path)
            first_tx = tx_range['first_tx'].tolist()
            last_tx = tx_range['last_tx'].tolist()
            for item in first_tx:
                self.tx_range_list.append([int(item)])
            for index, item in enumerate(last_tx):
                self.tx_range_list[index].append(int(item))

        self.tx_input = pd.DataFrame()
        self.tx_output = pd.DataFrame()
        self.tx_height = pd.DataFrame()
        self.result = pd.DataFrame(index=[0])
        self.case_name = None
        self.evaluate = pd.DataFrame(
            columns=['case_name', 'frequency', 'total_address', 'reuse_adr', 'fresh_reuse_adr', 'fresh_adr', 'service_adr', 'service_tx',
                     'total_tx', 'pets', 'coinjoin_tx', 'mixer_adr', 'mixer_adr_tx', 'mixer_tx',
                     'known_user', 'adr_per_tx', 'fee_dif', 'lightning_tx'])

    def prepare_data(self, adr='', tx='', limit_option=None, save_tx_height=True, case_name=None):
        """
        Search for transactions or addresses, and prepare blockchain dataframe. Then fill data into class variables: tx_output, tx_input and tx_height

        :param adr: String or List of either internal adr index or adr hash.  Mixed types search does not work.
        :param tx: String or List of either internal transaction index or transaction hash.  Mixed types search does not work.
        :param limit_option: None for full blockchain search, year number ('2017', '2015to2016', '2014-01-01to2014-01-31'), numpy timedelta64 type such as np.timedelta64(30,'D') will start from the first transaction found from the parameter input while list [np.timedelta64(10,'D'), np.timedelta64(30,'D')] will start from 10 days after the first transaction and 30 days before the first transaction.
        :param save_tx_height: Save tx_height as class variable or not as it is not used in the taint analysis process.
        :param case_name: String of case name for file saving.

        The process currently accept only either search by adr or transaction, not both.
        """

        self.case_name = case_name
        self.result = pd.DataFrame()
        result_df = pd.DataFrame()
        if adr != '':
            logging.info('Searching for address data')
            self.adr = adr
            if type(adr) == str or type(adr) == int:
                self.adr = [adr]
                result_df = result_df.reindex(self.adr)
            if type(self.adr[0]) == str and any([re.search('[a-zA-Z]', stuff) is not None for stuff in self.adr]):  # adr hash search

                result_df = self.adr_check('adr_hash', self.adr)

                if len(result_df) != len(self.adr):
                    logging.warning('adr found not equal to adr input')

            for this_year in self.year_list:
                this_output = read_option(output_filename, self.path + str(this_year) + '/')
                self.result = self.result.append(this_output[this_output['adr_index'].isin(result_df.index)])

        elif tx != '':
            logging.info('Searching for transaction data')
            self.tx = tx
            if type(tx) != list:
                self.tx = [tx]
            if type(tx[0]) == str and any([re.search('[a-zA-Z]', stuff) is not None for stuff in self.tx]):  # tx hash search
                for year in self.year_list:
                    tx_hash = read_option(tx_hash_filename, self.path + str(year) + '/')
                    result_df = tx_hash[tx_hash['tx_hash'].isin(self.tx)]
                    self.result = self.result.append(result_df)
                    if len(self.result) == len(self.tx):
                        break
            else:  # tx index search
                result_df = result_df.reindex(self.tx)
                self.result = self.result.append(result_df)

        logging.info('Preparing database')

        self.limit_search_range(limit_option)
        self.result = self.tx_output[self.tx_output['tx_index'].isin(self.result.index)]
        logging.info('Finish preparing')
        if save_tx_height is False:
            self.tx_height = pd.DataFrame()
        return self.result

        # except:logging.warning('Invalid Parameter, make sure the parameter is (address hash, year). If there are multiple adr put them in list type ([]), year should be either 'all', 'limit', or number of year (2017), or period of year (2015-2016)')

    def adr_check(self, input_type, adr):
        """
        Retrieving address data from adr_hash file

        :param input_type: Either 'adr_index' for searching with address internal index or 'adr_hash' for public key hash
        :param adr: List of addresses to search.

        :return: DataFrame with address hash and index or adr.
        """

        result_df = pd.DataFrame()
        check = pd.DataFrame()
        for year in self.year_list:
            address_df = read_option(address_filename, self.path + str(year) + '/')
            if input_type == 'adr_index':
                check = address_df[address_df.index.isin(adr)]
            elif input_type == 'adr_hash':
                check = address_df[address_df['adr_hash'].isin(adr)]
            result_df = result_df.append(check)
            result_df = result_df[~result_df.index.duplicated(keep='first')]
            if len(result_df) >= len(adr):
                break
        return result_df

    def tx_check(self, input_type, tx):
        """
        Retrieving transaction data from tx_hash file

        :param input_type: Either 'tx_index' for searching with address internal index or 'tx_hash' for public key hash
        :param tx: List of transactions to search.

        :return: DataFrame with transaction hash and index or adr.
        """

        result_df = pd.DataFrame()
        check = pd.DataFrame()
        for year in self.year_list:
            df = read_option(tx_hash_filename, self.path + str(year) + '/')
            if input_type == 'tx_index':
                check = df[df.index.isin(tx)]
            elif input_type == 'tx_hash':
                check = df[df['tx_hash'].isin(tx)]
            result_df = result_df.append(check)
            result_df = result_df[~result_df.index.duplicated(keep='first')]
            if len(result_df) >= len(tx):
                break
        return result_df

    def limit_search_range(self, option=None):
        """
        Limit the blockchain data frame to transactions within the assigned time range.

        :param option: Same as limit_option in prepare_data
        """

        self.option = option
        block = read_option(block_filename, self.path)
        # limit tx_input and tx_output according to year parameter
        if option is not None:
            if type(option) == str:
                if ':' not in option and '-' not in option:
                    if 'to' in option:  # convert input year range to list
                        time = [int(i) for i in option.split('to')]
                        time_range1, time_range2 = int(time[0]), int(time[1]) + 1
                        year_list = list(range(time_range1, time_range2))
                    else:  # single year search
                        year_list = [int(option)]

                else:
                    if 'to' in option:  # convert input year range to list
                        time = [i for i in option.split('to')]
                        time = [this_time.split('-')[0] for this_time in time]
                        time_range1, time_range2 = int(time[0]), int(time[1]) + 1
                        year_list = list(range(time_range1, time_range2))
                    else:  # single year search
                        year_list = [int(option)]
                for this_time in year_list:
                    tx_input = read_option(input_filename, self.path + str(this_time) + '/')
                    self.tx_input = self.tx_input.append(tx_input)
                    tx_output = read_option(output_filename, self.path + str(this_time) + '/')
                    self.tx_output = self.tx_output.append(tx_output)
                    tx_height = read_option(tx_height_filename, self.path + str(this_time) + '/')
                    self.tx_height = self.tx_height.append(tx_height)
                if ':' in option or '-' in option:  # date time search
                    if 'to' in option:  # convert input year range to list
                        time_search = option.split('to')
                        time_search = block[(block['time'] >= time_search[0]) & (block['time'] <= time_search[1])]
                    else:
                        if ':' not in option:  # no hour
                            time_search = block[(block['time'] >= option) &
                                                (block['time'] <= pd.Timestamp(option) + pd.DateOffset(1))]
                        else:
                            time_search = block[(block['time'] >= option) &
                                                (block['time'] <= pd.Timestamp(option) + pd.Timedelta(minutes=1))]
                    self.tx_height = self.tx_height[self.tx_height['block_index'].isin(time_search.index)]
                    self.tx_input = self.tx_input[self.tx_input['tx_index'].isin(self.tx_height.index)]
                    self.tx_output = self.tx_output[self.tx_output['tx_index'].isin(self.tx_height.index)]

            elif type(option) != str:
                option1 = option
                option2 = None
                if type(option) == list:
                    option1 = option[0]
                    option2 = option[1]
                start_tx = self.result.index[0]
                for index, tx_range in enumerate(self.tx_range_list):
                    if tx_range[0] <= start_tx <= tx_range[1]:
                        tx_height = read_option(tx_height_filename, self.path + str(self.year_list[index]) + '/')
                        start_block = tx_height[tx_height.index == start_tx]['block_index'].values
                        start_time = block[block.index == start_block[0]]['time'].values
                        break

                before_tx = None
                end_time = start_time + option1
                if option2 is not None:
                    before_time = start_time - option2
                for yearindex, year in enumerate(self.year_list):
                    if (option2 is None or pd.to_datetime(before_time).year[0] <= year) and year <= pd.to_datetime(end_time).year[0]:
                        tx_height = read_option(tx_height_filename, self.path + str(year) + '/')
                        if option2 is not None and before_tx is None:
                            before_tx = tx_height[
                                tx_height['block_index'] == block[block['time'].dt.date == np.datetime64(before_time[0], 'D')].index[0]]
                            if len(before_tx) > 0:
                                before_tx = before_tx.index[-1]

                        if end_time[0] > block['time'].values[-1]:  # end time exceed current data
                            end_tx = self.tx_range_list[-1][-1]
                        else:
                            end_tx = tx_height[tx_height['block_index'] == block[block['time'].dt.date == np.datetime64(end_time[0], 'D')].index[-1]]
                            if len(end_tx) > 0:
                                end_tx = end_tx.index[-1]
                        self.tx_height = self.tx_height.append(tx_height)

                endloop = False
                found_start = False
                use_tx = start_tx
                if option2 is not None:
                    use_tx = before_tx
                for this_index, this_range in enumerate(self.tx_range_list):
                    this_year = self.year_list[this_index]
                    if (this_range[0] <= start_tx <= this_range[1]) or (this_range[0] <= use_tx <= this_range[1]) or (
                            this_range[0] <= end_tx <= this_range[1]):
                        this_output = read_option(output_filename, self.path + str(this_year) + '/')
                        if len(this_output[this_output['tx_index'].isin([use_tx])]) > 0:
                            found_start = True
                            this_output = this_output[(this_output['tx_index'] >= use_tx) & (this_output['tx_index'] <= end_tx)]
                            this_input = read_option(input_filename, self.path + str(this_year) + '/')
                            this_input = this_input[(this_input['tx_index'] >= use_tx) & (this_input['tx_index'] <= end_tx)]
                            if len(this_output[this_output['tx_index'].isin([end_tx])]) > 0:
                                endloop = True
                            self.tx_input = self.tx_input.append(this_input)
                            self.tx_output = self.tx_output.append(this_output)
                            if endloop:
                                break
                        else:
                            if found_start:
                                if len(this_output[this_output['tx_index'] >= end_tx]) > 0:
                                    this_output = this_output[this_output['tx_index'] <= end_tx]
                                    this_input = this_input[this_input['tx_index'] <= end_tx]
                                    endloop = True
                                self.tx_input = self.tx_input.append(this_input)
                                self.tx_output = self.tx_output.append(this_output)
                                if endloop:
                                    break

        elif option is None:  # read all data
            for year in self.year_list:
                tx_input = read_option(input_filename, self.path + str(year) + '/')
                self.tx_input = self.tx_input.append(tx_input)
                tx_output = read_option(output_filename, self.path + str(year) + '/')
                self.tx_output = self.tx_output.append(tx_output)
                tx_height = read_option(tx_height_filename, self.path + str(year) + '/')
                self.tx_height = self.tx_height.append(tx_height)

        self.tx_height = self.tx_height[self.tx_height.index.isin(self.tx_output['tx_index'])]

    def tx_taint_search(self, target_tx=None, depth_limit=-1, continue_mode=False,
                        taint_limit=None, case_name=None):
        """
        Search for connected transaction to the original and taint any directly connected transaction.

        :param target_tx: list of transaction index to starting tainting from.
        :param depth_limit: Limit how many transaction depth to search.
        :param continue_mode: Continue from previous run. Will load from file with the same case_name.
        :param taint_limit: list of three items [string to indicate for exclude during ('taint') of after tainting ('after'), dataframe of addresses for checking, dataframe of transactions]
        :param case_name: Name of the case used for saving into file.

        :return: DataFrame with tainted transaction outputs.
        """

        tx_output = self.tx_output
        if 'tx_index' in tx_output.columns:
            tx_output = tx_output.reset_index().set_index('tx_index')
        else:
            tx_output.index.names = ['tx_index']

        self.case_name = case_name
        self.target_tx = target_tx

        depth = 0  # depth of search
        if continue_mode:
            searching_tx = pd.read_csv('taintresults/' + self.case_name + 'searching_tx' + str(self.option).replace(' ', '') + '.csv',
                                       index_col='tx_index')
            tx_tainted = pd.read_csv('taintresults/' + self.case_name + 'tx_tainted' + str(self.option).replace(' ', '') + '.csv',
                                     index_col='tx_index')
            depth = list(sorted(set(tx_tainted['depth'].tolist())))[-1] + 1
        else:
            searching_tx = pd.DataFrame()  # list of output to search
            tx_tainted = pd.DataFrame()  # list of tainted outputs

            if target_tx is not None:
                tx_tainted = tx_output[tx_output.index.isin(target_tx)]

                tx_tainted.is_copy = False
                searching_tx = tx_tainted

                tx_tainted['depth'] = depth
                tx_tainted['taint_value'] = tx_tainted['output_value']
                tx_tainted['clean_value'] = tx_tainted['output_value'] - tx_tainted['taint_value']
                depth += 1

        if os.path.isdir('taintresults') is False:
            os.makedirs('taintresults')

        if taint_limit is not None and taint_limit[0] not in ['after', 'taint']:
            raise Exception(
                'Unknown service limit input: use "after" for remove service transaction after finish tainting or "taint" for remove service transaction during tainting')

        logging.info('Start Search')
        if continue_mode is False:
            searching_tx = tx_output[tx_output.index.isin(searching_tx['spent_index'])]
            searching_tx['depth'] = depth
        while depth != depth_limit and len(searching_tx) > 0:
            if taint_limit is not None and taint_limit[0] == 'taint':
                if taint_limit[1] is not None:
                    searching_tx = searching_tx[
                        ~searching_tx['adr_index'].isin(taint_limit[1].index)]
                if taint_limit[2] is not None:
                    searching_tx = searching_tx[
                        ~searching_tx['tx_index'].isin(taint_limit[2].index)]

            tx_tainted = tx_tainted.append(searching_tx[~searching_tx.index.isin(tx_tainted.index)])
            tx_output = tx_output[~tx_output.index.isin(searching_tx.index)]
            searching_tx = tx_output[tx_output.index.isin(searching_tx['spent_index'])]
            searching_tx['depth'] = depth
            if len(searching_tx) > 0:  # remove tx before  the first tx found in searching_tx
                tx_output = tx_output[tx_output.index > searching_tx.index[0]]
            depth += 1

            if depth % 5000 == 0 and self.case_name is not None:  # temporary save
                tx_tainted.to_csv('taintresults/' + self.case_name + 'tx_tainted' + str(self.option).replace(' ', '') + '.csv')
                searching_tx.to_csv('taintresults/' + self.case_name + 'searching_tx' + str(self.option).replace(' ', '') + '.csv')

        # Final touch
        tx_tainted = tx_tainted.reset_index().drop_duplicates(subset=['tx_index', 'adr_index', 'output_value', 'spent_index']).set_index(
            'tx_index')  # remove duplicate
        logging.info('End, now adding extra information')
        if taint_limit is not None and taint_limit[0] == 'after':  # remove transaction reaching identified addresses and transactions
            if taint_limit[1] is not None:
                tx_tainted = remove_service(tx_tainted, taint_limit[1])
            if taint_limit[2] is not None:
                tx_tainted = remove_txchain(tx_tainted, taint_limit[2])

        tx_tainted = tx_tainted.sort_index().reset_index().set_index('output_index')

        if self.case_name is not None:
            tx_tainted.to_pickle('taintresults/' + self.case_name + 'tx_tainted' + str(self.option).replace(' ', '') + '.pkl')

            try:  # remove saved for continue csv file
                os.remove('taintresults/' + self.case_name + 'tx_tainted' + str(self.option).replace(' ', '') + '.csv')
                os.remove('taintresults/' + self.case_name + 'searching_tx' + str(self.option).replace(' ', '') + '.csv')
            except:
                pass
        return tx_tainted

    def haircut_distribute(self, tx_tainted, tx_input, search_df):
        """
        Distribute tainted coins according to the proportion.

        :param tx_tainted: DataFrame to perform distribution on.
        :param tx_input: tx_input data.
        :param search_df: searching_tx from policy_tx_taint as it should be run in on going search.

        :return: DataFrame with tainted coins from inputs distributed to outputs proportionally

        Note that it is possible for tainted coin to be so proportionally small (less than 1) that not all output can receive tainted coins
        """

        temp_search_df = tx_tainted[tx_tainted['spent_index'].isin(search_df['tx_index'])]  # get taint of previous tx
        haircut_input_taint = get_inout(tx_input, search_df, 'tx_index')  # get input of current tx
        haircut_input_taint = haircut_input_taint[(haircut_input_taint['adr_index'].isin(temp_search_df['adr_index'])) &
                                                  (haircut_input_taint['tx_index'].isin(temp_search_df['spent_index']))].reset_index().rename(
            columns={'tx_index': 'spent_index', 'spent_index': 'tx_index'})
        haircut_input_taint = temp_search_df.merge(haircut_input_taint, how='left',
                                                  left_on=['spent_index', 'adr_index'], right_on=['spent_index', 'adr_index'])
        haircut_input_taint = haircut_input_taint.groupby('spent_index').sum()
        haircut_input_taint['fee_value'] = haircut_input_taint['output_value'] - haircut_input_taint['input_value']
        temp_search_df = temp_search_df.drop(columns=['clean_value', 'output_value', 'adr_index']).groupby('tx_index').sum()['taint_value'].values[0]
        fee_value = haircut_input_taint['fee_value'].values[0]
        if fee_value > 0:
            temp_search_df = round(temp_search_df - (temp_search_df * (fee_value / haircut_input_taint['input_value'].values[0])))

        haircut_output_taint = search_df
        output_value_sum = haircut_output_taint.groupby('tx_index').sum()['output_value'].values[0]
        haircut_output_taint['taint_value'] = haircut_output_taint['output_value'] * temp_search_df / output_value_sum  # distribute
        haircut_output_taint['taint_value'] = haircut_output_taint['taint_value'].values.round()  # round number 

        # fix round to make value as close to original as possible
        sum_fix = temp_search_df  
        sum_fix2 = haircut_output_taint.drop(columns=['spent_index', 'adr_index']).groupby('tx_index').sum()['taint_value'].values[0]
        sum_fix = sum_fix - sum_fix2
        if sum_fix != 0:
            each_sum_fix = round(sum_fix / len(haircut_output_taint))
            if each_sum_fix == 0:
                each_sum_fix = 1
                if sum_fix < 0:
                    each_sum_fix = -1
            how_many = round(sum_fix / each_sum_fix)
            while how_many != 0:
                for this_index in range(0, len(haircut_output_taint)):
                    haircut_output_taint.iloc[this_index, haircut_output_taint.columns.get_loc('taint_value')] = haircut_output_taint.iloc[
                                                                                                                    this_index, haircut_output_taint.columns.get_loc(
                                                                                                                        'taint_value')] + each_sum_fix
                    how_many = how_many - 1
                    if how_many == 0:
                        break
        return haircut_output_taint

    def order_tainting(self, tx_input, search_df, this_index, taint_record):
        """
        Assign transaction inputs with clean or taint classification for distribution later.

        :param tx_input: tx_input data.
        :param search_df: DataFrame of transaction outputs of the current searching transaction
        :param this_index: Current transaction index.
        :param taint_record: The record dataframe.

        :return: DataFrame of transaction inputs
        """

        input_taint = tx_input[tx_input['tx_index'].isin(search_df['tx_index'])]
        input_taint['taint'] = np.nan
        # find which taint or clean
        temp_check_df = taint_record[taint_record['spent_index'].isin(this_index)].set_index('output_index').drop(columns='total_amount')
        spent = temp_check_df['spent_index']
        temp_check_df['spent_index'] = temp_check_df['tx_index']
        temp_check_df['tx_index'] = spent
        try:
            for item in list(set(temp_check_df.index)):
                row_num = input_taint.reset_index()
                row_num = row_num[row_num['output_index'] == item].index[0]
                input_taint = input_taint.drop(item)
                input_taint = pd.concat([input_taint.iloc[:row_num], temp_check_df[temp_check_df.index == item], input_taint.iloc[row_num:]])
        except:
            pass

        input_taint['taint'] = input_taint['taint'].fillna('c')  # any input not in record is clean

        run = 0
        while True:
            if len(input_taint) > run:  # merge same taint type in continuous order
                if len(input_taint) > run + 1:
                    coin_type = input_taint.iloc[run]['taint']
                    next_one = input_taint.iloc[run + 1]
                    while next_one['taint'] == coin_type:
                        input_taint.iloc[run, input_taint.columns.get_loc('input_value')] += next_one['input_value']
                        input_taint = input_taint.drop(next_one.name)
                        if len(input_taint) > run + 1:
                            next_one = input_taint.iloc[run + 1]
                        else:
                            break
                run += 1
            else:
                break
        if len(input_taint) > 0 and len(input_taint[input_taint['taint'] == 't']) > 0:
            fee_value = input_taint['input_value'].sum() - search_df['output_value'].sum()
            if fee_value > 0:  # haircut fee distribute
                input_sum = input_taint['input_value'].sum()
                input_taint['input_value'] = input_taint['input_value'] - (input_taint['input_value'] * (fee_value / input_sum))
                input_taint['input_value'] = input_taint['input_value'].astype('int')
        else:
            input_taint = []

        return input_taint

    def policy_tx_taint(self, tx_tainted, policy, test=0, keep_full_clean=True, input_fee_file=None, show_progress=False, continue_mode=False):
        """
        Distribute tainted coins according to the policy/strategy.

        :param tx_tainted: DataFrame to perform taint analysis.
        :param policy: List of accepted policy: poison, haircut, dirtyfirst/df, puredirtyfirst/pdf and variants of In-Out taint policy. In and out string can be in various acceptable variant combination, currently accept IN: f(first), l(last), t(taint), c(clan) b or h(biggest/highest), s(smallest,lowest); Out:f(first), l(last), b or h(biggest/highest) (e.g., FIFO, LIFO, TIHO).
        :param test: Number to limit how many transactions to run, 0 means running until the end.
        :param keep_full_clean: Keep transaction outputs with completely clean coins.
        :param input_fee_file: Use transaction fee file instead of haircut fee distribution.
        :param show_progress: Show progress in console.
        :param continue_mode: Continue from the previous run. Will load from file with the same case_name.

        :return: DataFrame with distributed tainted coins
        """

        self.policy = policy.lower()
        tx_input = self.tx_input[self.tx_input['tx_index'].isin(tx_tainted['tx_index'])]
        tx_input.is_copy = False
        tx_output = self.tx_output[self.tx_output['tx_index'].isin(tx_tainted['tx_index'])]
        tx_output.is_copy = False
        new_tainted_tx = pd.DataFrame()

        if input_fee_file is None:  # haircut fee distribution
            tx_fee_df = pd.DataFrame(index=tx_input.drop(columns=['adr_index', 'spent_index']).groupby('tx_index').sum().index)
            tx_fee_df['input_sum'] = tx_input.drop(columns='adr_index').groupby('tx_index').sum()['input_value']
            tx_fee_df['fee_value'] = tx_fee_df['input_sum'] - \
                                   tx_output.drop(columns=['adr_index', 'spent_index']).groupby('tx_index').sum()['output_value']
            tx_fee_df = tx_fee_df.reset_index()
            tx_input = tx_input.reset_index().merge(tx_fee_df, how="left", left_on='tx_index', right_on='tx_index').set_index('output_index')

            tx_input['fee_value'] = tx_input['input_value'] / tx_input['input_sum'] * tx_input['fee_value']
            tx_input['fee_value'] = tx_input['fee_value'].fillna(value=0)
            tx_input['fee_value'] = tx_input['input_value'] - tx_input['fee_value']
            tx_input['fee_value'] = tx_input['fee_value'].values.round()
            tx_input['input_value'] = tx_input['fee_value'].astype(int)
            tx_input = tx_input.drop(columns=['fee_value', 'input_sum'])

        else:  # use transaction fee replacement file instead
            tx_input_fee = input_fee_file
            tx_input_fee = tx_input_fee[tx_input_fee.index.isin(tx_input.index)]
            tx_input['input_value'] = tx_input_fee

        logging.info('start ' + str(self.case_name) + self.policy)
        if (self.policy in ('dirtyfirst', 'puredirtyfirst', 'df', 'pdf')) or (
                len(self.policy) == 4 and self.policy[1] == 'i' and self.policy[3] == 'o'):  # dirtyfirst or In-Out taint
            if continue_mode is False:  # create record file
                self.record = pd.DataFrame()
                target_tx = tx_tainted[tx_tainted['taint_value'] > 0]['tx_index']
                self.record = tx_output[tx_output['tx_index'].isin(target_tx)].reset_index().rename(columns={'output_value': 'input_value'})
                self.record.is_copy = False
                self.record['total_amount'] = self.record['input_value']
                self.record['taint'] = 't'

            searching_tx = tx_tainted[tx_tainted['taint_value'] > 0]
            searching_tx = searching_tx[~searching_tx['spent_index'].isnull()]
            searching_tx = searching_tx[~searching_tx['tx_index'].isin(searching_tx['spent_index'])].sort_values(
                'spent_index')  # remove already distributed outputs
            already_search = pd.Series()

            while len(searching_tx) > 0:
                try:
                    searching_tx = searching_tx.sort_values('spent_index')
                    this_search = [searching_tx['spent_index'].values[0]]
                    tainted_input = self.order_tainting(tx_input, tx_tainted[tx_tainted['tx_index'].isin(this_search)], this_search, self.record)

                    if len(tainted_input) > 0:  # reorder ditribute order base on the policy
                        if self.policy in ('dirtyfirst', 'puredirtyfirst', 'df', 'pdf'):  # use FIFO
                            temp_tainted_tx = tx_output[tx_output['tx_index'].isin(this_search)]
                        else:
                            if self.policy[0] == 'l':  # Last In
                                tainted_input = tainted_input.iloc[::-1]
                            elif self.policy[0] == 't':  # Taint In
                                tainted_input = tainted_input.sort_values('taint', ascending=False)
                            elif self.policy[0] == 'c':  # Clean In
                                tainted_input = tainted_input.sort_values('taint', ascending=True)
                            elif self.policy[0] == 'f':  # First In
                                pass  # do nothing since it go according to natural order in blockchain
                            elif self.policy[0] in ('b', 'h'):  # Biggest/Highest
                                tainted_input = tainted_input.sort_values('input_value', ascending=False)
                            elif self.policy[0] == 's':  # Smallest/Lowest (commonly called LO)
                                tainted_input = tainted_input.sort_values('input_value', ascending=True)
                            else:
                                logging.warning(self.policy[0] + ' of ' + self.policy + 'not found')

                            if self.policy[2] == 'f':  # First Out
                                temp_tainted_tx = tx_output[tx_output['tx_index'].isin(this_search)]
                            elif self.policy[2] == 'l':  # Last Out
                                temp_tainted_tx = tx_output[tx_output['tx_index'].isin(this_search)].iloc[::-1]
                            elif self.policy[2] in ('b', 'h'):  # Biggest/Highest
                                temp_tainted_tx = tx_output[tx_output['tx_index'].isin(this_search)].sort_values('output_value', ascending=False)
                            elif self.policy[2] == 's':  # Smallest/Lowest
                                temp_tainted_tx = tx_output[tx_output['tx_index'].isin(this_search)].sort_values('output_value', ascending=True)
                            else:
                                logging.warning(self.policy[2] + ' of ' + self.policy + 'not found')

                        temp_tainted_tx.is_copy = False
                        temp_tainted_tx['taint_value'] = np.nan
                        policy_df = tainted_input.reset_index(drop=True)
                        value_list = policy_df['input_value'].tolist()

                        tainted_list = temp_tainted_tx['output_value'].tolist()
                        check_list = pd.DataFrame(columns=['policy_index', 'tainted_index', 'amount'])
                        check_list.is_copy = False
                        run_index, tainted_index, run = 0, 0, 0

                        for m in tainted_list:  # distribute taint and clean portion to output
                            try:
                                k = m
                                m -= value_list[run_index]
                                if m > 0:
                                    while m > 0:
                                        check_list.loc[run] = [run_index, tainted_index, value_list[run_index]]
                                        run_index += 1
                                        run += 1
                                        k = m
                                        m -= value_list[run_index]
                                if m < 0:
                                    check_list.loc[run] = [run_index, tainted_index, value_list[run_index] - (value_list[run_index] - k)]
                                    run += 1
                                    value_list[run_index] -= k
                                elif m == 0:
                                    check_list.loc[run] = [run_index, tainted_index, value_list[run_index]]
                                    run_index += 1
                                    run += 1
                                tainted_index += 1
                            except:
                                break

                        check_list2 = policy_df[policy_df['taint'].isin(['t'])].index
                        check_list2 = check_list[check_list['policy_index'].isin(check_list2)]
                        check_list2 = check_list2.drop(columns=['policy_index']).rename(columns={'amount': 'taint_value'})
                        check_list2['tainted_index'] = check_list2['tainted_index'].astype(int)
                        check_list2 = check_list2.set_index('tainted_index')

                        for p in check_list2.index:
                            temp_tainted_tx.iloc[p, temp_tainted_tx.columns.get_loc('taint_value')] = check_list2[check_list2.index.isin([p])][
                                'taint_value'].sum()
                        temp_tainted_tx['taint_value'] = temp_tainted_tx['taint_value'].fillna(0)
                        temp_tainted_tx['clean_value'] = temp_tainted_tx['output_value'] - temp_tainted_tx['taint_value']
                        tx_tainted = tx_tainted[~tx_tainted['tx_index'].isin(temp_tainted_tx['tx_index'])]
                        new_tainted_tx = new_tainted_tx.append(temp_tainted_tx)

                        check_list['tainted_index'] = check_list['tainted_index'].astype(int)
                        check_list['policy_index'] = check_list['policy_index'].astype(int)
                        check_list['check_index'] = check_list['tainted_index']
                        for z in list(set(check_list['tainted_index'].values.tolist())):
                            check_list['tainted_index'] = check_list['tainted_index'].replace(z, temp_tainted_tx.iloc[z]['spent_index'])

                        # get total tx value
                        merge_out_value = temp_tainted_tx.drop(columns=['spent_index', 'taint_value', 'clean_value']).reset_index()
                        check_list = check_list.set_index('check_index', drop=False).merge(merge_out_value, left_index=True, right_index=True,
                                                                                           how='left').rename(
                            columns={'output_value': 'total_amount'}).reset_index(drop=True)

                        # setup tx to add in record
                        policy_df = policy_df.drop(columns=['adr_index', 'tx_index'])
                        check_list = check_list.set_index('policy_index').merge(policy_df, left_index=True, right_index=True).drop(
                            columns=['spent_index', 'input_value'])
                        check_list = check_list.rename(
                            columns={'amount': 'input_value', 'tainted_index': 'spent_index'}).drop(columns=['check_index']).reset_index(drop=True)

                        self.record = self.record.append(check_list).reset_index(drop=True)

                        searching_tx = searching_tx.append(temp_tainted_tx)
                        if self.policy in ('dirtyfirst', 'puredirtyfirst', 'df', 'pdf'):  # remove tx with clean coins
                            found_clean = searching_tx[searching_tx['clean_value'] > 0]
                            searching_tx = searching_tx[~searching_tx['tx_index'].isin(found_clean['tx_index'])]
                        searching_tx = searching_tx[searching_tx['spent_index'].isin(tx_tainted['tx_index'])]
                        searching_tx = searching_tx[~searching_tx['spent_index'].isnull()]
                        searching_tx = searching_tx[searching_tx['taint_value'] > 0]
                    else:  # no tainted coints
                        temp_tainted_tx = tx_tainted[tx_tainted['tx_index'].isin(this_search)]
                        temp_tainted_tx['taint_value'] = 0
                        temp_tainted_tx['clean_value'] = temp_tainted_tx['output_value'] - temp_tainted_tx['taint_value']
                        tx_tainted = tx_tainted[~tx_tainted['tx_index'].isin(temp_tainted_tx['tx_index'])]
                        new_tainted_tx = new_tainted_tx.append(temp_tainted_tx)

                    already_search = already_search.append(pd.Series(this_search), ignore_index=True)
                    searching_tx = searching_tx[~searching_tx['spent_index'].isin(already_search)]
                    searching_tx = searching_tx.drop_duplicates(subset='spent_index')
                    if show_progress:
                        logging.info("progress " + str(self.case_name) + self.policy + " " + str(len(tx_tainted)) + " at index " + str(
                            this_search) + " have " + str(len(searching_tx)))
                    tx_input = tx_input[~tx_input['tx_index'].isin(already_search)]
                    tx_output = tx_output[~tx_output['spent_index'].isin(already_search)]
                    
                    test -= 1
                    if test == 0:
                        break
                        
                except KeyboardInterrupt:  # user interupt, save progress
                    if self.case_name is not None:
                        tx_tainted.to_pickle(
                            'taintresults/' + self.case_name + self.policy + 'dftainted' + str(self.option).replace(' ', '') + '.pkl')
                        self.record.to_pickle('taintresults/' + self.case_name + self.policy + 'record' + str(self.option).replace(' ', '') + '.pkl')
                    break
            tx_tainted = tx_tainted.append(new_tainted_tx)
            tx_tainted = tx_tainted.fillna(value=0)
            tx_tainted = tx_tainted.sort_index()
            tx_tainted['clean_value'] = tx_tainted['output_value'] - tx_tainted['taint_value']

            if self.policy == 'puredirtyfirst' or self.policy == 'pdf':  # remove all outputs with clean value
                found_clean = tx_tainted[tx_tainted['clean_value'] > 0]
                tx_tainted = tx_tainted[~tx_tainted['tx_index'].isin(found_clean['tx_index'])]
            self.record['input_value'] = self.record['input_value'].astype(int)
            self.record['taint'] = self.record['taint'].astype(str)

        elif self.policy == 'poison':  # poison simply taint fully
            tx_tainted['taint_value'] = tx_tainted['output_value']

        elif self.policy == 'haircut':
            this_list = sorted(list(set(tx_tainted[tx_tainted['taint_value'].isnull()]['tx_index'].tolist())))
            for i in this_list:
                tainted_input = self.haircut_distribute(tx_tainted, self.tx_input, tx_tainted[tx_tainted['tx_index'] == i])
                tainted_input = tainted_input.sort_index()

                tainted_input['clean_value'] = tainted_input['output_value'] - tainted_input['taint_value']
                new_tainted_tx = new_tainted_tx.append(tainted_input)
                if show_progress:
                    logging.info(
                        "progress " + str(self.case_name) + self.policy + " " + str(len(new_tainted_tx)) + " out of " + str(len(tx_tainted)))
                test -= 1
                if test == 0:
                    break

            tx_tainted = tx_tainted[~tx_tainted['tx_index'].isin(new_tainted_tx['tx_index'])].append(new_tainted_tx)
            tx_tainted = tx_tainted.fillna(value={'taint_value': 0})
            tx_tainted = tx_tainted.sort_index()

        else:  # policy name not match any
            logging.warning('Policy not exist')
            return

        self.record.rename(columns={'input_value': 'portion_value'}, inplace=True)

        if keep_full_clean is False:
            original = tx_tainted.groupby('tx_index').sum()
            original = original[original['taint_value'] > 0]
            tx_tainted = tx_tainted[tx_tainted['tx_index'].isin(original['tx_index'])]

        if self.case_name is not None:
            tx_tainted.to_pickle('taintresults/' + self.case_name + self.policy + 'dftainted' + str(self.option).replace(' ', '') + '.pkl')
            self.record.to_pickle('taintresults/' + self.case_name + self.policy + 'record' + str(self.option).replace(' ', '') + '.pkl')

        return tx_tainted

    def dirtyfirst(self, policy, tx_tainted):
        """process tx_tainted to keep only full dirty, policy must be either 'dirtyfirst / df' or 'puredirtyfirst / pdf'"""
        reset = False
        if 'tx_index' in tx_tainted.columns:
            reset = True
            tx_tainted = tx_tainted.reset_index().set_index('tx_index')
        tx_tainted_old = tx_tainted

        original = tx_tainted[~tx_tainted.index.isin(tx_tainted['spent_index'])]
        no_clean_df = tx_tainted[tx_tainted['clean_value'] > 0]  # find tx with clean
        tx_tainted = tx_tainted[~tx_tainted.index.isin(no_clean_df.index)]

        while len(tx_tainted[(~tx_tainted.index.isin(original.index)) & (~tx_tainted.index.isin(tx_tainted['spent_index']))]) > 0:
            tx_tainted = tx_tainted[(tx_tainted.index.isin(original.index)) | (
                tx_tainted.index.isin(tx_tainted['spent_index']))]  # keep tx in original and has spent_index connection

        if policy == 'dirtyfirst' or policy == 'df':  # keep only the first tx that clean coin show up
            no_clean_df = tx_tainted_old[tx_tainted_old.index.isin(no_clean_df.index)]
            no_clean_df = no_clean_df[no_clean_df.index.isin(tx_tainted['spent_index'])]  # keep only the first no clean
            tx_tainted = tx_tainted.append(no_clean_df)

        elif policy == 'puredirtyfirst' or policy == 'pdf':  # keep only fully taint tx
            pass

        tx_tainted = tx_tainted.sort_index()
        if reset:
            tx_tainted = tx_tainted.reset_index().set_index('output_index')

        return tx_tainted

    def adr_taint_search(self, target_adr, depth_limit=-1, continue_mode=False, taint_limit=None, backward=False, case_name=None):
        """
        Search for connected adr to the original and taint any direct address either forward or backward.
        specify depth_limit option to limit how many transaction depth to search but shouldn't be used unless for testing

        :param target_adr: List of target address index.
        :param depth_limit: DataFrame for public checking.
        :param continue_mode: Continue from previous run. Will load from file with the same case_name.
        :param taint_limit: list of two items [string to indicate for exclude during ('taint') of after tainting ('after'), DataFrame of addresses for checking]
        :param backward: Run the tainting backward instead of forward.
        :param case_name: Name of the case used for saving into file.

        :return: DataFrame with tainted transactions and DataFrame with tainted addresses.
        """

        self.case_name = case_name
        depth = 0  # depth of search
        done = []

        tx_output = self.tx_output
        if 'tx_index' in tx_output.columns:
            tx_output = tx_output.reset_index().set_index('tx_index')
        else:
            tx_output.index.names = ['tx_index']

        tx_input = self.tx_input
        if 'tx_index' in tx_input.columns:
            tx_input = tx_input.reset_index().set_index('tx_index')
        else:
            tx_input.index.names = ['tx_index']

        if continue_mode:
            searching_tx = pd.read_csv('taintresults/' + self.case_name + 'searching_tx' + str(self.option).replace(' ', '') + '.csv',
                                       index_col='tx_index')
            tx_tainted = pd.read_csv('taintresults/' + self.case_name + 'tx_tainted' + str(self.option).replace(' ', '') + '.csv',
                                     index_col='tx_index')
            adr_tainted = pd.read_csv('taintresults/' + self.case_name + 'adr_tainted' + str(self.option).replace(' ', '') + '.csv',
                                      index_col='addressindex')
        else:
            tx_tainted = pd.DataFrame()  # list of tainted outputs
            adr_tainted = pd.DataFrame()
            adr_found = tx_output[tx_output['adr_index'].isin(target_adr)]
            adr_tainted = adr_tainted.append(tx_output[tx_output.index.isin(adr_found.index)])

            for i in tx_input[tx_input.index.isin(tx_tainted.index)]['adr_index'].values:
                adr_tainted.loc[i] = np.nan
            #         adr_tainted['role'] = adr_tainted['role'].fillna(value=1)
            adr_tainted.index = adr_tainted.index.rename('adr_index')

            if backward:
                searching_tx = tx_output[tx_output['adr_index'].isin(adr_tainted['adr_index'])]
                searching_tx = tx_input[tx_input.index.isin(searching_tx.index)]
            else:
                searching_tx = tx_input[tx_input['adr_index'].isin(adr_tainted['adr_index'])]
                searching_tx = tx_output[tx_output.index.isin(searching_tx.index)]

        logging.info('Start Search')

        while depth != depth_limit and len(searching_tx) > 0:
            tx_tainted = tx_tainted.append(searching_tx[~searching_tx.index.isin(tx_tainted.index)])
            tx_tainted = tx_tainted.reset_index().drop_duplicates().set_index('tx_index')

            done = done + searching_tx['adr_index'].tolist()
            done = list(set(done))
            if backward:
                searching_tx = tx_output[tx_output['adr_index'].isin(searching_tx['adr_index'])]
                searching_tx = tx_input[tx_input.index.isin(searching_tx.index)]
            else:  # don't taint any previous tx before the tainted one
                searching_tx = tx_input[tx_input['adr_index'].isin(searching_tx['adr_index'])]
                searching_tx = tx_output[tx_output.index.isin(searching_tx.index)]
            print(len(done), len(searching_tx), depth)

            searching_tx = searching_tx[~searching_tx['adr_index'].isin(done)]

            # add adr from searching_tx into adr_tainted
            ii = pd.DataFrame(index=tx_tainted[~tx_tainted['adr_index'].isin(adr_tainted.index)]['adr_index'].values.tolist())
            ii = ii.groupby(ii.index).first()
            adr_tainted = adr_tainted.append(ii)
            adr_tainted = adr_tainted[~adr_tainted.index.duplicated()]
            # adr_tainted['first_block'] = adr_tainted['first_block'].fillna(value=self.blocksearch)

            if taint_limit is not None and taint_limit[0] == "taint":  # discontinue tx of limit adr from further searching if true
                searching_tx = searching_tx[
                    ~searching_tx['adr_index'].isin(taint_limit[1].index)]  # remove tx with adr input belong to service
            depth += 1
            if depth % 5000 == 0:
                tx_tainted.to_csv('taintresults/' + self.case_name + 'tx_tainted' + str(self.option).replace(' ', '') + '.csv')
                adr_tainted.to_csv('taintresults/' + self.case_name + 'adr_tainted' + str(self.option).replace(' ', '') + '.csv')
                searching_tx.to_csv('taintresults/' + self.case_name + 'searching_tx' + str(self.option).replace(' ', '') + '.csv')
        logging.info('End, now adding extra information')  # Final touch to add tainted address stat

        if taint_limit is not None and taint_limit[0] == 'after':  # remove transaction reaching identified addresses and transactions
            if taint_limit[1] is not None:
                tx_tainted = remove_service(tx_tainted, taint_limit[1])
                adr_tainted = adr_tainted[adr_tainted.index.isin(tx_tainted['adr_index'])]

        #         first_tx_insert = pd.DataFrame(index=tx_tainted.set_index('adr_index').index)
        #         first_tx_insert['first_taint_tx'] = tx_tainted.set_index('adr_index')['spent_index']
        #         first_tx_insert = first_tx_insert.reset_index().sort_values('first_taint_tx').drop_duplicates(subset='adr_index', keep='first').set_index(
        #             'adr_index').sort_index()
        #         adr_tainted['first_taint_tx'] = first_tx_insert['first_taint_tx']
        tx_tainted = tx_tainted.sort_index()

        input_count = tx_input[tx_input['adr_index'].isin(adr_tainted.index)].reset_index()
        output_count = tx_output[tx_output['adr_index'].isin(adr_tainted.index)].reset_index()
        count = output_count.append(input_count)
        count = count[~count.index.duplicated()].groupby('adr_index').count()
        count = count.sort_index()
        adr_tainted = adr_tainted[~adr_tainted.index.duplicated()].sort_index()
        adr_tainted['tx_count'] = count['tx_index']
        adr_tainted = adr_tainted[['tx_count']]
        #         adr_tainted['remain'] = adr_tainted['output_value'] - adr_tainted[
        #             'input_value']  # find how much coin left in adr

        #         adr_tainted['first_taint_tx'] = tx_tainted.drop_duplicates('adr_index').reset_index().set_index('adr_index').drop(
        #             columns=['clean_value', 'output_value', 'spent_index', 'taint_value']).sort_index()

        # change tx_tainted and adr_tainted with policy
        if self.case_name is not None:
            tx_tainted.to_pickle('taintresults/' + self.case_name + 'tx_tainted' + str(self.option).replace(' ', '') + '.pkl')
            adr_tainted.to_pickle('taintresults/' + self.case_name + 'adr_tainted' + str(self.option).replace(' ', '') + '.pkl')
            try:
                os.remove('taintresults/' + self.case_name + 'tx_tainted' + str(self.option).replace(' ', '') + '.csv')
                os.remove('taintresults/' + self.case_name + 'adr_tainted' + str(self.option).replace(' ', '') + '.csv')
            except:
                pass
        return tx_tainted, adr_tainted

    def filtering(self, tx_tainted, mix_time=None, filter_input=None, filter_output=None, filter_chain=None, filter_reuse=None,
                  filter_mix_fee=None, filter_tx_fee=None, target=None):
        """
        Filter fault-positive transactions.

        :param tx_tainted: DataFrame containing clustered adr for adding more.
        :param mix_time: Maximum mixing time allowed, must be in timedelta64 format (e.g., np.timedelta64(3, 'D'))
        :param filter_input: Number of the transaction inputs.
        :param filter_output: Number of the transaction outputs.
        :param filter_chain: the first item is number for input and second is for output. Item can be none.
        :param filter_reuse: Remove transaction with reused input addresses.
        :param filter_mix_fee: The first item being the number and second item being the type of calculation ('fixed' or 'percent')
        :param filter_tx_fee: The first item is DataFrame with transaction fee, second item is for filter transaction fee value and third item is for filter transaction fee rate.
        :param target: DataFrame containing targeted deposit coin output (optional but required for mix time and mixer fee calculation).

        :return: DataFrame after removing fault-positive results.
        """

        new_tx_tainted = self.tx_output[self.tx_output.index.isin(tx_tainted.index)]

        if mix_time is not None:
            block = read_option(block_filename, self.path)
            first = self.tx_output[self.tx_output.index.isin(target.index)]
            start_tx = first.index[0]
            start_block = self.tx_height[self.tx_height.index == start_tx]['block_index'].values
            start_time = block[block.index == start_block[0]]['time'].values
            end_time = start_time + mix_time
            end_tx = self.tx_height[
                self.tx_height['block_index'] == block['time'].loc[block['time'].dt.date == np.datetime64(end_time[0], 'D')].index[-1]].index[
                -1]

            new_tx_tainted = new_tx_tainted[new_tx_tainted.index >= start_tx]
            new_tx_tainted = new_tx_tainted[new_tx_tainted.index <= end_tx]

        # filtering tx input
        if filter_input is not None:
            df2 = self.tx_input[self.tx_input.index.isin(new_tx_tainted.index)].groupby('tx_index').count()
            df2 = df2[df2['input_value'] == filter_input]
            new_tx_tainted = new_tx_tainted[new_tx_tainted.index.isin(df2.index)]

        # filtering tx output
        if filter_output is not None:
            pet_tx = self.tx_output[self.tx_output.index.isin(new_tx_tainted.index)].groupby('tx_index').count()
            pet_tx = pet_tx[pet_tx['output_value'] == filter_output]
            new_tx_tainted = new_tx_tainted[new_tx_tainted.index.isin(pet_tx.index)]

        # filtering previous and next in same tx pattern chain
        if filter_chain is not None:
            df2 = self.tx_input[~self.tx_input.index.isin(new_tx_tainted.index)].groupby('tx_index').count()
            if filter_chain[0] is not None:
                df2 = df2[df2['input_value'] == filter_chain[0]]
            pet_tx = self.tx_output[~self.tx_output.index.isin(new_tx_tainted.index)].groupby('tx_index').count()
            if filter_chain[1] is not None:
                pet_tx = pet_tx[pet_tx['output_value'] == filter_chain[1]]

            df2 = df2[df2.index.isin(pet_tx.index)]
            df2 = self.tx_input[self.tx_input.index.isin(df2.index)]
            pet_tx = self.tx_output[self.tx_output.index.isin(df2.index)]
            df4 = df2[df2.index.isin(new_tx_tainted['spent_index'])]
            df5 = pet_tx[pet_tx['spent_index'].isin(new_tx_tainted.index)]
            df6 = new_tx_tainted[new_tx_tainted.index.isin(df5['spent_index'])]
            df7 = new_tx_tainted[new_tx_tainted['spent_index'].isin(df4.index)]
            new_tx_tainted = new_tx_tainted[new_tx_tainted.index.isin(df6.index) | new_tx_tainted.index.isin(df7.index)]

        # filtering reused adr
        if filter_reuse is not None:
            df2 = self.tx_input[self.tx_input.index.isin(new_tx_tainted.index)]
            df2 = df2[~df2['adr_index'].isin(filter_reuse.index)]  # find adr that get reused
            new_tx_tainted = new_tx_tainted[~new_tx_tainted.index.isin(df2.index)]  # remove tx with reused adr

        # filtering mixing fee
        if filter_mix_fee is not None:
            if filter_mix_fee[1] == 'fixed':
                new_tx_tainted = new_tx_tainted[new_tx_tainted['output_value'] <= target['output_value'].values[0] - filter_mix_fee[0]]
            elif filter_mix_fee[1] == 'percent':
                new_tx_tainted = new_tx_tainted[
                    new_tx_tainted['output_value'] <= target['output_value'].values[0] - (target['output_value'].values[0] * filter_mix_fee[0] / 100)]

        # tx fee
        if filter_tx_fee is not None:
            tx_fee = filter_tx_fee[0]
            tx_fee = tx_fee[tx_fee.index.isin(tx_tainted.index)]
            check_this = tx_fee
            check_this = check_this[check_this.index.isin(new_tx_tainted.index)]
            if filter_tx_fee[1] is not None:
                check_this = check_this[check_this['fee_value'] == filter_tx_fee[1]]
            if filter_tx_fee[2] is not None:
                check_this = check_this[check_this['fee_rate'] == filter_tx_fee[2]]
            new_tx_tainted = new_tx_tainted[new_tx_tainted.index.isin(check_this.index)]
        return new_tx_tainted
