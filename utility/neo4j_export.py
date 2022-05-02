import csv
import os

import numpy as np
import pandas as pd

pd.options.mode.chained_assignment = None


def neo4j_export(tttx, export_path='export/', option='detailed'):
    """
    Export dataframes from TaintedTX object to csv file for neo4j import.

    :param tttx: TaintedTX object.
    :param export_path: String of folder path for the exported files.
    :param option: String of the option name that dictates how the neo4j exported data structure.

    'detailed' includes block/tx/output/input/adr as their own nodes.
    'simple' includes only adr and tx as node and put output/input as relationship detail.
    'outputonly' includes only output as node.

    :return: csv files containing header for nodes and relationships.

    """
    if option == 'detailed':
        """Use this import code for neo4j import
        $NEO4J_HOME/bin/neo4j-admin import \
        --mode csv \
        --database blockchain.db \
        --nodes:Block blocks_header.csv,blocks.csv \
        --nodes:Transaction txhash_header.csv,txhash.csv \
        --nodes:Address address_header.csv,adr.csv \
        --nodes:Output output_header.csv,output.csv \
        --nodes:Input input_header.csv,input.csv \
        --relationships:In txheight_header.csv,tx_height.csv \
        --relationships:OUTPUT rel_txoutput_header.csv,rel_txoutput.csv \
        --relationships:INPUT rel_txinput_header.csv,rel_txinput.csv \
        --relationships:RECEIVE rel_addressoutput_header.csv,rel_addressoutput.csv \
        --relationships:SEND rel_addressinput_header.csv,rel_addressinput.csv \ """
        export_path += 'detailed/'
        oldoutputindex = tttx.tx_output['output_index']
        tttx.tx_output['output_index'] = np.arange(len(tttx.tx_output))
        tttx.tx_input['input_index'] = np.arange(len(tttx.tx_input))
        
        # rel_txoutput.csv for link between output and tx
        df2 = tttx.tx_output
        df2 = df2[df2.index.isin(tttx.tx_tainted.index)]
        df2 = df2.drop(columns=['address_index', 'output_value'])
        df2.to_csv(os.path.join(export_path, 'rel_txoutput.csv'), header=False)

        # output.csv for amount of output
        df2 = tttx.tx_output.drop(columns=['address_index'])
        df2 = df2.set_index(['output_index'])
        df2.to_csv(os.path.join(export_path, 'output.csv'), header=False)

        # rel_addressoutput.csv for output adr
        df2 = tttx.tx_output.drop(columns=['output_value'])
        df2 = df2[df2.index.isin(tttx.tx_tainted.index)]
        df2 = df2.set_index(['output_index'])
        df2.to_csv(os.path.join(export_path, 'rel_addressoutput.csv'), header=False)

        # rel_txinput.csv for link between input and tx
        df2 = tttx.tx_input
        df2 = df2[df2.index.isin(tttx.tx_tainted.index)]
        input_address = df2['address_index'].tolist()
        df2 = df2.drop(columns=['address_index', 'input_value'])
        df2.to_csv(os.path.join(export_path, 'rel_txinput.csv'), header=False)

        # input.csv for amount of input
        df2 = tttx.tx_input.drop(columns=['address_index'])
        df2 = df2.set_index(['input_index'])
        df2.to_csv(os.path.join(export_path, 'input.csv'), header=False)

        # rel_addressinput.csv for input adr
        df2 = tttx.tx_input.drop(columns=['input_value'])
        df2 = df2[df2.index.isin(tttx.tx_tainted.index)]
        df2 = df2.set_index(['input_index'])
        df2.to_csv(os.path.join(export_path, 'rel_addressinput.csv'), header=False)

        # blocks.csv for list of block and detail
        tttx.block = tttx.block.reset_index().set_index(['blockindex'])
        tttx.block.to_csv(os.path.join(export_path, 'blocks.csv'), header=False)

        # tx_height.csv for link between tx and block
        df2 = tttx.tx_height[tttx.tx_height.index.isin(tttx.tx_tainted.index)]
        df2.to_csv(os.path.join(export_path, 'tx_height.csv'), header=False)

        # txhash.csv for list of tx hash
        df2 = tttx.tx_check(input_type="index", tx=tttx.tx_tainted.index)
        df2.to_csv(os.path.join(export_path, 'tx_hash.csv'), header=False)
        # create header file for csv import to neo4j

        # blocks.csv header
        with open(os.path.join(export_path, 'blocks_header.csv'), 'w', newline='') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            filewriter.writerow(['blockindex:ID(block)', 'time', 'blockhash', 'miner', 'feeperbyte'])
        csvfile.close()

        # adr.csv header
        with open(os.path.join(export_path, 'address_header.csv'), 'w', newline='') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            filewriter.writerow(['adr:ID(address_index)', 'address_hash'])
        csvfile.close()

        # input.csv header
        with open(os.path.join(export_path, 'input_header.csv'), 'w', newline='') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            filewriter.writerow(['inputindex:ID(input_index)', 'input_value'])
        csvfile.close()

        # output.csv header
        with open(os.path.join(export_path, 'output_header.csv'), 'w', newline='') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            filewriter.writerow(['outputindex:ID(output_index)', 'output_value'])
        csvfile.close()

        # rel_addressinput.csv header
        with open(os.path.join(export_path, 'rel_addressinput_header.csv'), 'w', newline='') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            filewriter.writerow([':END_ID(input_index)', ':START_ID(address_index)'])
        csvfile.close()

        # rel_addressoutput.csv header
        with open(os.path.join(export_path, 'rel_addressoutput_header.csv'), 'w', newline='') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            filewriter.writerow([':START_ID(output_index)', ':END_ID(address_index)'])
        csvfile.close()

        # rel_txinput.csv header
        with open(os.path.join(export_path, 'rel_txinput_header.csv'), 'w', newline='') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            filewriter.writerow([':END_ID(tx_index)', ':START_ID(input_index)'])
        csvfile.close()

        # rel_txoutput.csv header
        with open(os.path.join(export_path, 'rel_txoutput_header.csv'), 'w', newline='') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            filewriter.writerow([':START_ID(tx_index)', ':END_ID(output_index)'])
        csvfile.close()

        # txhash.csv header
        with open(os.path.join(export_path, 'txhash_header.csv'), 'w', newline='') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            filewriter.writerow(['tx_index:ID(tx_index)', 'tx_hash'])
        csvfile.close()

        # tx_height
        with open(os.path.join(export_path, 'txheight_header.csv'), 'w', newline='') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            filewriter.writerow([':START_ID(tx_index)', ':END_ID(block)'])
        csvfile.close()

        # change df back
        tttx.tx_output = oldoutputindex
        tttx.tx_input = tttx.tx_input.drop(columns=['input_index'])

    elif option == 'simple':
        """Use this import code for neo4j import
        $NEO4J_HOME/bin/neo4j-admin import \
        --mode csv \
        --database blockchain.db \
        --nodes:Transaction txhash_header.csv,txhash.csv \
        --nodes:Address address_header.csv,adr.csv \
        --relationships:RECEIVE rel_addressoutput_header.csv,rel_addressoutput.csv \
        --relationships:SEND rel_addressinput_header.csv,rel_addressinput.csv \ """
        export_path += 'simple/'

        # rel_addressoutput.csv for output adr
        df2 = tttx.tx_output
        df2 = df2[df2.index.isin(tttx.tx_tainted.index)]
        df2 = df2[['output_value', 'address_index']]
        df2.to_csv(os.path.join(export_path, 'rel_addressoutput.csv'), header=False)

        # rel_addressinput.csv for input adr
        df2 = tttx.tx_input
        df2 = df2[df2.index.isin(tttx.tx_tainted.index)]
        df2 = df2[['input_value', 'address_index']]
        input_address = df2['address_index'].tolist()
        df2 = df2.groupby(['tx_index', 'address_index']).sum()
        df2 = df2.reset_index().set_index('tx_index')
        df2 = df2[['input_value', 'address_index']]
        df2.to_csv(os.path.join(export_path, 'rel_addressinput.csv'), header=False)

        # txhash.csv for list of tx
        df2 = tttx.tx_check(input_type="index", tx=tttx.tx_tainted.index)

        df2['block'] = tttx.tx_height[tttx.tx_height.index.isin(df2.index)]['block_index'].values.tolist()
        df2.to_csv(os.path.join(export_path, 'txhash.csv'), header=False)
        # create header file for csv import to neo4j

        # adr.csv header
        with open(os.path.join(export_path, 'address_header.csv'), 'w', newline='') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            filewriter.writerow(['adr:ID(address_index)', 'address_hash', 'taint', 'service_name', 'service_type'])
        csvfile.close()

        # rel_addressinput.csv header
        with open(os.path.join(export_path, 'rel_addressinput_header.csv'), 'w', newline='') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            filewriter.writerow([':END_ID(tx_index)', 'input', ':START_ID(address_index)'])
        csvfile.close()

        # rel_addressoutput.csv header
        with open(os.path.join(export_path, 'rel_addressoutput_header.csv'), 'w', newline='') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            filewriter.writerow([':START_ID(tx_index)', 'output', ':END_ID(address_index)'])
        csvfile.close()

        # txhash.csv header
        with open(os.path.join(export_path, 'txhash_header.csv'), 'w', newline='') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            filewriter.writerow(['tx_index:ID(tx_index)', 'tx_hash', 'block'])
        csvfile.close()

    elif option == 'outputonly':
        """Use this import code for neo4j import
        $NEO4J_HOME/bin/neo4j-admin import \
        --mode csv \
        --database blockchain.db \
        --nodes:Output output_header.csv,output.csv \
        --relationships:SEND rel_outputsend_header.csv,rel_outputsend.csv \ """
        export_path += 'outputonly/'
        df2 = tttx.tx_output
        df2 = df2[df2.index.isin(tttx.tx_tainted.index)]
        df3 = tttx.tx_check(input_type="index", tx=tttx.tx_tainted.index)
        df2 = df2.append(df3)
        address_list = tttx.tx_tainted['address_index'].tolist()
        addressdf = tttx.address_check(address_list)
        addressdf = addressdf[addressdf.index.isin(address_list)]
        addressdf = addressdf.reset_index()
        df2 = df2.merge(addressdf, left_on="address_index", right_on="address_index")
        df2 = df2.drop(columns=['output_index', 'address_index'])
        df2.to_csv(os.path.join(export_path, 'output.csv'), header=False)

        # create header file for csv import to neo4j

        with open(os.path.join(export_path, 'output_header.csv'), 'w', newline='') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            filewriter.writerow(['tx_index:ID(tx_index)', 'tx_hash', 'address_hash', 'output_value', 'spent_index'])
        csvfile.close()

        # rel_addressinput.csv header
        with open(os.path.join(export_path, 'rel_outputsend_header.csv'), 'w', newline='') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            filewriter.writerow([':END_ID(tx_index)', 'output_value', ':START_ID(spent_index)'])
        csvfile.close()

    # adr info
    if option != 'outputonly':
        address_list = tttx.tx_tainted['address_index'].tolist()
        address_list = address_list + input_address
        df2 = pd.DataFrame()
        # make one adr into list so can be used with pandas isin command
        addressdf = tttx.address_check(address_list)
        addressdf = addressdf[addressdf.index.isin(address_list)]
        df2 = df2.append(addressdf)
        # stop searching when found all input adr

        df2 = df2[~df2.index.duplicated(keep='first')]
        df2.to_csv(os.path.join(export_path, 'adr.csv'), header=False)
