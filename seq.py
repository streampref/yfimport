#!/usr/bin/python -u
# -*- coding: utf-8 -*-
'''
Module for generation of environments for experiments over Yahoo finance data
using SEQ operator
'''

import csv
import os

from yfimport import TRANSACTION_FILE, TRANSACTION_HEADER, get_max_timestamp


# =============================================================================
# Experiment parameters
# =============================================================================
# Range
RAN = 'ran'
# Slide
SLI = 'sli'
# Identifier attributes
PARAMETER_LIST = [RAN, SLI]

# =============================================================================
# Parameters for query generation
# =============================================================================
# List of ranges
RANGE_LIST = [2, 3, 4, 5, 6]
# Default range
RANGE_DEFAULT = 4
# List of slides
SLIDE_LIST = [1, 2, 3, 4]
# Default slide
SLIDE_DEFAULT = 1

# =============================================================================
# Directories and filenames
# =============================================================================
MAIN_DIR = 'streampref_seq'
DETAIL_DIR = MAIN_DIR + os.sep + 'details'
SUMMARY_DIR = MAIN_DIR + os.sep + 'summary'
RESULT_DIR = MAIN_DIR + os.sep + 'result'
PREF_QUERY_DIR = MAIN_DIR + os.sep + 'queries_seq'
CQL_QUERY_DIR = MAIN_DIR + os.sep + 'queries_cql'
DATA_DIR = MAIN_DIR + os.sep + 'data'
PREF_ENV_DIR = MAIN_DIR + os.sep + 'env_seq'
CQL_ENV_DIR = MAIN_DIR + os.sep + 'env_cql'
DIR_LIST = [MAIN_DIR, DETAIL_DIR, SUMMARY_DIR, RESULT_DIR, DATA_DIR,
            CQL_QUERY_DIR, PREF_QUERY_DIR, PREF_ENV_DIR, CQL_ENV_DIR]

DATA_FILE = DATA_DIR + os.sep + 's.csv'

# =============================================================================
# Other parameters
# =============================================================================
# Algorithms / equivalences
SEQ_ALG = 'seqop'
CQL_ALG = 'cql'
# List of algorithms
ALGORITHM_LIST = [SEQ_ALG, CQL_ALG]

# Timestamp attribute for StremPref streams and tables
TS_ATT = '_TS'
# Flag attribute for StreamPref tables
FL_ATT = '_FL'
# Integer type
INTEGER = 'INTEGER'
# Result fields
RUNTIME = 'runtime'
MEMORY = 'memory'

# =============================================================================
# Queries with SEQ operator
# =============================================================================

# TRANSACTION_ATT = [SYMBOL, SECTOR, COUNTRY, PRICE, VOLUME, METHOD, RATE]

# Queries
# Query with SEQ operator
Q_Z = '''
    SELECT SEQUENCE IDENTIFIED BY symbol, method
    [RANGE {ran} SECOND, SLIDE {sli} SECOND] FROM s;
    '''

# =============================================================================
# Queries with CQL equivalence
# =============================================================================
# RPOS (_pos attribute with original timestamp)
CQL_RPOS_QUERY = 'SELECT _ts AS _pos, * FROM s[RANGE 1 SECOND];'
# SPOS (convert RPOS back to stream format)
CQL_SPOS_QUERY = 'SELECT RSTREAM FROM rpos;'
# W (Window of tuples from SPOS)
CQL_W_QUERY = '''
SELECT _pos, symbol, sector, country, price, volume, method, rate
FROM spos[RANGE {ran} SECOND, SLIDE {sli} SECOND];
'''
# W_1 (Sequence positions from 1 to end)
CQL_W1_QUERY = 'SELECT _pos, symbol, method FROM w;'
# W_i (Sequence positions from i to end,  w_(i-1) - p_(i-1))
CQL_WI_QUERY = '''
SELECT * FROM w{prev}
EXCEPT
SELECT * FROM p{prev};
'''
# P_i (Tuples with minimum _pos for each identifier)
CQL_PI_QUERY = '''
SELECT * FROM w{pos}
EXCEPT
SELECT wa._pos, wa.symbol, wa.method FROM w{pos} AS wa, w{pos} AS wb
WHERE wa._pos > wb._pos
AND wa.symbol = wb.symbol
AND wa.method = wb.method;
'''
CQL_FINALPOS_QUERY = '''
SELECT {pos} AS _pos, w.symbol, w.method, sector, country,
price, volume, rate
FROM p{pos}, w
WHERE p{pos}.symbol = w.symbol
AND p{pos}.method = w.method
AND p{pos}._pos = w._pos
'''

# Strings for registration in environment file
REG_STREAM_STR = '''
REGISTER STREAM s(symbol STRING, sector STRING, country STRING,
             price FLOAT, volume INTEGER, method INTEGER, rate FLOAT)
INPUT '{d}';'''
REG_Z_STR = "REGISTER QUERY seq \nINPUT '{q}';"
REG_RPOS_SPO_STR = \
    "\n\nREGISTER QUERY rpos \nINPUT '" + CQL_QUERY_DIR + "/rpos.cql';" + \
    "\n\nREGISTER QUERY spos \nINPUT '" + CQL_QUERY_DIR + "/spos.cql';"
REG_W_STR = "\n\nREGISTER QUERY w \nINPUT '{q}';"
REG_WI_STR = "\n\nREGISTER QUERY w{pos} \nINPUT '{q}';"
REG_PI_STR = "\n\nREGISTER QUERY p{pos} \nINPUT '{q}';"
REG_CQL_FINAL_STR = "\n\nREGISTER QUERY equiv \nINPUT '{q}';"

# =============================================================================
# Experiment execution
# =============================================================================
# Number of experiment runs
RUN_COUNT = 5
# Command for experiment run
RUN_COMMAND = \
    "streampref -r'|' -e {env} -d {det} -m {ite}"
# Command for calculation of confidence interval
CONFINTERVAL_COMMAND = \
    "confinterval -i {inf} -o {outf} -k {keyf}"
# Command for sort stream file download
SORT_COMMAND = \
    'cat ' + TRANSACTION_FILE + ' | sort -g > ' + DATA_FILE

# =============================================================================


def get_id(experiment_conf, key_list=None):
    '''
    Return experiment identifier
    '''
    id_str = ''
    if key_list is None:
        key_list = PARAMETER_LIST[:]
    key_list.sort()
    for key in PARAMETER_LIST:
        if key in key_list:
            id_str += key + str(experiment_conf[key])
    return id_str


def add_experiment(experiment_list, experiment):
    '''
    Add an experiment into experiment list
    '''
    if experiment not in experiment_list:
        experiment_list.append(experiment.copy())


def gen_experiment_list():
    '''
    Generate the list of experiments
    '''
    exp_list = []
    # Default parameters configuration
    def_conf = {RAN: RANGE_DEFAULT, SLI: SLIDE_DEFAULT}
    # Variations
    var_dict = {RAN: RANGE_LIST, SLI: SLIDE_LIST}
    for parameter in var_dict:
        for value in var_dict[parameter]:
            conf = def_conf.copy()
            conf[parameter] = value
            add_experiment(exp_list, conf)
    return exp_list


def gen_endseq_query(experiment_conf):
    '''
    Generate queries with SEQ operator
    '''
    filename = PREF_QUERY_DIR + os.sep \
        + get_id(experiment_conf, [RAN, SLI]) + '.cql'
    if not os.path.isfile(filename):
        out_file = open(filename, 'w')
        query = Q_Z.format(ran=experiment_conf[RAN],
                                 sli=experiment_conf[SLI])
        out_file.write(query)
        out_file.close()


def gen_cql_rpos_spos_queries():
    '''
    Generate RPOS and SPOS relations
    '''
    filename = 'rpos.cql'
    out_file = open(CQL_QUERY_DIR + os.sep + filename, 'w')
    out_file.write(CQL_RPOS_QUERY)
    out_file.close()
    filename = 'spos.cql'
    out_file = open(CQL_QUERY_DIR + os.sep + filename, 'w')
    out_file.write(CQL_SPOS_QUERY)
    out_file.close()


def gen_cql_w_queries(experiment_conf):
    '''
    Consider RANGE and SLIDE and generate W relation
    '''
    range_value = experiment_conf[RAN]
    slide_value = experiment_conf[SLI]
    # W: Calculate _POS and slide
    query = CQL_W_QUERY.format(ran=range_value, sli=slide_value)
    filename = CQL_QUERY_DIR + os.sep + 'w-' \
        + get_id(experiment_conf, [RAN, SLI]) + '.cql'
    if not os.path.isfile(filename):
        out_file = open(filename, 'w')
        out_file.write(query)
        out_file.close()


def gen_cql_position_queries():
    '''
    Generate queries to get each position
    '''
    # Generate W_1
    query = CQL_W1_QUERY
    filename = CQL_QUERY_DIR + os.sep + 'w1.cql'
    if not os.path.isfile(filename):
        out_file = open(filename, 'w')
        out_file.write(query)
        out_file.close()
    max_range = RANGE_LIST[-1]
    # W_i
    for range_value in range(2, max_range + 1):
        query = CQL_WI_QUERY.format(prev=range_value - 1)
        filename = CQL_QUERY_DIR + os.sep + 'w' + str(range_value) + '.cql'
        if not os.path.isfile(filename):
            out_file = open(filename, 'w')
            out_file.write(query)
            out_file.close()
    # P_n
    for range_value in range(1, max_range + 1):
        query = CQL_PI_QUERY.format(pos=range_value)
        filename = CQL_QUERY_DIR + os.sep + 'p' + str(range_value) + '.cql'
        if not os.path.isfile(filename):
            out_file = open(filename, 'w')
            out_file.write(query)
            out_file.close()


def gen_cql_final_query(experiment_conf):
    '''
    Generate final query equivalent to SEQ operator for a range parameter
    '''
    filename = CQL_QUERY_DIR + os.sep + 'final-' \
        + get_id(experiment_conf, [RAN]) + '.cql'
    if not os.path.isfile(filename):
        range_value = experiment_conf[RAN]
        pos_query_list = []
        for position in range(1, range_value + 1):
            pos_query = CQL_FINALPOS_QUERY.format(pos=position)
            pos_query_list.append(pos_query)
        query = '\nUNION\n'.join(pos_query_list) + ';'
        out_file = open(filename, 'w')
        out_file.write(query)
        out_file.close()


def gen_all_cql_queries(experiment_list):
    '''
    Generate all CQL queries equivalent to SEQ operator
    '''
    gen_cql_rpos_spos_queries()
    for exp_conf in experiment_list:
        gen_cql_position_queries()
        gen_cql_w_queries(exp_conf)
        gen_cql_final_query(exp_conf)


def gen_pref_env(experiment_conf):
    '''
    Generate environment files for SEQ operator
    '''
    # Environment files for SEQ operator
    filename = PREF_ENV_DIR + os.sep + get_id(experiment_conf) + '.env'
    if not os.path.isfile(filename):
        text = REG_STREAM_STR.format(d=DATA_FILE)
        text += '\n\n' + '#' * 80 + '\n\n'
        query_file = PREF_QUERY_DIR + os.sep \
            + get_id(experiment_conf, [RAN, SLI]) + '.cql'
        text += REG_Z_STR.format(q=query_file)
        out_file = open(filename, 'w')
        out_file.write(text)
        out_file.close()


def gen_cql_env(experiment_conf):
    '''
    Generate enviroNment files for StremPref
    '''
    # Environment files for SEQ operator
    text = REG_STREAM_STR.format(d=DATA_FILE)
    text += '\n\n' + '#' * 80 + '\n\n'
    # Environment files for equivalent CQL queries
    # RPOS
    text += REG_RPOS_SPO_STR
    qfile = CQL_QUERY_DIR + os.sep + 'w-' \
        + get_id(experiment_conf, [RAN, SLI]) + '.cql'
    # W
    text += REG_W_STR.format(q=qfile)
    # W1 and P1
    filename = CQL_QUERY_DIR + os.sep + 'w1.cql'
    text += REG_WI_STR.format(pos=1, q=filename)
    filename = CQL_QUERY_DIR + os.sep + 'p1.cql'
    text += REG_PI_STR.format(pos=1, q=filename)
    # W_n and P_n
    range_value = experiment_conf[RAN]
    for position in range(2, range_value + 1):
        filename = CQL_QUERY_DIR + os.sep + 'w' + str(position) + '.cql'
        text += REG_WI_STR.format(pos=position, q=filename)
        filename = CQL_QUERY_DIR + os.sep + 'p' + str(position) + '.cql'
        text += REG_PI_STR.format(pos=position, q=filename)
    # Final equivalent query
    filename = CQL_QUERY_DIR + os.sep + 'final-' \
        + get_id(experiment_conf, [RAN]) + '.cql'
    text += REG_CQL_FINAL_STR.format(q=filename)
    filename = CQL_ENV_DIR + os.sep + get_id(experiment_conf) + '.env'
    out_file = open(filename, 'w')
    out_file.write(text)
    out_file.close()


def get_detail_file(experiment_conf, algorithm, count):
    '''
    Return detail filename
    '''
    return DETAIL_DIR + os.sep + algorithm + '-' + get_id(experiment_conf) \
        + '-' + str(count) + '.csv'


def run(experiment_conf, algorithm, count, iterations):
    '''
    Run experiment for range and slide
    '''
    exp_id = get_id(experiment_conf)
    env_dir = PREF_ENV_DIR
    if algorithm == CQL_ALG:
        env_dir = CQL_ENV_DIR
    env_file = env_dir + os.sep + exp_id + '.env'
    detail_file = get_detail_file(experiment_conf, algorithm, count)
    if not os.path.isfile(detail_file):
        command = RUN_COMMAND.format(env=env_file, det=detail_file,
                                     ite=iterations)
        print command
        os.system(command)
        if not os.path.isfile(detail_file):
            print 'Detail results file not found: ' + detail_file
            print "Check if 'streampref' is in path"


def run_experiments(experiment_list):
    '''
    Run all experiments
    '''
    max_ts = get_max_timestamp(DATA_FILE, TRANSACTION_HEADER)
    for count in range(1, RUN_COUNT+1):
        for exp_conf in experiment_list:
            for alg in ALGORITHM_LIST:
                run(exp_conf, alg, count, max_ts)


def gen_data_files():
    '''
    Generate all files (queries and environments)
    '''
    # Copy imported data file and sort by timestamp
    os.system(SORT_COMMAND)
    if not os.path.isfile(DATA_FILE):
        print 'Error copying data file\n' + \
            'Make sure that import tool was executed'


def gen_all_env_files(experiment_list):
    '''
    Generate all environment files
    '''
    for exp_conf in experiment_list:
        gen_pref_env(exp_conf)
        gen_cql_env(exp_conf)


def gen_all_queries(experiment_list):
    '''
    Generate all queries
    '''
    for exp_conf in experiment_list:
        gen_endseq_query(exp_conf)
    gen_all_cql_queries(experiment_list)


def summarize_all():
    '''
    Summarize all results
    '''
    def_conf = {RAN: RANGE_DEFAULT, SLI: SLIDE_DEFAULT}
    # Variations
    var_dict = {RAN: RANGE_LIST, SLI: SLIDE_LIST}
    for par, par_list in var_dict.items():
        summarize(par, par_list, def_conf)


def summarize(parameter, value_list, default_values):
    '''
    Summarize experiments about range variation
    '''
    time_list = []
    mem_list = []
    exp_conf = default_values.copy()
    for value in value_list:
        exp_conf[parameter] = value
        for rcount in range(1, RUN_COUNT + 1):
            time_rec = {parameter: value}
            mem_rec = {parameter: value}
            for alg in ALGORITHM_LIST:
                dfile = get_detail_file(exp_conf, alg, rcount)
                runtime, memory = get_summaries(dfile)
                time_rec[alg] = runtime
                mem_rec[alg] = memory
            time_list.append(time_rec)
            mem_list.append(mem_rec)
    fname = SUMMARY_DIR + os.sep + 'runtime-' + parameter + '.csv'
    write_file(fname, time_list, parameter)
    fname = SUMMARY_DIR + os.sep + 'memory-' + parameter + '.csv'
    write_file(fname, mem_list, parameter)


def write_file(filename, record_list, key_field):
    '''
    Write record_list to file
    '''
    if len(record_list):
        field_list = [field for field in record_list[0].keys()
                      if field != key_field]
        field_list.sort()
        field_list.insert(0, key_field)
        output_file = open(filename, 'w')
        writer = csv.DictWriter(output_file, field_list)
        header = {field: field for field in field_list}
        writer.writerow(header)
        for rec in record_list:
            writer.writerow(rec)
        output_file.close()


def get_summaries(detail_file):
    '''
    Import a result file to database
    '''
    if not os.path.isfile(detail_file):
        print 'File does not exists: ' + detail_file
        return (float('NaN'), float('NaN'))
    in_file = open(detail_file, 'r')
    reader = csv.DictReader(in_file, skipinitialspace=True)
    sum_time = 0.0
    sum_memory = 0.0
    count = 0
    for rec in reader:
        sum_time += float(rec[RUNTIME])
        sum_memory += float(rec[MEMORY])
        count += 1
    in_file.close()
    return (sum_time, sum_memory / count)


def confidence_interval(parameter, in_file, out_file):
    '''
    Calculate final result with confidence interval
    '''
    if not os.path.isfile(in_file):
        print 'File does not exists: ' + in_file
        return
    command = CONFINTERVAL_COMMAND.format(inf=in_file, outf=out_file,
                                          keyf=parameter)
    print command
    os.system(command)
    if not os.path.isfile(out_file):
        print 'Output file not found: ' + out_file
        print "Check if 'confinterval.py' is in path"


def confidence_interval_all():
    '''
    Calculate confidence interval for all summarized results
    '''
    # Deletions and insertions
    for parameter in PARAMETER_LIST:
        in_file = SUMMARY_DIR + os.sep + 'runtime-' + parameter + '.csv'
        out_file = RESULT_DIR + os.sep + 'runtime-' + parameter + '.csv'
        confidence_interval(parameter, in_file, out_file)
        in_file = SUMMARY_DIR + os.sep + 'memory-' + parameter + '.csv'
        out_file = RESULT_DIR + os.sep + 'memory-' + parameter + '.csv'
        confidence_interval(parameter, in_file, out_file)


def get_arguments(print_help=False):
    '''
    Get arguments
    '''
    import argparse
    parser = argparse.ArgumentParser('Seq')
    parser.add_argument('-g', '--gen', action="store_true",
                        default=False,
                        help='Generate files')
    parser.add_argument('-r', '--run', action="store_true",
                        default=False,
                        help='Run experiments')
    parser.add_argument('-s', '--summarize', action="store_true",
                        default=False,
                        help='Summarize results')
    args = parser.parse_args()
    if print_help:
        parser.print_help()
    return args


def create_directories():
    '''
    Create default directories if they do not exists
    '''
    for directory in DIR_LIST:
        if not os.path.exists(directory):
            os.mkdir(directory)


def main():
    '''
    Main routine
    '''
    csv.register_dialect('table', delimiter='|', skipinitialspace=True)
    create_directories()
    exp_list = gen_experiment_list()
    args = get_arguments()
    if args.gen:
        print 'Generating data files'
        gen_data_files()
        print 'Generating queries'
        gen_all_queries(exp_list)
        print 'Generating environments'
        gen_all_env_files(exp_list)
    elif args.run:
        print 'Running experiments'
        run_experiments(exp_list)
    elif args.summarize:
        print 'Summarizing results'
        summarize_all()
        print 'Calculating confidence intervals'
        confidence_interval_all()
    else:
        get_arguments(True)


if __name__ == '__main__':
    main()
