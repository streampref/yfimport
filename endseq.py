#!/usr/bin/python -u
# -*- coding: utf-8 -*-
'''
Module for generation of environments for experiments over Yahoo finance data
using ENDSEQ operator
'''

import csv
import os

from yfimport import TRANSACTION_FILE, TRANSACTION_HEADER, get_max_timestamp


# =============================================================================
# Directories and filenames
# =============================================================================
MAIN_DIR = 'streampref_endseq'
DETAIL_DIR = MAIN_DIR + os.sep + 'details'
SUMMARY_DIR = MAIN_DIR + os.sep + 'summary'
RESULT_DIR = MAIN_DIR + os.sep + 'result'
ENDSEQ_QUERY_DIR = MAIN_DIR + os.sep + 'queries_endseq'
CQL_QUERY_DIR = MAIN_DIR + os.sep + 'queries_cql'
DATA_DIR = MAIN_DIR + os.sep + 'data'
ENDSEQ_ENV_DIR = MAIN_DIR + os.sep + 'env_endseq'
CQL_ENV_DIR = MAIN_DIR + os.sep + 'env_cql'
DIR_LIST = [MAIN_DIR, DETAIL_DIR, SUMMARY_DIR, RESULT_DIR, DATA_DIR,
            CQL_QUERY_DIR, ENDSEQ_QUERY_DIR, ENDSEQ_ENV_DIR, CQL_ENV_DIR]

DATA_FILE = DATA_DIR + os.sep + 's.csv'

# =============================================================================
# Experiment execution
# =============================================================================
# Number of experiment runs
RUN_COUNT = 5
# Command for experiment run
# CQL
CQL_RUN_COMMAND = \
    "streampref -r'|' -e {env} -d {det} -m {max}"
# ENDSEQ operator
ENDSEQ_RUN_COMMAND = \
    "streampref -r'|' -e {env} -d {det} -m {max} -s {alg}"
# Command for calculation of confidence interval
CONFINTERVAL_COMMAND = \
    "confinterval -i {inf} -o {outf} -k {keyf}"
# Command for sort stream file download
SORT_COMMAND = \
    'cat ' + TRANSACTION_FILE + ' | sort -g > ' + DATA_FILE

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
# Algorithms
# =============================================================================
# ENDSEQ operator (naive algorithm)
NAIVE_ALG = 'naive'
# ENDSEQ operator (incremental algorithm)
INCREMENTAL_ALG = 'incremental'
# Equivalence in CQL
CQL_ALG = 'cql'
# List of algorithms
ALGORITHM_LIST = [NAIVE_ALG, INCREMENTAL_ALG, CQL_ALG]

# =============================================================================
# Other parameters
# =============================================================================
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
# Queries with ENDSEQ operator
# =============================================================================

# TRANSACTION_ATT = [SYMBOL, SECTOR, COUNTRY, PRICE, VOLUME, METHOD, RATE]

# Queries
# Query with SEQ operator
Q_Z = '''
SELECT SUBSEQUENCE END POSITION
FROM
SEQUENCE IDENTIFIED BY symbol, method
[RANGE {ran} SECOND, SLIDE {sli} SECOND] FROM s;
'''

# =============================================================================
# CQL Equivalent Queries
# =============================================================================
# Query to get sequences from stream
CQL_Z_QUERY = '''
SELECT SEQUENCE IDENTIFIED BY symbol, method
[RANGE {ran} SECOND, SLIDE {sli} SECOND]
FROM s;
'''

# Query get ep-subsequences with length {ran}
CQL_POS_QUERY = '''
SELECT _pos - {ran} + 1 AS _pos, symbol, sector, country,
             price, volume, method, rate
FROM z WHERE _pos >= {ran}
'''

# =============================================================================
# Strings for registration in environment file
# =============================================================================
# Stream
REG_STREAM_STR = '''
REGISTER STREAM s(symbol STRING, sector STRING, country STRING,
             price FLOAT, volume INTEGER, method INTEGER, rate FLOAT)
INPUT '{d}';'''
# ENDSEQ
REG_ENDSEQ_STR = "REGISTER QUERY endseq \nINPUT '{q}';"
# CQL
# sequences from stream
REG_CQL_Z_STR = \
    "\n\nREGISTER QUERY z \nINPUT '{q}';"
# Equivalence for ep-subsequences
REG_CQL_FINAL_STR = "\n\nREGISTER QUERY equiv \nINPUT '{q}';"

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
    filename = ENDSEQ_QUERY_DIR + os.sep \
        + get_id(experiment_conf, [RAN, SLI]) + '.cql'
    if not os.path.isfile(filename):
        out_file = open(filename, 'w')
        query = Q_Z.format(ran=experiment_conf[RAN],
                                 sli=experiment_conf[SLI])
        out_file.write(query)
        out_file.close()


def gen_cql_z_query(experiment_conf):
    '''
    Generate query for sequence extraction
    '''
    filename = CQL_QUERY_DIR + os.sep \
        + 'z-' + get_id(experiment_conf, [RAN, SLI]) + '.cql'
    if os.path.isfile(filename):
        return
    out_file = open(filename, 'w')
    query = CQL_Z_QUERY.format(ran=experiment_conf[RAN],
                               sli=experiment_conf[SLI])
    out_file.write(query)
    out_file.close()


def gen_cql_final_query(experiment_conf):
    '''
    Generate final query equivalent to ENDSEQ operator
    '''
    filename = CQL_QUERY_DIR + os.sep + 'final-' \
        + get_id(experiment_conf, [RAN]) + '.cql'
    if os.path.isfile(filename):
        return
    range_value = experiment_conf[RAN]
    pos_query_list = []
    for position in range(1, range_value + 1):
        pos_query = CQL_POS_QUERY.format(ran=position)
        pos_query_list.append(pos_query)
    query = '\nUNION\n'.join(pos_query_list) + ';'
    out_file = open(filename, 'w')
    out_file.write(query)
    out_file.close()


def gen_all_cql_queries(experiment_conf):
    '''
    Generate all equivalent CQL queries
    '''
    gen_cql_z_query(experiment_conf)
    gen_cql_final_query(experiment_conf)


def gen_endseq_env(experiment_conf):
    '''
    Generate environment files for SEQ operator
    '''
    # Environment files for SEQ operator
    filename = ENDSEQ_ENV_DIR + os.sep + get_id(experiment_conf) + '.env'
    if not os.path.isfile(filename):
        text = REG_STREAM_STR.format(d=DATA_FILE)
        text += '\n\n' + '#' * 80 + '\n\n'
        query_file = ENDSEQ_QUERY_DIR + os.sep \
            + get_id(experiment_conf, [RAN, SLI]) + '.cql'
        text += REG_ENDSEQ_STR.format(q=query_file)
        out_file = open(filename, 'w')
        out_file.write(text)
        out_file.close()


def gen_cql_env(experiment_conf):
    '''
    Generate environment files for CQL queries
    '''
    filename = CQL_ENV_DIR + os.sep + get_id(experiment_conf) + '.env'
    if not os.path.isfile(filename):
        text = REG_STREAM_STR.format(d=DATA_FILE)
        text += '\n\n' + '#' * 80 + '\n\n'
        # Environment files for equivalent CQL queries
        # Sequences
        filename = CQL_QUERY_DIR + os.sep \
            + 'z-' + get_id(experiment_conf, [RAN, SLI]) + '.cql'
        text += REG_CQL_Z_STR.format(q=filename)
        # Final query
        qfile = CQL_QUERY_DIR + os.sep + 'final-' \
            + get_id(experiment_conf, [RAN]) + '.cql'
        text += REG_CQL_FINAL_STR.format(q=qfile)
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
    env_dir = ENDSEQ_ENV_DIR
    if algorithm == CQL_ALG:
        env_dir = CQL_ENV_DIR
    env_file = env_dir + os.sep + exp_id + '.env'
    detail_file = get_detail_file(experiment_conf, algorithm, count)
    if not os.path.isfile(detail_file):
        if algorithm == CQL_ALG:
            command = CQL_RUN_COMMAND.format(env=env_file, det=detail_file,
                                             max=iterations)
        else:
            command = ENDSEQ_RUN_COMMAND.format(env=env_file, det=detail_file,
                                                max=iterations,
                                                alg=algorithm)
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
        gen_endseq_env(exp_conf)
        gen_cql_env(exp_conf)


def gen_all_queries(experiment_list):
    '''
    Generate all queries
    '''
    for exp_conf in experiment_list:
        gen_endseq_query(exp_conf)
        gen_all_cql_queries(exp_conf)


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
