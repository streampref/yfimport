#!/usr/bin/python -u
# -*- coding: utf-8 -*-
'''
Module for generation of environments for experiments over Yahoo finance data
using CONSEQ operator
'''

import csv
import os

from yfimport import TRANSACTION_FILE, TRANSACTION_HEADER, get_max_timestamp


# =============================================================================
# Directories and filenames
# =============================================================================
MAIN_DIR = 'streampref_conseq'
DETAIL_DIR = MAIN_DIR + os.sep + 'details'
SUMMARY_DIR = MAIN_DIR + os.sep + 'summary'
RESULT_DIR = MAIN_DIR + os.sep + 'result'
CONSEQ_QUERY_DIR = MAIN_DIR + os.sep + 'queries_conseq'
CQL_QUERY_DIR = MAIN_DIR + os.sep + 'queries_cql'
DATA_DIR = MAIN_DIR + os.sep + 'data'
CONSEQ_ENV_DIR = MAIN_DIR + os.sep + 'env_conseq'
CQL_ENV_DIR = MAIN_DIR + os.sep + 'env_cql'
DIR_LIST = [MAIN_DIR, DETAIL_DIR, SUMMARY_DIR, RESULT_DIR, DATA_DIR,
            CQL_QUERY_DIR, CONSEQ_QUERY_DIR, CONSEQ_ENV_DIR, CQL_ENV_DIR]

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
# CONSEQ operator
CONSEQ_RUN_COMMAND = \
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
# CONSEQ operator (naive algorithm)
NAIVE_ALG = 'naive'
# CONSEQ operator (incremental algorithm)
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
# Queries with CONSEQ operator
# =============================================================================

# TRANSACTION_ATT = [SYMBOL, SECTOR, COUNTRY, PRICE, VOLUME, METHOD, RATE]

# Queries
# Query with SEQ operator
Q_Z = '''
SELECT SUBSEQUENCE CONSECUTIVE TIMESTAMP
FROM
SEQUENCE IDENTIFIED BY symbol, method
[RANGE {ran} SECOND, SLIDE {sli} SECOND] FROM s;
'''

# =============================================================================
# CQL Equivalent Queries
# =============================================================================
# Original timestamp of tuples in original TS attribute
TABLE_ORIGINAL_TS_QUERY = '''
SELECT _ts AS ots, * FROM s[NOW];
'''

# Transform table with original TS into stream
STREAM_ORIGINAL_TS_QUERY = '''
SELECT RSTREAM FROM table_ots;
'''

# Query to get sequence from stream with OTS
SEQ_ORIGINAL_TS_QUERY = '''
SELECT SEQUENCE IDENTIFIED BY symbol, method
[RANGE {ran} SECOND, SLIDE {sli} SECOND]
FROM stream_ots;
'''

# Query with consecutive positions of subsequences
CONSECUTIVE_POS_QUERY = '''
SELECT p.symbol, p.method, p._pos
FROM seq_ots AS p, seq_ots AS p_prev
WHERE p.symbol = p_prev.symbol
AND p.method = p_prev.method
AND p._pos = p_prev._pos + 1
AND p.ots = p_prev.ots + 1;
'''

# Query with first positions of subsequences
FIRST_POS_QUERY = '''
SELECT symbol, method, _pos
FROM seq_ots
EXCEPT
SELECT * FROM consecutive_pos;
'''

# Query with start position and all end positions following this start
START_END_FOLLOWING_QUERY = '''
SELECT s.symbol, s.method, s._pos AS start, e._pos - 1 AS end
FROM first_pos AS s, first_pos AS e
WHERE s.symbol = e.symbol
AND s.method = e.method
AND s._pos < e._pos
UNION
SELECT symbol, method, _pos AS start, CURRENT() + 1 AS end
FROM first_pos;
'''

# Query with start and end of each subsequence
START_END_QUERY = '''
SELECT * FROM start_end_following
EXCEPT
SELECT s.symbol, s.method, s.start, s.end
FROM start_end_following AS s, start_end_following AS s2
WHERE s.symbol = s2.symbol
AND s.method = s2.method
AND s.end > s2.end
AND s.start = s2.start;
'''

# Query equivalent to CONSEQ operator
CONSEQ_EQUIV_QUERY = '''
SELECT z._pos - se.start + 1 AS _pos,
       z.symbol, z.method, sector, country, price, volume, rate
FROM start_end AS se, seq_ots AS z
WHERE z.symbol = se.symbol
AND z.method = se.method
AND z._pos >= se.start
AND z._pos <= se.end;
'''

# =============================================================================
# Strings for registration in environment file
# =============================================================================
# Stream
REG_STREAM_STR = '''
REGISTER STREAM s(symbol STRING, sector STRING, country STRING,
             price FLOAT, volume INTEGER, method INTEGER, rate FLOAT)
INPUT '{d}';'''
# CONSEQ
REG_CONSEQ_STR = "REGISTER QUERY conseq \nINPUT '{q}';"
# CQL
# Table and stream with original ts
REG_ORIGINAL_TS_STR = \
    "\n\nREGISTER QUERY table_ots \nINPUT '" + \
    CQL_QUERY_DIR + "/table_ots.cql';" + \
    "\n\nREGISTER QUERY stream_ots \nINPUT '" + \
    CQL_QUERY_DIR + "/stream_ots.cql';"
# Sequences with original TS
REG_SEQ_ORIGINAL_TS_STR = "\n\nREGISTER QUERY seq_ots \nINPUT '{q}';"
# Consecutive positions
REG_CONSECUTIVE_POS_STR = "\n\nREGISTER QUERY consecutive_pos \nINPUT '{q}';"
# First position
REG_FIRST_POS_STR = "\n\nREGISTER QUERY first_pos \nINPUT '{q}';"
# Start, end and following
REG_START_END_FOLLOWING_STR = \
    "\n\nREGISTER QUERY start_end_following \nINPUT '{q}';"
# Start and end
REG_START_END_STR = "\n\nREGISTER QUERY start_end \nINPUT '{q}';"
# Final equivalence
REG_EQUIV_STR = "\n\nREGISTER QUERY equiv \nINPUT '{q}';"

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
    filename = CONSEQ_QUERY_DIR + os.sep \
        + get_id(experiment_conf, [RAN, SLI]) + '.cql'
    if not os.path.isfile(filename):
        out_file = open(filename, 'w')
        query = Q_Z.format(ran=experiment_conf[RAN],
                                 sli=experiment_conf[SLI])
        out_file.write(query)
        out_file.close()


def gen_common_cql_queries():
    '''
    Generate common CQL queries
    '''
    filename = CQL_QUERY_DIR + os.sep + 'table_ots.cql'
    if not os.path.isfile(filename):
        out_file = open(filename, 'w')
        out_file.write(TABLE_ORIGINAL_TS_QUERY)
        out_file.close()
    filename = CQL_QUERY_DIR + os.sep + 'stream_ots.cql'
    if not os.path.isfile(filename):
        out_file = open(filename, 'w')
        out_file.write(STREAM_ORIGINAL_TS_QUERY)
        out_file.close()
    filename = CQL_QUERY_DIR + os.sep + 'consecutive_pos.cql'
    if not os.path.isfile(filename):
        out_file = open(filename, 'w')
        out_file.write(CONSECUTIVE_POS_QUERY)
        out_file.close()
    filename = CQL_QUERY_DIR + os.sep + 'first_pos.cql'
    if not os.path.isfile(filename):
        out_file = open(filename, 'w')
        out_file.write(FIRST_POS_QUERY)
        out_file.close()
    filename = CQL_QUERY_DIR + os.sep + 'start_end_follow.cql'
    if not os.path.isfile(filename):
        out_file = open(filename, 'w')
        out_file.write(START_END_FOLLOWING_QUERY)
        out_file.close()
    filename = CQL_QUERY_DIR + os.sep + 'start_end.cql'
    if not os.path.isfile(filename):
        out_file = open(filename, 'w')
        out_file.write(START_END_QUERY)
        out_file.close()
    filename = CQL_QUERY_DIR + os.sep + 'equiv.cql'
    if not os.path.isfile(filename):
        out_file = open(filename, 'w')
        out_file.write(CONSEQ_EQUIV_QUERY)
        out_file.close()


def gen_seq_ots_query(experiment_conf):
    '''
    Generate query to select sequences with original timestamp as OTS attribute
    '''
    range_value = experiment_conf[RAN]
    slide_value = experiment_conf[SLI]
    query = SEQ_ORIGINAL_TS_QUERY.format(ran=range_value, sli=slide_value)
    filename = CQL_QUERY_DIR + os.sep + 'seq_ots-' \
        + get_id(experiment_conf, [RAN, SLI]) + '.cql'
    if not os.path.isfile(filename):
        out_file = open(filename, 'w')
        out_file.write(query)
        out_file.close()


def gen_all_cql_queries(experiment_list):
    '''
    Generate all CQL queries equivalent to SEQ operator
    '''
    gen_common_cql_queries()
    for exp_conf in experiment_list:
        gen_seq_ots_query(exp_conf)


def gen_conseq_env(experiment_conf):
    '''
    Generate environment files for SEQ operator
    '''
    # Environment files for SEQ operator
    filename = CONSEQ_ENV_DIR + os.sep + get_id(experiment_conf) + '.env'
    if not os.path.isfile(filename):
        text = REG_STREAM_STR.format(d=DATA_FILE)
        text += '\n\n' + '#' * 80 + '\n\n'
        query_file = CONSEQ_QUERY_DIR + os.sep \
            + get_id(experiment_conf, [RAN, SLI]) + '.cql'
        text += REG_CONSEQ_STR.format(q=query_file)
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
    # Original TS stream
    text += REG_ORIGINAL_TS_STR
    # Sequence with original TS
    qfile = CQL_QUERY_DIR + os.sep + 'seq_ots-' \
        + get_id(experiment_conf, [RAN, SLI]) + '.cql'
    text += REG_SEQ_ORIGINAL_TS_STR.format(q=qfile)
    qfile = CQL_QUERY_DIR + os.sep + 'consecutive_pos.cql'
    text += REG_CONSECUTIVE_POS_STR.format(q=qfile)
    qfile = CQL_QUERY_DIR + os.sep + 'first_pos.cql'
    text += REG_FIRST_POS_STR.format(q=qfile)
    qfile = CQL_QUERY_DIR + os.sep + 'start_end_follow.cql'
    text += REG_START_END_FOLLOWING_STR.format(q=qfile)
    qfile = CQL_QUERY_DIR + os.sep + 'start_end.cql'
    text += REG_START_END_STR.format(q=qfile)
    qfile = CQL_QUERY_DIR + os.sep + 'equiv.cql'
    text += REG_EQUIV_STR.format(q=qfile)
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
    env_dir = CONSEQ_ENV_DIR
    if algorithm == CQL_ALG:
        env_dir = CQL_ENV_DIR
    env_file = env_dir + os.sep + exp_id + '.env'
    detail_file = get_detail_file(experiment_conf, algorithm, count)
    if not os.path.isfile(detail_file):
        if algorithm == CQL_ALG:
            command = CQL_RUN_COMMAND.format(env=env_file, det=detail_file,
                                             max=iterations)
        else:
            command = CONSEQ_RUN_COMMAND.format(env=env_file, det=detail_file,
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
        gen_conseq_env(exp_conf)
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
