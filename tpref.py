#!/usr/bin/python -u
# -*- coding: utf-8 -*-
'''
Module for generation of environments for experiments over Yahoo finance data
using preference operators
'''

import csv
import os

from yfimport import TRANSACTION_FILE, TRANSACTION_HEADER, get_max_timestamp,\
    PRICE, RATE

# =============================================================================
# Directories and filenames
# =============================================================================
MAIN_DIR = 'streampref_tpref'
DETAIL_DIR = MAIN_DIR + os.sep + 'details'
SUMMARY_DIR = MAIN_DIR + os.sep + 'summary'
RESULT_DIR = MAIN_DIR + os.sep + 'result'
TPREF_QUERY_DIR = MAIN_DIR + os.sep + 'queries_tpref'
CQL_QUERY_DIR = MAIN_DIR + os.sep + 'queries_cql'
DATA_DIR = MAIN_DIR + os.sep + 'data'
TPREF_ENV_DIR = MAIN_DIR + os.sep + 'env_tpref'
CQL_ENV_DIR = MAIN_DIR + os.sep + 'env_cql'
DIR_LIST = [MAIN_DIR, DETAIL_DIR, SUMMARY_DIR, RESULT_DIR, DATA_DIR,
            CQL_QUERY_DIR, TPREF_QUERY_DIR, TPREF_ENV_DIR, CQL_ENV_DIR]

DATA_FILE = DATA_DIR + os.sep + 's.csv'
TUP_FILE = DATA_DIR + os.sep + 'tup.csv'

# =============================================================================
# Experiment execution
# =============================================================================
# Number of experiment runs
RUN_COUNT = 5
# Command for experiment run
TPREF_RUN_COMMAND = \
    "streampref -r'|' -e {env} -d {det} -m {ite} -t {alg}"
# Command for experiment run
CQL_RUN_COMMAND = "streampref -r'|' -e {env} -d {det} -m {ite}"
# Command for calculation of confidence interval
CONFINTERVAL_COMMAND = \
    "confinterval -i {inf} -o {outf} -k {keyf}"
# Command for sort stream file download
SORT_COMMAND = \
    "cat " + TRANSACTION_FILE + \
    " | cut -d'|' -f1,2,5,7,8 | sort -g > " + DATA_FILE

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
# Others
# =============================================================================
# Algorithms / equivalences
CQL = 'cql'
BNL_SEARCH = 'bnl_search'
INC_PARTITION_SEQTREE = 'inc_partition_seqtree'
INC_PARTITIONLIST_SEQTREE = 'inc_partitionlist_seqtree'
ALGORITHM_LIST = [CQL, BNL_SEARCH, INC_PARTITION_SEQTREE,
                  INC_PARTITIONLIST_SEQTREE]

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
# Queries with preference operators
# =============================================================================

# ATTRIBUTES: SYMBOL, METHOD, PRICE, RATE

# Queries
# Query with preference operator
PREF_QUERY = '''
SELECT SEQUENCE IDENTIFIED BY symbol, method
[RANGE {ran} SECOND, SLIDE {sli} SECOND]
FROM s
TEMPORAL PREFERENCES
IF ALL PREVIOUS (price >= 100)
    THEN (price < 100) BETTER (price >= 100) [rate]
AND IF PREVIOUS (price < 100)
    THEN (price >= 100) BETTER (price < 100) [rate]
AND (rate > 0.25) BETTER (rate <= 0.25);
'''

# =============================================================================
# Queries with CQL equivalence
# =============================================================================
# Query for sequence extraction
Q_Z = '''
SELECT SEQUENCE IDENTIFIED BY symbol, method
[RANGE {N} SECOND, SLIDE {L} SECOND]
FROM s;
'''

Q_DICT = {}
Q_ID_LIST = ['nv', 'mnv', 'r1', 'r2', 'r3', 'cp', 'ncp', 'p', 't', 'd1', 'd2',
             'd3', 't1', 't2', 't3', 'id', 'equiv']

# Queries equivalent to rule with ALL PREVIOUS condition
Q_DICT['nv'] = '''
SELECT symbol, method, _pos AS _pos FROM z
EXCEPT
SELECT symbol, method, _pos-1 AS _pos FROM z
UNION
SELECT symbol, method, _pos FROM z
WHERE NOT price >= 100;
'''

Q_DICT['mnv'] = '''
SELECT * FROM nv
EXCEPT
SELECT nv.* FROM nv AS nv, nv AS p
WHERE nv.symbol = p.symbol AND nv.method = p.method AND nv._pos > p._pos;
'''

Q_DICT['r1'] = '''
SELECT z.symbol, z.method, z._pos FROM z, mnv AS mv
WHERE z.symbol = mv.symbol AND z.method = mv.method
AND z._pos <= mv._pos AND z._pos > 1;
'''

# Query equivalent to rule with PREVIOUS condition
Q_DICT['r2'] = '''
SELECT z.symbol, z.method, z._pos FROM z, z AS zp
WHERE z.symbol = zp.symbol AND z.method = zp.method AND z._pos = zp._pos+1
AND zp.price < 100;
'''

# Query equivalent to rule with simple condition
Q_DICT['r3'] = '''
SELECT symbol, method, _pos FROM z;
'''

# Query to get correspondent positions
Q_DICT['cp'] = '''
SELECT z1.symbol AS x1, z1.method AS y1,
    z2.symbol AS x2, z2.method AS y2, z1._pos
FROM z AS z1, z AS z2
WHERE z1._pos =  z2._pos
AND z1.price = z2.price
AND z1.rate = z2.rate
AND z1.symbol <= z2.symbol
UNION
SELECT z1.symbol AS x1, z1.method AS y1,
    z2.symbol AS x2, z2.method AS y2, z1._pos
FROM z AS z1, z AS z2
WHERE z1._pos =  z2._pos
AND z1.price = z2.price
AND z1.rate = z2.rate
AND z1.method <= z2.method;
'''

# Non correspondent positions
Q_DICT['ncp'] = '''
SELECT z1.symbol AS x1, z1.method AS y1,
    z2.symbol AS x2, z2.method AS y2, z1._pos
FROM z AS z1, z AS z2
WHERE z1._pos =  z2._pos AND z1.symbol <= z2.symbol
UNION
SELECT z1.symbol AS x1, z1.method AS y1,
    z2.symbol AS x2, z2.method AS y2, z1._pos
FROM z AS z1, z AS z2
WHERE z1._pos =  z2._pos AND z1.method <= z2.method
EXCEPT
SELECT * FROM cp;
'''

# Smaller non correspondent position (positions to be compared)
Q_DICT['p'] = '''
SELECT * FROM ncp
EXCEPT
SELECT ncp.x1, ncp.y1, ncp.x2, ncp.y2, ncp._pos FROM ncp, ncp AS p
WHERE ncp.x1 = p.x1 AND ncp.y1 = p.y1
AND ncp.x2 = p.x2 AND ncp.y2 = p.y2 AND ncp._pos > p._pos;
'''

# All tuples: sequence tuples (z) and transitive tuples (tup)
Q_DICT['t'] = '''
SELECT p.x1, p.y1, p.x2, p.y2, p._pos, z.price, z.rate, 1 AS t
FROM p, z
WHERE p._pos = z._pos AND p.x1 = z.symbol AND p.y1 = z.method
UNION
SELECT p.x1, p.y1, p.x2, p.y2, p._pos, z.price, z.rate, 1 AS t
FROM p, z
WHERE p._pos = z._pos AND p.x2 = z.symbol AND p.y2 = z.method
UNION
SELECT p.x1, p.y1, p.x2, p.y2, p._pos, tup.price, tup.rate, 0 AS t
FROM p, tup, z
WHERE p._pos = z._pos AND p.x1 = z.symbol AND p.y1 = z.method
;
'''

# Direct comparisons
Q_DICT['d1'] = '''
SELECT DISTINCT b._pos, b.x1, b.y1, b.x2, b.y2, b.price, b.rate, b.t,
    w.price AS wprice, w.rate as wrate, w.t AS wt
FROM t AS b, t AS w, r1 AS br, r1 AS wr
WHERE b.x1 = w.x1 AND b.y1 = w.y1
AND b.x2 = w.x2 AND b.y2 = w.y2
AND b._pos = w._pos AND b._pos = br._pos AND w._pos = wr._pos
AND b.x1 = br.symbol AND b.y1 = br.method
AND w.x2 = wr.symbol AND w.y2 = wr.method
AND b.price < 100 AND w.price >= 100
;
'''

Q_DICT['d2'] = '''
SELECT DISTINCT b._pos, b.x1, b.y1, b.x2, b.y2, b.price, b.rate, b.t,
    w.price AS wprice, w.rate as wrate, w.t AS wt
FROM t AS b, t AS w, r2 AS br, r2 AS wr
WHERE b.x1 = w.x1 AND b.y1 = w.y1
AND b.x2 = w.x2 AND b.y2 = w.y2
AND b._pos = w._pos AND b._pos = br._pos AND w._pos = wr._pos
AND b.x1 = br.symbol AND b.y1 = br.method
AND w.x2 = wr.symbol AND w.y2 = wr.method
AND b.price >= 100 AND w.price < 100
;
'''

Q_DICT['d3'] = '''
SELECT DISTINCT b._pos, b.x1, b.y1, b.x2, b.y2, b.price, b.rate, b.t,
    w.price AS wprice, w.rate as wrate, w.t AS wt
FROM t AS b, t AS w, r3 AS br, r3 AS wr
WHERE b.x1 = w.x1 AND b.y1 = w.y1
AND b.x2 = w.x2 AND b.y2 = w.y2
AND b._pos = w._pos AND b._pos = br._pos AND w._pos = wr._pos
AND b.x1 = br.symbol AND b.y1 = br.method
AND w.x2 = wr.symbol AND w.y2 = wr.method
AND b.rate > 0.25 AND w.rate <= 0.25
AND b.price = w.price
;
'''

Q_DICT['t1'] = '''
SELECT * FROM D1
UNION
SELECT * FROM D2
UNION
SELECT * FROM D3
;
'''

Q_DICT['t2'] = '''
SELECT b._pos, b.x1, b.y1, b.x2, b.y2, b.price, b.rate, b.t,
    w.wprice, b.wrate, w.wt
FROM t1 AS b, t1 AS w
WHERE b.x1 = w.x1 AND b.y1 = w.y1 AND b.x2 = w.x2 AND b.y2 = w.y2
AND b.wprice = w.price and b.wrate = w.rate
UNION
SELECT * FROM t1;
'''

Q_DICT['t3'] = '''
SELECT b._pos, b.x1, b.y1, b.x2, b.y2, b.price, b.rate, b.t,
    w.wprice, b.wrate, w.wt
FROM t2 AS b, t2 AS w
WHERE b.x1 = w.x1 AND b.y1 = w.y1 AND b.x2 = w.x2 AND b.y2 = w.y2
AND b.wprice = w.price and b.wrate = w.rate
UNION
SELECT * FROM t2;
'''

# Identifiers of dominant sequences
Q_DICT['id'] = '''
SELECT DISTINCT symbol, method FROM z
EXCEPT
SELECT DISTINCT x2 AS symbol, y2 AS method FROM t3
WHERE t = 1 AND wt = 1;
'''

# Dominant sequences
Q_DICT['equiv'] = '''
SELECT z.* FROM z, id
WHERE z.symbol = id.symbol
AND z.method = id.method;
'''

# =============================================================================
# Strings for registration in environment file
# =============================================================================
# Input stream
REG_STREAM_STR = '''
REGISTER STREAM s(symbol STRING, price FLOAT, method INTEGER, rate FLOAT)
INPUT '{dfile}';'''
# Preference query
REG_PREF_STR = "\n\nREGISTER QUERY pref \nINPUT '{qfile}';"
# CQL
# Transitive tuples
REG_TUP_STR = "\nREGISTER TABLE tup (price FLOAT, rate FLOAT) \nINPUT '" + \
    TUP_FILE + "';"
# Sequences
REG_Z_STR = "\n\nREGISTER QUERY z \nINPUT '{qfile}';"
# Positions to be compared
REG_Q_STR = "\n\nREGISTER QUERY {qname} \nINPUT '{qfile}';"
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


def get_experiment_id(experiment_conf):
    '''
    Return a query ID for given parameters
    '''
    return RAN + str(experiment_conf[RAN]) + \
        SLI + str(experiment_conf[SLI])


def gen_pref_query(experiment_conf):
    '''
    Generate StreamPref queries with BESTSEQ operator
    '''
    query_id = get_experiment_id(experiment_conf)
    filename = TPREF_QUERY_DIR + os.sep + query_id + '.cql'
    query = PREF_QUERY.format(ran=experiment_conf[RAN],
                              sli=experiment_conf[SLI])
    out_file = open(filename, 'w')
    out_file.write(query)
    out_file.close()


def gen_transitive_tup():
    '''
    Generate tuples for equivalence of transitive closure
    '''
    in_file = open(DATA_FILE, 'r')
    reader = csv.DictReader(in_file, skipinitialspace=True, dialect='table')
    price_set = set([100])
    rate_set = set([0.25])
    # Read all values of price and rate of input data
    for rec in reader:
        price_set.add(rec[PRICE])
        rate_set.add(rec[RATE])
    rec_list = []
    # Cartesian product between price and rate
    for price in price_set:
        for rate in rate_set:
            rec = {TS_ATT: 0, FL_ATT: '+', PRICE: price, RATE: rate}
            rec_list.append(rec)
    att_name_list = [TS_ATT, FL_ATT, PRICE, RATE]
    # Store records on file
    out_file = open(TUP_FILE, 'w')
    out_write = csv.DictWriter(out_file, att_name_list, dialect='table')
    out_write.writeheader()
    out_write.writerows(rec_list)
    out_file.close()


def gen_cql_queries(experiment_conf):  # IGNORE:too-many-statements
    '''
    Generate queries with CQL original operators equivalent to BESTSEQ operator
    '''
    # Generate query z (sequences)
    query = Q_Z.format(N=experiment_conf[RAN],
                       L=experiment_conf[SLI])
    out_file = open(CQL_QUERY_DIR + os.sep + 'z-' +
                    get_experiment_id(experiment_conf) + '.cql', 'w')
    out_file.write(query)
    out_file.close()
    # Remaining queries
    for query_id in Q_ID_LIST:
        filename = CQL_QUERY_DIR + os.sep + query_id + '.cql'
        query = Q_DICT[query_id]
        out_file = open(filename, 'w')
        out_file.write(query)
        out_file.close()


def gen_pref_env(experiment_conf):
    '''
    Generate environment files for SEQ operator
    '''
    # environment files for SEQ operator
    filename = TPREF_ENV_DIR + os.sep + get_id(experiment_conf) + '.env'
    if not os.path.isfile(filename):
        text = REG_STREAM_STR.format(dfile=DATA_FILE)
        text += '\n\n' + '#' * 80 + '\n\n'
        query_file = TPREF_QUERY_DIR + os.sep \
            + get_id(experiment_conf, [RAN, SLI]) + '.cql'
        text += REG_Z_STR.format(qfile=query_file)
        out_file = open(filename, 'w')
        out_file.write(text)
        out_file.close()


def gen_cql_env(experiment_conf):
    '''
    Generate environment files for StremPref
    '''
    # Input stream
    text = REG_STREAM_STR.format(dfile=DATA_FILE)
    text += '\n\n' + '#' * 80 + '\n\n'
    # Trasitive tuples
    text += REG_TUP_STR
    # Sequences
    filename = CQL_QUERY_DIR + os.sep + 'z-' + \
        get_experiment_id(experiment_conf) + '.cql'
    text += REG_Z_STR.format(qfile=filename)
    # Remaining queries
    for query_id in Q_ID_LIST:
        filename = CQL_QUERY_DIR + os.sep + query_id + '.cql'
        text += REG_Q_STR.format(qname=query_id, qfile=filename)
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
    env_dir = TPREF_ENV_DIR
    if algorithm == CQL:
        env_dir = CQL_ENV_DIR
    env_file = env_dir + os.sep + exp_id + '.env'
    detail_file = get_detail_file(experiment_conf, algorithm, count)
    if not os.path.isfile(detail_file):
        if algorithm == CQL:
            command = CQL_RUN_COMMAND.format(env=env_file, det=detail_file,
                                             ite=iterations)
        else:
            command = TPREF_RUN_COMMAND.format(env=env_file, det=detail_file,
                                               ite=iterations, alg=algorithm)
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
    Generate all query files
    '''
    # Set of queries
    generated_set = set()
    # For every experiment configuration
    for exp in experiment_list:
        exp_id = get_experiment_id(exp)
        if exp_id not in generated_set:
            generated_set.add(exp_id)
            # Generate queries
            gen_cql_queries(exp)
            gen_pref_query(exp)


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
        gen_transitive_tup()
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
