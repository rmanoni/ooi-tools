#!/usr/bin/env python

import glob
import logging
import shutil
import pprint
import urllib2
import time
import uuid
import yaml
import json
import sys
import os
import qpid.messaging as qm

all_data_url = 'http://localhost:12570/sensor/user/inv/null/null'

edex_dir = os.getenv('EDEX_HOME')
if edex_dir is None:
    edex_dir = os.path.join(os.getenv('HOME'), 'uframes', 'ooi', 'uframe-1.0', 'edex')
startdir = os.path.join(edex_dir, 'data/utility/edex_static/base/ooi/parsers/mi-dataset/mi')
drivers_dir = os.path.join(startdir, 'dataset/driver')
ingest_dir = os.path.join(edex_dir, 'data', 'ooi')
log_dir = os.path.join(edex_dir, 'logs')

USER = 'guest'
HOST = 'localhost'
PORT = 5672
PURGE_MESSAGE = qm.Message(content='PURGE_ALL_DATA', content_type='text/plain', user_id=USER)


class FAILURES:
    MISSING_SAMPLE = 'MISSING_SAMPLE'
    MISSING_FIELD = 'MISSING_FIELD'
    BAD_VALUE = 'BAD_VALUE'
    UNEXPECTED_VALUE = 'UNEXPECTED_VALUE'
    AMBIGUOUS = 'AMBIGUOUS'

    def __init__(self):
        pass


def get_logger():
    logger = logging.getLogger('dataset_test')
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)-7s %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)
    return logger


log = get_logger()


class TestCase(object):
    def __init__(self, config):
        self.config = config
        self.instrument = config.get('stream_name')
        self.resource = os.path.join(drivers_dir, config.get('resource'))
        self.endpoint = os.path.join(ingest_dir, config.get('endpoint'))
        self.pairs = config.get('pairs', [])

    def __str__(self):
        return pprint.pformat(self.config)

    def __repr__(self):
        return self.config.__repr__()


def read_test_cases(f):
    log.info('Finding test cases in: %s', f)
    if os.path.isdir(f):
        for filename in os.listdir(f):
            config = yaml.load(open(os.path.join(f, filename), 'r'))
            yield TestCase(config)
    elif os.path.isfile(f):
        config = yaml.load(open(f, 'r'))
        yield TestCase(config)


def get_from_edex():
    """
    Retrieve all stored sensor data from edex
    :return: list of edex records
    """
    proxy_handler = urllib2.ProxyHandler({})
    opener = urllib2.build_opener(proxy_handler)
    req = urllib2.Request(all_data_url)
    r = opener.open(req)
    records = json.loads(r.read())
    log.debug('RETRIEVED:')
    log.debug(pprint.pformat(records, depth=3))
    d = {}
    for record in records:
        timestamp = record.get('internal_timestamp')
        stream_name = record.get('stream_name')
        key = (timestamp, stream_name)
        if key in d:
            log.error('Duplicate record found in retrieved values %s', key)
        else:
            d[key] = record
    return d


def compare(stored, expected):
    """
    Compares a set of expected results against the retrieved values
    :param stored:
    :param expected:
    :return: list of failures
    """

    #TODO - NEED TO UPDATE FOR INSTRUMENT
    failures = []
    for record in expected:
        timestamp = record.get('internal_timestamp')
        stream_name = record.get('particle_type')
        # Not all YAML files contain the particle type
        # if we don't find it, let's check the stored data
        # if all particles are the same type, then we'll proceed
        if stream_name is None:
            log.warn('Missing stream name from YML file, attempting to infer')
            keys = stored.keys()
            keys = [x[1] for x in keys]
            keys = set(keys)
            if len(keys) == 1:
                key = (timestamp, keys.pop())
            else:
                failures.append((FAILURES.AMBIGUOUS, 'Multiple streams in output, no stream in YML'))
                log.error('Ambiguous stream information in YML file and unable to infer')
                continue
        else:
            key = (timestamp, stream_name)
        if key not in stored:
            failures.append((FAILURES.MISSING_SAMPLE, key))
            log.error('No matching record found in retrieved data for key %s', key)
        else:
            f = diff(record, stored.get(key))
            if f:
                failures.append(f)
    return failures


def diff(a, b, ignore=None, rename=None):
    """
    Compare two data records
    :param a:
    :param b:
    :param ignore: fields to ignore
    :param rename: fields to rename before comparison
    :return: list of failures
    """
    if ignore is None:
        ignore = ['particle_object', 'quality_flag', 'driver_timestamp', 'stream_name', 'preferred_timestamp']
    if rename is None:
        rename = {'particle_type': 'stream_name'}

    failures = []

    # verify from expected to retrieved
    for k, v in a.iteritems():
        if k in ignore or k.startswith('_'):
            continue
        if k in rename:
            k = rename[k]
        if k not in b:
            failures.append((FAILURES.MISSING_FIELD, k))
            log.error('missing key: %s in retrieved record', k)
            continue
        if type(v) == dict:
            _round = v.get('round')
            value = v.get('value')
            rvalue = round(b[k], _round)
        else:
            value = v
            rvalue = b[k]
        if value != rvalue:
            failures.append((FAILURES.BAD_VALUE, 'expected=%s retrieved=%s' % (v, b[k])))
            log.error('non-matching value: expected=%s retrieved=%s', v, b[k])

    # verify no extra (unexpected) keys present in retrieved data
    for k in b:
        if k in ignore:
            continue
        if k not in a:
            failures.append((FAILURES.UNEXPECTED_VALUE, k))
            log.error('item in retrieved data not in expected data: %s', k)

    return failures


def purge_edex(logfile=None):
    log.info('Purging edex')
    conn = qm.Connection(host=HOST, port=PORT, username=USER, password=USER)
    conn.open()
    conn.session().sender('purgeRequest').send(PURGE_MESSAGE)
    conn.close()
    watch_log_for('Purge Operation: PURGE_ALL_DATA completed', logfile=logfile)


def copy_file(resource, endpoint, test_file):
    log.info('copy test file %s into endpoint %s from %s', test_file, endpoint, resource)
    source_file = os.path.join(drivers_dir, resource, test_file)
    destination_file = os.path.join(ingest_dir, endpoint, str(uuid.uuid4()))
    shutil.copy(source_file, destination_file)


def find_latest_log():
    todayglob = time.strftime('edex-ooi-%Y%m%d.log*', time.localtime())
    files = glob.glob(os.path.join(log_dir, todayglob))
    files = [(os.stat(f).st_mtime, f) for f in files if not f.endswith('lck')]
    files.sort()
    fh = open(files[-1][1], 'r')
    fh.seek(0, 2)
    return fh


def watch_log_for(expected_string, logfile=None, expected_count=1, timeout=60):
    if logfile is None:
        logfile = find_latest_log()
    log.debug('waiting for %s in logfile: %s', expected_string, logfile.name)

    endtime = time.time() + timeout
    count = 0
    while time.time() < endtime:
        data = logfile.read()
        for line in data.split('\n'):
            if expected_string in line:
                count += 1
                log.debug('Found expected string %d times of %d', count, expected_count)
                if count == expected_count:
                    return
        time.sleep(.1)
    raise Exception('timeout waiting for log output')


def wait_for_ingest_complete():
    watch_log_for('Ingest: EDEX: Ingest')


def test_results(expected):
    retrieved = get_from_edex()
    log.debug('Retrieved %d records from edex:', len(retrieved))
    log.debug(pprint.pformat(retrieved, depth=3))
    log.debug('Retrieved %d records from expected data file:', len(expected))
    log.debug(pprint.pformat(expected, depth=3))
    failures = compare(retrieved, expected)
    return len(retrieved), len(expected), failures

def get_expected(filename):
    """
    Loads expected results from the supplied YAML file
    :param filename:
    :return: list of records containing the expected results
    """
    return_data = []

    for each in open(filename, 'r').read().split('\n\n'):
        if each is not None:
            print each
            data = yaml.load(each)
            log.debug('Raw data from YAML: %s', data)

            stream_name = data.get('stream_name')
            log.debug('%s', stream_name)
            timestamp = data.get('port_timestamp')
            log.debug('%s', timestamp)
            values = data.get('values')
            log.debug('%s', values)

            timestamp = data.get('internal_timestamp')
            if type(timestamp) == str:
                timestamp, millis = timestamp.split('.')
                timestamp = time.mktime(time.strptime(timestamp + 'GMT', '%Y-%m-%dT%H:%M:%S%Z'))
                if millis.endswith('Z'):
                    millis = millis[:-1]
                divisor = 10 ** len(millis)
                millis = float(millis)
                timestamp = timestamp + millis / divisor + 2208988800l - time.timezone
                each['internal_timestamp'] = timestamp
            return_data.append(each)

    return return_data


def test(file_name):

    scorecard = {}
    log.debug('Processing test case: %s', file_name)
    expected = get_expected(file_name)
    time.sleep(1)
    # #TODO HOW TO GET INSTRUMENT NAME??? (PARTICLE NAME)
    # #NEED TO HANDLE DIFF STREAMS FROM DIFF DRIVERS
    # scorecard[instrument] = test_results(expected)
    #
    # print('\n------------------------------------------------TEST RESULTS------------------------------------------------')
    #
    # total_instrument_count = 0
    # total_yaml_count = 0
    # total_edex_count = 0
    # total_pass_count = 0
    # total_fail_count = 0
    # table_data = [['Instrument','YAML_count','EDEX_count','Pass','Fail']]
    #
    # for instrument in scorecard:
    #
    #     edex_count, yaml_count, fail_count = scorecard[instrument]
    #     pass_count = yaml_count - len(fail_count)
    #     table_data.append([instrument,yaml_count,edex_count,pass_count,len(fail_count)])
    #
    #     total_yaml_count += yaml_count
    #     total_edex_count += edex_count
    #     total_pass_count += pass_count
    #     total_fail_count += len(fail_count)
    #     total_instrument_count += 1
    #
    # for row in table_data:
    #     print("{: >40} {: >15} {: >15} {: >15} {: >15}".format(*row))
    #
    # print("\n------------------------------------------------------------------------------------------------------------\n")
    # row = ['Total Instrument', 'Total YAML', 'Total EDEX', 'Total Pass', 'Total Fail']
    # print("{: >40} {: >15} {: >15} {: >15} {: >15}".format(*row))
    #
    # row = [total_instrument_count, total_yaml_count, total_edex_count, total_pass_count, total_fail_count]
    # print("{: >40} {: >15} {: >15} {: >15} {: >15}".format(*row))
    # print("\n")


if __name__ == '__main__':
    test_cases = []
    if len(sys.argv) <= 1:
        print('useage: ./validate_instrument [file1] [file2] ...')
    else:
        for each in sys.argv[1:]:
            test(each)

