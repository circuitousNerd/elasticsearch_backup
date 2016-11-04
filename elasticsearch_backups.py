#!/usr/bin/env python3

def fetch_all_snapshots(elastic_node, backup_repository, port):
    import json, logging, requests

    # Assemble URL
    url = 'http://{0}:{1}/_snapshot/{2}/_all'.format(elastic_node, port, backup_repository)

    try:
        #Send REST API call
        request = requests.get(url)
        request.raise_for_status()
        logging.info('fetch_all_snapshots: retrieved all snapshots metadata')
        return json.loads(request.text)
    except ConnectionError as e:
        logging.error("fetch_all_snapshots: Failed to create {0}".format(e))
        logging.error(json.loads(request.text))
    except requests.exceptions.HTTPError as e:
        logging.error("fetch_all_snapshots: Failed to create {0}".format(e))
        logging.error(json.loads(request.text)['error']['reason'])
    except requests.exceptions.ConnectionError as e:
        logging.error("fetch_all_snapshots: Failed to create {0}".format(e))

def find_old_snapshots(all_snapshots, snapshot_time_delta):
    import datetime, logging

    old_snapshots = []

    for snapshot in all_snapshots['snapshots']:
        snapshot_date = parse_snapshot_name(snapshot['snapshot'])
        logging.info('find_old_snapshots: snapshot name: {0}: snapshot date: {1}\
        '.format(snapshot['snapshot'], snapshot_date))
        if snapshot_date <= snapshot_time_delta:
            old_snapshots.append(snapshot['snapshot'])
            logging.info('find_old_snapshots: {0}'.format(old_snapshots))
    return old_snapshots

def bulk_delete(snapshot_time_delta, elastic_node, backup_repository, port):
    import logging, requests

    # Fetch all snapshot metadata
    all_snapshots = fetch_all_snapshots(elastic_node, backup_repository, port)
    # Find snapshots older than snapshot_time_delta
    old_snapshots = find_old_snapshots(all_snapshots, snapshot_time_delta)

    for snapshot in old_snapshots:
        # Assemble url
        url = 'http://{0}:{1}/_snapshot/{2}/{3}'.format(elastic_node, port,
        backup_repository, snapshot)
        logging.debug('bulk_delete:  URL is: {0}'.format(url))
        # Send REST API call
        try:
            request = requests.delete(url)
            request.raise_for_status()
            logging.info('bulk_delete: {0} completed'.format(snapshot))
        except ConnectionError as e:
            logging.error("bulk_delete: Failed to create {0}".format(e))
            logging.error(json.loads(request.text))
        except requests.exceptions.HTTPError as e:
            logging.error("bulk_delete: Failed to create {0}".format(e))
            logging.error(json.loads(request.text)['error']['reason'])
        except requests.exceptions.ConnectionError as e:
            logging.error("bulk_delete: Failed to create {0}".format(e))

def parse_snapshot_name(snapshot_name=None):
    import logging, re
    from datetime import datetime

    try:
        if snapshot_name is None:
            raise ValueError('snapshot_name cannot be None')
        else:
            # Pull datetime out of snapshot_name
            search_result = re.search(r'\d{8}', snapshot_name)
            logging.info('parse_snapshot_name: Parsed date is: \
            {0}'.format(search_result.group()))

            #Convert regex result into datetime
            date = datetime.strptime(search_result.group(), '%Y%m%d')
            logging.info('parse_snapshot_name: Confirm parsed date converted \
            to date: {0}'.format(type(date)))
            return date
    except ValueError as e:
        logging.error('parse_snapshot_name: {0}'.format(e))

def calculate_delta(snapshot_name=None, age=40):
    import logging, re
    from datetime import timedelta

    try:
        if snapshot_name == None:
            raise ValueError('snapshot_name cannot be None')
        else:
            date = parse_snapshot_name(snapshot_name)

            # Calculate time delta
            date_delta = date - timedelta(days=age)
            logging.info('calculate_delta: date {0} days ago is {1} \
            '.format(age, date_delta))
            return date_delta
    except ValueError as e:
        logging.error('calculate_delta: {0}}'.format(e))

def generate_snapshot_name(prefix='snapshot-'):
    from datetime import datetime

    # generate date string in UTC time
    date = datetime.utcnow().strftime("%Y%m%d")
    snapshot_name = prefix + date
    return snapshot_name

def find_config(config_arg):
    import yaml
    # Check if custom config defined
    if config_arg == '' or config_arg == None:
        configfile = '/etc/elasticsearch/backup.yaml'
    else:
        configfile = config_arg

    # Read in config from config file
    try:
        with open(configfile, 'r') as ymlfile:
            config = yaml.load(ymlfile)
            return(config)
    except FileNotFoundError as e:
        print(e)
        exit()

def backup(elastic_node, backup_repository, port=9200, snapshot_name=None):
    import json, logging, requests

    # Get new snapshot name if none provided
    if snapshot_name == None:
        snapshot_name = generate_snapshot_name()

    # Assemble url
    snapshot_url = 'http://{0}:{1}/_snapshot/{2}/{3}?wait_for_completion=true \
    '.format(elastic_node, port, backup_repository, snapshot_name)

    # Send REST API call
    try:
        request = requests.put(snapshot_url)
        request.raise_for_status()
        logging.info('backup: {0} completed'.format(snapshot_name))
    except ConnectionError as e:
        logging.error("backup: Failed to create {0}".format(e))
        logging.error(json.loads(request.text))
    except requests.exceptions.HTTPError as e:
        logging.error("backup: Failed to create {0}".format(e))
        logging.error(json.loads(request.text)['error']['reason'])
    except requests.exceptions.ConnectionError as e:
        logging.error("backup: Failed to create {0}".format(e))

def delete(age, elastic_node, backup_repository, port=9200, snapshot_name=None):
    import logging, json, requests

    # If age not provided, use default
    if age == None:
        age = 40

    # If snapshot_name provided delete it
    if snapshot_name is not None:
        url = 'http://{0}:{1}/_snapshot/{2}/{3}'.format(elastic_node, port,
        backup_repository, snapshot_name)
        # Send REST API call
        try:
            request = requests.delete(url)
            request.raise_for_status()
            logging.info('delete: {0} completed'.format(snapshot_name))
        except ConnectionError as e:
            logging.error("delete: Failed to create {0}".format(e))
            logging.error(json.loads(request.text))
        except requests.exceptions.HTTPError as e:
            logging.error("delete: Failed to create {0}".format(e))
            logging.error(json.loads(request.text)['error']['reason'])
        except requests.exceptions.ConnectionError as e:
            logging.error("delete: Failed to create {0}".format(e))
    # Get today's snapshot name if none provided
    else:
        snapshot_name = generate_snapshot_name()
        logging.info('delete: Generated snapshot name \
        {0}'.format(snapshot_name))
        snapshot_time_delta = calculate_delta(snapshot_name, age=age)
        bulk_delete(snapshot_time_delta, elastic_node, backup_repository,
        port)

def main():
    import argparse, logging

    # Parse arguments
    parser = argparse.ArgumentParser(description='Take subcommands and \
    parameters for elasticsearch backup script.')
    parser.add_argument('function', type=str, choices=['backup','delete'],
    help='Triggers a new elasticsearch action.')
    parser.add_argument('--config', type=str,
    help='Specify path to a config file.')
    parser.add_argument('--name', type=str, help='Specify snapshot name')
    parser.add_argument('--age', type=int,
    help='Specify age to delete backups after')
    parser.add_argument('--logfile', type=str,
    help='Specify where to put a logfile')
    parser.add_argument('--loglevel', type=str, help='Specify log level',
    choices=['INFO', 'DEBUG', 'WARNING', 'ERROR', 'CRITICAL'])

    args = parser.parse_args()

    # Find config file, and read in config
    config = find_config(args.config)

    # Check if logfile was specified
    if args.logfile == None:
        logfile = '/var/log/elasticsearch/snapshot_backup.log'
    else:
        logfile = args.logfile

    # Check if logging level was specified
    try:
        if args.loglevel != None:
            logging_level = logging.getLevelName(args.loglevel)
            print('I am args: {0}'.format(logging_level))
        elif config['logging_level'] != None:
            logging_level = logging.getLevelName(config['logging_level'])
            print('I am config: {0}'.format(logging_level))
    except KeyError as e:
        logging_level = logging.getLevelName('ERROR')

    # Set up logging
    logging.basicConfig(filename=logfile, level=logging_level,
    format='%(asctime)s - %(levelname)s - %(message)s')

    # Map argument string to function name
    FUNCTION_MAP = {'backup': backup, 'delete': delete}

    # Initialise callable variable using the FUNCTION_MAP
    func = FUNCTION_MAP[args.function]

    # Call funky voodoo variable function
    if args.function == 'backup':
        func(elastic_node=config['elasticsearch_host'],
        backup_repository=config['backup_repository'], snapshot_name=args.name)
    elif args.function == 'delete':
        func(elastic_node=config['elasticsearch_host'],
        backup_repository=config['backup_repository'], snapshot_name=args.name,
        age=args.age)
    else: # Should never get here. argparse should pick this up
        logging.error('Invalid option {}'.format(args.function))
        exit()

if __name__ == '__main__':
    main()
