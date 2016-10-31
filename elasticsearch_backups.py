#!/usr/bin/env python3

def generate_snapshot_name(prefix='snapshot-'):
    import datetime

    # generate date string in UTC time
    date = datetime.datetime.utcnow().strftime("%Y%m%d")
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

def backup(elastic_node, backup_repository, port=9200):
    import json, requests, os, sys

    # Get new snapshot name
    snapshot_name = generate_snapshot_name()

    # Assemble url
    snapshot_url = 'http://{0}:{1}/_snapshot/{2}/{3}'.format(elastic_node, port, backup_repository, snapshot_name)

    # Send REST API call
    try:
        request = requests.put(snapshot_url)
        request.raise_for_status()
    except ConnectionError as e:
        print("Failed to create {0}:\n{1}:\n{2}".format(snapshot_name, e, json.loads(request.text)))
    except requests.exceptions.HTTPError as e:
        print("Failed to create {0}:\n{1}:\n{2}".format(snapshot_name, e, json.loads(request.text)))
    except requests.exceptions.ConnectionError as e:
        print("Failed to create {0}:\n{1}".format(snapshot_name, e))
    # except Exception as e:
    #     pass
    #     print("Failed to create {0}".format(snapshot_name), e)

def delete():
    pass # TODO

def main():
    import argparse

    # Parse arguments
    parser = argparse.ArgumentParser(description='Take subcommands and parameters for elasticsearch backup script.')
    parser.add_argument('function', type=str, choices=['backup','delete'], help='Triggers a new elasticsearch action.')
    parser.add_argument('--config', type=str, help='Specify path to a config file.')
    args = parser.parse_args()

    # Find config file, and read in config
    config = find_config(args.config)

    # Map argument string to function name
    FUNCTION_MAP = {'backup': backup, 'delete': delete}

    # Initialise callable variable using the FUNCTION_MAP
    func = FUNCTION_MAP[args.function]

    # Call funky voodoo variable function
    if args.function == 'backup':
        func(elastic_node=config['elasticsearch_host'], backup_repository=config['backup_repository'])
    elif args.function == 'delete':
        func()
    else: # Should never get here. argparse should pick this up
        print('Invalid option {}'.format(args.function))
        exit()






if __name__ == '__main__':
    main()
