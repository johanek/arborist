''' cli '''

import getopt
import sys
import yaml
from voluptuous import Schema, Required, MultipleInvalid


def getconfig(argv):
    ''' process command line arguments '''
    try:
        opts, _ = getopt.getopt(argv, "c:h", ['config', 'help'])  # pylint: disable=unused-variable
        if not opts:
            raise SystemExit(usage())
    except getopt.GetoptError:
        raise SystemExit(usage())

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            usage()
            sys.exit(2)
        elif opt in ('-c', '--config'):
            config = loadconfig(arg)
            with open(arg, 'r') as stream:
                config = yaml.load(stream, Loader=yaml.FullLoader)
        else:
            raise SystemExit(usage())

    return config


def loadconfig(filename):
    ''' load config and validate schema '''
    with open(filename, 'r') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    configschema = {
        Required('kafka_servers'): list,
        Required('log_consumer_topics'): list,
        Required('consumer_group'): str,
        Required('debug'): bool,
    }

    try:
        schema = Schema(configschema, extra=False)
        schema(config)
    except MultipleInvalid as e:
        print("Missing values from config file!")
        print(str(e))
        sys.exit(1)

    return config


def usage():
    ''' usage info '''
    output = """
Usage:
  streamric -c configfile

Options:
  -c <configfile>   Read config from configfile"""

    return output
