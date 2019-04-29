''' Tests for XXX '''


from arborist import Arborist

CONFIG = {
        'kafka_servers': ['kafka1'],
        'log_consumer_topics': ['test'],
        'consumer_group': 'test',
        'debug': False,
    }

def test_config():
    instance = Arborist(CONFIG)
    assert instance

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
