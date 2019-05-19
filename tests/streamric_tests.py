''' Tests for XXX '''


from streamric.processor import Stream

CONFIG = {
        'kafka_servers': ['kafka1'],
        'log_consumer_topics': ['test'],
        'consumer_group': 'test',
        'debug': False,
    }

def test_config():
    instance = Stream(CONFIG)
    assert instance

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
