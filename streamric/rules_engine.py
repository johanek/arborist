import logging
import re
from streamric.cache import StreamCache

LOGGER = logging.getLogger('streamric')


class StreamRules(object):

    # @classmethod
    # def process(cls, record):

    #     rules = [
    #         {
    #             'name': 'ssh_invalid_user',
    #             'log_type': 'syslog',
    #             'match_type': 'field',
    #             'field': 'tags',
    #             'value': 'SSH_INVALID_USER',
    #             'alert': [ 'pagerduty', 'slack'],
    #             'config': {
    #                 'slack_channel': '#testing'
    #             },
    #             'summary': 'Failed login for {0} on {1} at {2}',
    #             'summary_fields' : [ 'username', 'syslog_hostname', '@timestamp' ]
    #         }
    #     ]
    #     alerts = []

    #     # Maybe collect just the relevant rules for the log type
    #     for rule in rules:
    #         try:
    #             if rule['value'] in record[rule['field']]:
    #                 # summary = 'Failed login for %s on %s at %s' % (record['username'], record['syslog_hostname'], record['@timestamp'])
    #                 summary_values = []
    #                 for i in range(len(rule['summary_fields'])):
    #                     if summary_values[i] is None:
    #                         summary_value = rule['summary_fields'][i]
    #                         if summary_value:
    #                             summary_values[i] = alert_value
    #                 summary = rules['summary'] % summary_values
    #                 alerts.append({ 'rule': rule, 'summary': summary})
    #         except KeyError as exception:
    #             continue

    #     return alerts
    @classmethod
    def process(cls, rule={}):
        cache = StreamCache()

        entries = cache.read_from_cache(rule['name'], rule['window'])
        matches = []
        rule_regex = re.compile(rule['match_value'])
        for entry in entries:
            if entry['type'] == rule['message_type']:
                message = entry['message']
                search_result = rule_regex.search(message)
                if search_result:
                    entry['_regex_matches'] = search_result.groups()
                    matches.append(entry)

        for match in matches:
            print(rule['summary'].format(*match['_regex_matches']))
        LOGGER.info(
            f"Matched {len(matches)} from {len(entries)} for rule {rule['name']}"
        )
