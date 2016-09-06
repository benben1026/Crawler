import re

class CrawlerConfig:
    def __init__(self):
        # Seed set of urls
        self.urls_init = []

        # Tor port list. Workers will switch port when one is too slow
        # The threshold of the time is defined by the next variable
        self.tor_port_list = [8118]

        self.port_switch_threshold = 5.0

        self.max_retry_time = 10

        self.conn_timeout = 10

        self.read_timeout = 10

        # urls that match these regex will be processed
        self.rules_include = []

        # after processing rules_include, urls that match
        # the following regex will be dropped
        self.rules_exclude = []

        self.hdfspath_output = '/datasets/crawl/tmp/'

        self.parse_handlers = dict()
        
        # If group_url is set to an integer, which means the master will 
        # group the urls into samll groups with the size of this integer.
        # And each group of urls will be sent to the worker at one time to 
        # do the downloading
        self.group_url = False

    def set_rules_include(self, rules):
        self.rules_include = [re.compile(rule) for rule in rules]

    def set_rules_exclude(self, rules):
        self.rules_exclude = [re.compile(rule) for rule in rules]

    def add_parse_handler(self, pattern, handler):
        self.parse_handlers[re.compile(pattern)] = handler
