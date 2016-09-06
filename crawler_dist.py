import timeit
import re
import json
import urllib3
import time
import sys

import bindings.frontend as fe
import bindings.frontend.env

import crawler_utils as ct
import crawler_downloader as cdo
def mapper_download_parse(url):
    response=ct.download_content(url, out_time, proxies)

    if(response is not None):
        res=ct.parse_browser_removetag(url, response)
        return res

    return []

def mapper_download(url, downloader):
    downloader.set_url(url)
    #downloader.renew_connection()
    return downloader.download_content()
    #response=ct.download_content(url, timeout, proxies)

def mapper_download_batch(urls, downloader):
    downloader.set_url(urls)
    return downloader.batch_download_content() 

def mapper_parse(htmltuple, rules_include, rules_exclude):
    if(htmltuple[1] is not None):
        res=ct.parse_browser_removetag_userules(htmltuple[0], htmltuple[1], rules_include, rules_exclude)
        return res
    else:
        return []

def parse_html(config, url_html_pair):
    parse_handlers = config.parse_handlers
    def mapper((url, html)):
        for pattern in parse_handlers:
            if pattern.match(url) != None:
                return parse_handlers[pattern](pattern.match(url).group(), html)
        return json.dumps({"type":"n", "url":url})
    return url_html_pair.map(mapper)

def log_msg(msg):
    print "[pyhusky-log]:"+msg

def crawler_run(config, existing_progress=None):
    # FIXME Do it in a clever way
    downloader = cdo.CrawlerDownloader(config.tor_port_list, config.max_retry_time, config.conn_timeout, config.read_timeout)
    #http = urllib3.ProxyManager('http://127.0.0.1:8118', maxsize=50)
    this_round_urls = fe.env.parallelize(config.urls_init).cache()
    history = fe.env.parallelize(config.urls_init).cache()
    if existing_progress != None:
        this_round_urls = fe.env.load(existing_progress).map(lambda line: json.loads(line)["url"]).cache()
        history = fe.env.load(existing_progress).map(lambda line: json.loads(line)["url"]).cache()

    iter = 0
    start = timeit.default_timer()
    next_len = 1

    while next_len:
        if config.group_url and iter >= 1:
            log_msg("converting")
            this_round_urls = fe.env.parallelize(ct.group_urls(this_round_urls.collect(), config.group_url))
            log_msg("downloading")
            this_round_htmltuple = this_round_urls.flat_map(lambda urls: mapper_download_batch(urls, downloader)).cache()
        else:    
            log_msg("downloading")
            this_round_htmltuple = this_round_urls.map(lambda url: mapper_download(url, downloader)).cache()
        iter += 1
        log_msg("writing to hdfs")
        parse_html(config, this_round_htmltuple).write_to_hdfs(config.hdfspath_output) #write to hdfs
        # next_round_urls = this_round_htmltuple.flat_map(lambda htmltuple: mapper_parse(htmltuple, config.rules_include, config.rules_exclude)).cache()
        next_round_urls = this_round_htmltuple.flat_map(lambda htmltuple: mapper_parse(htmltuple, config.rules_include, config.rules_exclude)) \
            .map(lambda url: (url, 1)) \
            .reduce_by_key(lambda a, b: a+b) \
            .map(lambda (url, cnt): url) \
            .cache()

        # release html memory
        this_round_htmltuple.uncache()

        log_msg("calculating difference with history")
        url_diff = next_round_urls.difference(history).cache()
        # free this_round_urls and set it to next_round_urls
        this_round_urls.uncache()
#        this_round_urls = next_round_urls
        this_round_urls = url_diff 

        log_msg("create new history")
        next_round_history = history.concat(url_diff).cache()
        # free history and set it to next_round_history
        history.uncache()
        history = next_round_history

        print 'husky_crawler_info: round '+str(iter)+' speed: '+str(next_len)+' pages within '+str((timeit.default_timer()-start))+' seconds'
        next_len = url_diff.count()
        url_diff.uncache()
        hlen = history.count()
        start = timeit.default_timer()
        print 'husky_crawler_info: history size: ', hlen, ', next round size: ', next_len

        if iter == config.num_iters:
            break
