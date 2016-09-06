# Crawler
Distributed webpage crawler making use of Husky

import sys
import re
import json
import bs4

from Crawler import crawler_dist as cd
from Crawler import crawler_config
import bindings.frontend.env as env
 
def parse_name(url, html):
  try:
    soup = bs4.BeautifulSoup(html, "lxml")
    content = soup.find("div", {"class" : "healthaz-header"}).text.strip()
    return json.dumps({"name": content.split('\n')[0], "url": url})
  except:
    sys.stderr.write('parse failed: ' + url + '\n')
    return json.dumps({"url": url})
 
 
config = crawler_config.CrawlerConfig()

config.urls_init=['http://www.nhs.uk/Conditions/Pages/BodyMap.aspx?Index=A']

config.set_rules_include(['http://www.nhs.uk/conditions/[a-zA-Z\- ]+'])
  
config.add_parse_handler('http://www.nhs.uk/conditions/[a-zA-Z\- ]+', parse_name)
  
config.hdfspath_output = '/datasets/crawl/nhs-new-A2/'
  
config.group_url = 5
  
config.max_retry_time = 10
  
config.conn_timeout = 100
  
config.read_timeout = 100
  
config.tor_port_list = range(18112, 18161)
  
config.port_switch_threshold = 5.0
  
config.num_iters = 2
 
env.pyhusky_start()
cd.crawler_run(config)
