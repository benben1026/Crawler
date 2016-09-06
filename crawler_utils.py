import requests
import grequests
import urllib2
import sys
import time
import random

from lxml import html
import urlparse

def request_exception_handler(request, exception):
    print 'request failed: '+str(exception)

def download_batch(urls, batch_size, to_secs): #set url set, int batch size, float timeout seconds
   urls_li=list(urls)
   result=[]
   for i in range(0, len(urls_li), batch_size): #send requests concurrently by one batch_size at a time
       reqs=(grequests.get(url, timeout=to_secs) for url in urls_li[i:i+batch_size])
       result=result+grequests.map(reqs, exception_handler=request_exception_handler)
   return result

def download_content(url, to_secs, proxies):
    timeout = 10
    while True:
        try:
            (proto, addr) = random.choice(proxies)
            agent = random.choice(['Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36'])
            headers = {'User-Agent':agent}
            response=requests.get(url, timeout=timeout, proxies={proto:addr}, headers=headers)
            sys.stderr.write('downloaded: '+url+"\n")
            return response.content
        except requests.exceptions.RequestException as e:
            timeout = min(to_secs, timeout+10)
            sys.stderr.write('request failed: '+url+" "+str(e)+"\n")
            time.sleep(random.choice([3, 6]))
            continue

headers={'User-Agent':'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

def download_browser(url, to_secs, proxies):
    try:
        #response=requests.get(url, timeout=to_secs, proxies=proxies)
        request=urllib2.Request(
                url=url,
                headers=headers
                )

#        proxy=urllib2.ProxyHandler(proxies)
#        opener=urllib2.build_opener(proxy)
#        urllib2.install_opener(opener)

#        response=urllib2.urlopen(url=request, timeout=to_secs).read()
        response=urllib2.urlopen(request).read()
        return response
    except urllib2.URLError as e:
        print 'request failed: '+str(e)
        return None


#return links in this reponse
def parse(response):
    try:
        parsed_body=html.fromstring(response.content)
        joined_link=list({unicode(urlparse.urljoin(response.url, url)) for url in parsed_body.xpath('//a/@href') if urlparse.urljoin(response.url, url).startswith('http')})

        return joined_link
    except:
        print 'parse failed: '+response.content
        return []

# remove the tag string (#...) in an url
def urlprocess(base_url, url):
    return urlparse.urljoin(base_url, url.split('#')[0])

def urlpredicate(url, rules_include, rules_exclude):
    if(not url.startswith('http')):
        return None

    included = False
    matched_url = ""
    for pattern in rules_include:
        if pattern.match(url) != None:
            included = True
            matched_url = pattern.match(url).group()
            break

    if not included:
        return None

    for pattern in rules_exclude:
        if pattern.match(url) != None:
            return None

    return matched_url

#return links in this reponse
def parse_removetag(response):
    try:
        parsed_body=html.fromstring(response.content)
        joined_link=list({unicode(urlprocess(response.url, url)) for url in parsed_body.xpath('//a/@href') if urlparse.urljoin(response.url, url).startswith('http')})

        return joined_link
    except:
        print 'parse failed: '+response.content
        return []

def parse_browser_removetag(_url, response):
    try:
        parsed_body=html.fromstring(response)
        joined_link=list({unicode(urlprocess(_url, url)) for url in parsed_body.xpath('//a/@href') if urlparse.urljoin(_url, url).startswith('http')})

        return joined_link
    except:
        print 'parse failed: '+response
        return []

def parse_browser_removetag_userules(_url, response, rules_include, rules_exclude):
    try:
        parsed_body=html.fromstring(response)
        joined_link = []
        for url in parsed_body.xpath('//a/@href'):
            # sys.stderr.write(url)
            url = urlpredicate(unicode(urlprocess(_url, url)), rules_include, rules_exclude)
            if url == None:
                continue
            joined_link.append(unicode(urlprocess(_url, url)))
        return joined_link
    except:
        print 'parse failed: '+response
        return []

#rearrange url list
#urls => list; group_size => int; return value => list
def group_urls(urls, group_size):
    return [urls[i:i+group_size] for i in range(0, len(urls), group_size)]



