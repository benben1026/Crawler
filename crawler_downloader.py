from TorCtl import TorCtl
import requests
import random
import sys
import time
import urllib3

class CrawlerDownloader:
    def __init__(self, tor_port_list, port_switch_threshold, max_retry_time = 5, conn_timeout = 10, read_timeout = 10):
        #create a new session
        #self.sess = requests.Session()

        self.url = ""

        self.port_switch_threshold = port_switch_threshold
       
        self.max_retry_time = max_retry_time

        self.tor_port_list = tor_port_list

        self.conn_timeout = conn_timeout

        self.read_timeout = read_timeout

        self.curr_tor_port = str(random.choice(self.tor_port_list))

        self.user_agent = ['Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36', 
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36']

    def set_url(self, url):
        self.url = url
    
    @staticmethod
    def renew_connection():
        sys.stderr.write('Renew Connection')
        conn = TorCtl.connect(controlAddr="127.0.0.1", controlPort=9051, passphrase="ddmbr")
        conn.send_signal("NEWNYM")
        conn.close()

    #port_list should be a python list
    def set_proxy_port_list(self, port_list):
        self.tor_port_list = port_list

    def get_new_tor_port(self):
        self.curr_tor_port = str(random.choice(self.tor_port_list))
        sys.stderr.write('Change to new port: ' + self.curr_tor_port + '\n') 

    def set_proxy_manager(self):
        self.get_new_tor_port()
        self.proxy_manager = urllib3.ProxyManager(
                    'http://127.0.0.1:' + self.curr_tor_port,
                    timeout = urllib3.Timeout(connect = self.conn_timeout, read = self.read_timeout),
                    retries = urllib3.Retry(self.max_retry_time),
                    maxsize = 5
                )

    def batch_download_content(self):
        self.set_proxy_manager()
        elapsed_time = 0
        output = []
        for i in range(0, len(self.url)):
            if elapsed_time > self.port_switch_threshold:
                self.set_proxy_manager()
            try:
                start = time.time()
                response = self.proxy_manager.request(
                            'GET',
                            self.url[i],
                            preload_content = False,
                        )
                elapsed_time = time.time() - start
                sys.stderr.write('Using Port: ' + self.curr_tor_port + ' | '  + str("{0:.4f}".format(elapsed_time)) + 's used to download: ' + self.url[i])
                output.append((self.url[i], response.read()))
            except urllib3.exceptions.MaxRetryError as e:
                sys.stderr.write('Max Retry Exception occurred when downloading ' + self.url[i] + '\n')
                elapsed_time = 0
                self.set_proxy_manager()
            except urllib3.exceptions.ReadTimeoutError as e:
                sys.stderr.write('Read Timeout Exception occurred when downloading ' + self.url[i] + '\n')
                elapsed_time = 0
                self.set_proxy_manager()
    
        return output       


    def download_content(self):
        self.set_proxy_manager()
        elapsed_time = 0
        try:
            start = time.time()
            response = self.proxy_manager.request(
                        'GET',
                        self.url,
                        preload_content = False,
                    )
            elapsed_time = time.time() - start
            sys.stderr.write('Using Port: ' + self.curr_tor_port + ' | '  + str("{0:.4f}".format(elapsed_time)) + 's used to download: ' + self.url)
            return (self.url, response.read())
        except urllib3.exceptions.MaxRetryError as e:
            sys.stderr.write('Max Retry Exception occurred when downloading ' + self.url + ' \n')

