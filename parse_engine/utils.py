import random
import requests
import re


def get_cityes():
    f = open('cityes.txt', 'r').read()
    f = f.split(',')
    return f


def get_vacancies():
    f = open('vacancies.txt', 'r').read()
    f = f.split(',')
    return f


async def get_random_user_agent():
    f = open('user-agents.txt', 'r').read()
    f = f.split('\n')
    return random.choice(f)

async def get_clear_description(raw):
    raw = re.sub('<p>', '', str(raw))
    raw = re.sub('</p>', '', str(raw))
    raw = re.sub('<br/>', ' ', str(raw))
    return raw

async def gen_random_headers():
    return {
        "accept": "*/*",
        "user-agent": await get_random_user_agent()
    }



class Proxy:

    def __init__(self, value: int = None, pool: list = None, connected: list = None, providers=None):
        self.api_key = 'r598f4-i05562-044v4j-175348'
        self.value = 3
        if value:
            self.value = value

        self.connected = connected
        self.available = None
        self.pool = []

        if pool:
            self.pool = pool
        if connected:
            self.connected = connected

        self.providers = [
            "https://www.proxy-list.download/api/v1/get?type=http",
            # "https://www.proxy-list.download/api/v1/get?type=https",
        ]

    def logger(self, message):
        print(f"[proxy]: {message}")

    def get_pool(self):
        if not self.pool:
            _list = []
            for p in self.providers:
                response = requests.get(p).text
                response = response.split("\r\n")[:-1]
                _list.append(response)
            self.pool = _list[0]
            return self.check_connection(self.pool)
        else:
            return self.check_connection(self.pool)

    def get_random(self):
        if self.connected:
            return random.choice(self.connected)

    def check_connection(self, pool):
        self.connected = []
        random_list = []

        for p in self.pool:
            random_list.append(random.sample(self.pool, len(p)))

        self.logger(f'looking for {self.value} available https proxies')
        for line in random_list:
            if not len(self.connected) >= self.value:
                for proxy in line:

                    host = proxy.split(":")[0]
                    port = proxy.split(":")[1]

                    request = requests.get('https://www.avito.ru/', proxies={host: port})
                    # print(f"{host}:{port} {request.status_code}")

                    if request.status_code == 200:
                        # check = requests.get(f'http://proxycheck.io/v2/{host}?key={self.api_key}').json()
                        check = requests.get(f'http://proxycheck.io/v2/{host}').json()
                        try:
                            self.logger(check)
                            proxy_status = check[host]['proxy']
                            proxy_type = check[host]['type']
                            if proxy_status == 'yes' and proxy_type == 'HTTPS' or proxy_type == 'HTTP':
                                with open('http_proxy_list.txt', 'a', encoding='utf-8') as proxy_file:
                                    proxy_file.write(f"{host}:{port},")
                                    proxy_file.close()
                                self.logger(host, proxy_status, proxy_type)
                                self.connected.append(proxy)
                                self.logger(f"{proxy}: 200")
                                self.logger(f"found {len(self.connected)} available https proxies from {self.value}")
                        except:
                            pass

                    else:
                        proxy_logger.info(f"{proxy}: {request.status_code}")

        return self.connected
