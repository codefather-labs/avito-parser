import sys
import uvloop
import ssl
import certifi
import asyncio
import re
import json
import time
import datetime

from bs4 import BeautifulSoup
from utils import Proxy
from aiohttp import ClientSession
from aiohttp.client_exceptions import (
    ClientHttpProxyError, ClientConnectorError, ServerDisconnectedError, ClientOSError
)

from utils import get_cityes, get_vacancies, gen_random_headers, get_clear_description

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
loop = uvloop.new_event_loop()
ssl_context = ssl.create_default_context(cafile=certifi.where())


class AvitoParser:
    '''
    Парсер организован таким образом, что есть возможность парсить каждую страницу отдельным proxy.
    Это может пригодиться при шедулинге тасков, распараллеливании запросов и прочего увеличения производительности.

    Для использования прокси листа, требуется раскоментить внутри класса следующее:
        # self.proxy = Proxy()
        # self.pool = self.proxy.get_pool()

    затем, везде где в коде вызывается self.proxy, - нужно заменить на self.proxy.get_random().
    Это заставит прокси класс инициализировать пул проксей (по умолчанию 3). Класс прокси найдет 3 рабочих на данный
    момент http прокси и сохранит их в текущий пул, для дальнейшего использования парсером при вызове функции -
    self.proxy.get_random()

    Парсер умеет повторять запросы, которые по какой-то причине не удалось распарсить.
    Так называемые "сломанные страницы" или "broken urls" это и есть список, в который добавляются ссылки, которые -
    не удалось распарсить. Функция работает, однако она временно закоменчена, так как не доделана. Смотреть #FIXME

    Проблема: сломанные ссылки отрабатывают заново и механизм работает, однако нужно научить механизм понимать, -
    какие сломанные ссылки являются родительскими, а какие дочерними, чтоб правильно их парсить. Эта проблема
    касается только контекста отработки broken urls или "сломанных ссылок".

    Все эксепшены отрабатываются и обрабатываются.
    В проэктировке частично был применен такой алгоритм как State machine или "Паттерн состояний".
    Чтобы проследить цепочку взаимодействий кода, нужно начать с функции start, и следовать нумерации комментариев.

    Глоссарий:
    Функция = Корутина (исключение __init__)
    '''

    def __init__(self):
        self.soup = None
        self.child_soup = None
        self.crawled = None
        self.proxy = Proxy()
        self.response = None
        self.session = None

        self.cityes = get_cityes()
        self.vacancies = get_vacancies()

        # self.proxy = Proxy()
        # self.pool = self.proxy.get_pool()
        # 187.16.4.121:8080
        self.proxy = None
        self.data = []

        self.base_url = 'https://www.avito.ru'
        self.url = None
        self.urls = []
        self.broken_urls = []
        self.current_page = None

        self.start_time = time.ctime()
        self.start_timestamp = datetime.datetime.now()

    async def logger(self, message, level: int = None):
        info_level = 'info'
        warning_level = 'warning'
        default_level = 'info'

        if not level:
            level = default_level
        if level == 1:
            level = info_level
        if level == 2:
            level = warning_level

        print(f'[avito logger][{level}]: {message}')

    async def gen_urls(self):
        # 1.1 - итеррируемся по файлам и собираем города и вакансии
        for city in self.cityes:
            for vacanci in self.vacancies:
                # 1.2 - кладем сгенерированный ссылки в локальньную переменную класса парсера
                self.urls.append(f"{self.base_url}/{city}/vakansii?s=104&q={vacanci}")

        # 1.3 - Информаируем пользователя о количестве сгенерированных ссылок
        await self.logger(f'Generated {len(self.urls)} urls for parse.')

    async def start(self):
        await self.logger(f"Started at {self.start_time}")

        # 1 - генерируем ссылки на основании городов и вакансий из файлов
        await self.gen_urls()
        # 2 - вызываем функцию запроса
        await self.request()
        # 3 - Вызываем функцию on_close()
        # Она исполняется когда собраны все данные по всем страницам.
        # Она отвечает за сохранение всех собранных данных в любой удобный нам формат.
        # В дальнейшем, когда нужно будет сохранять данные в бд, нужно будет редактировать только эту функцию.
        await self.on_close()

    async def on_close(self):
        with open('data.json', 'w', encoding='utf-8') as data_file:
            data_file.write(json.dumps(self.data, indent=4))

        await self.logger('---------------------------------')
        await self.logger(f"Parsed {len(self.data)} items")
        await self.logger(f"Started at {self.start_time}")
        await self.logger(f"Ended at {time.ctime()}")
        await self.logger('---------------------------------')
        if not self.broken_urls:
            await self.logger('No one broken url. Exit code: 0')

        # FIXME
        # if self.broken_urls:
        #     try:
        #         self.urls = self.broken_urls
        #         await self.request(broken=True)
        #     except:
        #         await self.logger(f'Cant fetch broken urls: [{self.broken_urls}]. Exit code: 1')

    async def fetch(self, proxy=None):
        # 2.3.1 - Информирует пользователя об используемом прокси (если используется) и используемой ссылке
        await self.logger(f"Using proxy: {proxy}")
        await self.logger(f"Using url: {self.url}")

        try:
            # 2.3.2 - Пытаемся отправить запрос и получить html страницы
            async with self.session.get(
                    self.url,
                    ssl=ssl_context,
                    # proxy=f"http://{proxy}",
                    headers=await gen_random_headers()) as response:
                # 2.3.3 - Возвращаем html страницы
                return await response.text()

        # 2.3.4 - Ловим эксепшены и стараемся спарсить еще раз сломанные страницы где это возможно.
        # В иных случаях, добавляем сломанные страницы в соответствующий список.
        # Позже, в функции on_close() парсер попытается получить их еще раз.

        except KeyboardInterrupt:
            await self.logger('KeyboardInterrupt exit', 2)
            await self.on_close()
            exit()

        except ServerDisconnectedError:
            await self.logger('Retry exception ServerDisconnectedError', 2)
            await self.on_close()
            exit()

        except ClientOSError:
            await self.logger('Retry exception ClientOSError', 2)
            self.broken_urls.append(self.url)
            await self.fetch(self.proxy)

        except ClientHttpProxyError:
            await self.logger('Retry exception ClientHttpProxyError', 2)
            self.broken_urls.append(self.url)
            await self.fetch(self.proxy)

        except ClientConnectorError:
            await self.logger('Retry exception with ClientConnectorError', 2)
            self.broken_urls.append(self.url)
            await self.fetch(self.proxy)

    async def request(self, broken=None):
        # 2.1 - Если ловим запрос со сломанными ссылками
        if broken:
            # 2.1.1 - Информируем пользователя о попытке собрать данные еще раз по сломанным ссылкам
            await self.logger(f'Try to fetch broken urls on close...')

        # 2.2 - Инициализируем клиентскую сессию
        async with ClientSession(loop=loop) as self.session:
            for url in self.urls:
                self.url = url
                # 2.3 - Фетчим содержание текущей страницы, передаем ему прокси (если нужно. по умолчанию None)
                # Результатом фетчинга будет содержание страницы
                self.response = await self.fetch(self.proxy)
                # 2.4 - Отправляем содержание страницы для краулинга
                await self.crawl(self.response)

            # 2.5 - Вызываем функцию, которая парсит дочерние страницы.
            # Эта функция будет вызвала после исполнения цикла выше, т.е., когда будут собраны все ссылки -
            # по внешним страницам.
            await self.crawl_children()

    async def crawl(self, response):
        # 2.4.1 - Пытаемся создать объект супа, получая на входе html страницу
        try:
            self.soup = BeautifulSoup(response, 'html.parser')
        except TypeError:
            # 2.4.2 - Если ловим эксепшн, информируем пользователя.
            await self.logger(f"Catch TypeError in: {self.url}.")

        # 2.4.3 - Объявляем нужное нам значение для нашего удобства и работы стейт машины
        self.crawled = True

        # вызываем функцию которая парсит главные страницы
        await self.parse()

    async def save_data(self, data):
        # асинхронная функция, которая сохраняет спарсенные данные в локальную переменную класса парсера

        await self.logger(
            f"saved data: {data['city']} {data['job']} ID: {data['post_id']} phone:{data['contact_phone_number']}"
        )
        self.data.append(data)

    async def parse(self):
        # 2.4.5.1 - Парсим страницу каталога
        if self.crawled and self.response:
            if str(self.soup.title) == '<title>Доступ с вашего IP-адреса временно ограничен — Авито</title>':
                await asyncio.sleep(1)
                return await self.fetch(self.proxy)

            await self.logger(self.soup.title.text)

            # дальше хардкор
            try:
                item_divs = self.soup.findAll('div', class_='item__line')
                try:
                    next_page_url = self.soup.find('div', class_='pagination-nav clearfix').a['href']
                    absolute_next_page_url = f"{self.base_url}{next_page_url}"
                    self.current_page = int(
                        self.soup.find('span', class_='pagination-page pagination-page_current').text)
                    await self.logger(f"Current page: {self.current_page}")
                except AttributeError:
                    pass

                for div in item_divs:
                    post_link = f"{self.base_url}{div.div.a['href']}"
                    post_id = post_link.split('_')[-1]
                    city = post_link.split('/')[3]

                    try:
                        img_src = div.div.a.img['src']
                        if img_src[0] == '/':
                            img_src = f'https:{img_src}'
                    except:
                        img_src = None

                    base_card = div.find('div', class_='item_table-wrapper')

                    job = str(re.sub('\n', '', str(base_card.div.div.h3.a.text))[:-2][2:])
                    currency = str(base_card.find('div', class_='about').span['content'])
                    price = str(re.sub('\n', '', str(base_card.find('span', class_='price').text))[:-4][1:])

                    if price == 'Зарплата не указа':
                        price = 'Зарплата не указана'

                    await asyncio.sleep(3)
                    # https://m.avito.ru/api/1/items/1803810996/phone?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir
                    api_key = 'af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir'
                    mlink = f'https://m.avito.ru/api/1/items/{post_id}/phone?key={api_key}'
                    async with self.session.get(
                            mlink,
                            ssl=ssl_context,
                            # proxy=f"http://{proxy}",
                            headers=await gen_random_headers()) as response:
                        result = await response.text()
                        result = json.loads(result)
                        phone_number = str(result['result']['action']['uri']).split('=')[1][3:]

                    data = {
                        "current_page": self.current_page,
                        "post_link": post_link,
                        "post_id": post_id,
                        "post_img_link": img_src,
                        "city": city,
                        "job": job,
                        "price": price,
                        "currency": currency,
                        "contact_phone_number": phone_number,
                        "post_time": None,                              # время публикации
                        "post_count_views": None,                       # количество просмотров
                        "kind_of_activity": None,                       # сфера деятельности
                        "job_schedule": None,                           # график работы
                        "required_experience": None,                    # ожидаемый опыт
                        "address": None,                                # адресс
                        "map_lat": None,
                        "map_lon": None,
                        "address_coordinates": None,                    # координаты адреса
                        "contact_llc": None,                            # наименование контакта (ИП Иванов А.А.)
                        "contact_from": None,                           # дата регистрации на авито
                        "count_finished_posts": None,                   # количество завершенных постов работадателя
                        "contact_name": None,                           # контактное лицо (Игорь)
                        "contact_url": None,                            # ссылка на контакт
                        "raw_description": None,                        # описание вакансии c html тегами
                        "clean_description": None,                      # чисто описание
                    }

                    await self.save_data(data)
                # 2.4.5.2 - Ждем для имитации задержки браузера
                await asyncio.sleep(2)

            except AttributeError:
                # 2.4.5.3 - если ловим AttributeError, информирует пользователя,
                # добавляем ссылку в список сломанных ссылок,
                # ждем 15 секунд
                await self.logger(f"Catch AttributeError in: {self.url}. Retry...")
                self.broken_urls.append(self.url)
                await self.logger("Added to broken urls. Continue.")
                await asyncio.sleep(15)

    async def crawl_children(self):
        # 2.5.1 - Информирут пользователя о старте краулинга с дочерни страниц
        await self.logger('Start crawl child pages')

        # 2.5.2 - Итеррируемся циклом по всех родительским данным, и заираем ссылки на посты
        for child in self.data:
            child_url = child['post_link']
            self.url = child_url

            self.response = await self.fetch(self.proxy)
            # 2.5.3 - Создаем новый объект супа и парсим его
            self.child_soup = BeautifulSoup(self.response, 'html.parser')

            if str(self.child_soup.title) == '<title>Доступ с вашего IP-адреса временно ограничен — Авито</title>':
                await asyncio.sleep(1)
                return await self.fetch(self.proxy)

            await self.logger(self.child_soup.title.text)
            # дальше хардкор

            base_card = self.child_soup.find('div', class_='item-view-content')
            left_side = base_card.find('div', class_='item-view-content-left')
            right_side = base_card.find('div', class_='item-view-content-right')

            post_time = str(left_side.find('div', class_='title-info-metadata-item-redesign').text)[:-2][4:]

            try:
                kind_of_activity = left_side.findAll('li', class_='item-params-list-item')[0]
                kind_of_activity = kind_of_activity.text

                job_schedule = left_side.findAll('li', class_='item-params-list-item')[1]
                job_schedule = job_schedule.text

                required_experience = left_side.findAll('li', class_='item-params-list-item')[2]
                required_experience = required_experience.text
            except:
                result = str(left_side.find('div', class_='item-params item-params_type-one-colon').text)

                kind_of_activity = result.split(';')[0]
                job_schedule = result.split(';')[1]
                required_experience = result.split(';')[2]

            kind_of_activity = re.sub('\n', '', str(kind_of_activity))
            job_schedule = re.sub('\n', '', str(job_schedule))
            required_experience = re.sub('\n', '', str(required_experience))

            address = f"{left_side.find('span', class_='item-address__string').text}," \
                      f"{left_side.find('span', class_='item-address-georeferences-item__content').text}"

            map_lat = left_side.find('div', class_='b-search-map item-map-wrapper js-item-map-wrapper')['data-map-lat']
            map_lon = left_side.find('div', class_='b-search-map item-map-wrapper js-item-map-wrapper')['data-map-lon']

            try:
                post_description = left_side.find('div', class_='item-description-text').p
            except AttributeError:
                post_description = left_side.find('div', class_='item-description-html').p

            clear_description = await get_clear_description(post_description)

            contact_llc = None
            try:
                contact_llc = right_side.find('div', class_='seller-info-name js-seller-info-name').a.text
                contact_llc = re.sub('\n', '', str(contact_llc))[:-1][1:]
            except AttributeError:
                pass

            contact_url = None
            try:
                contact_url = self.base_url + right_side.find('div', class_='seller-info-name js-seller-info-name').a['href']
            except AttributeError:
                pass

            contact_from = None
            try:
                contact_from = right_side.findAll('div', class_='seller-info-value')[1]
                contact_from = re.sub('\n', '', str(contact_from.div.text))[:-1][1:]
            except AttributeError:
                pass

            count_finished_posts = None
            try:
                for line in right_side.findAll('div', class_='seller-info-value'):
                    if 'Завершено' in str(line):
                        count_finished_posts = str(line.text).split('\n')[4]
                        break
            except:
                pass

            try:
                contact_name = right_side.findAll('div', class_='seller-info-value')[5]
                contact_name = re.sub(' ', '', str(contact_name.text))
                contact_name = re.sub('\n', '', str(contact_name))
            except:
                contact_name = None

            post_count_views = right_side.find('a', class_='js-show-stat').text

            # 2.5.4 - обновляем родительские данные дочерними
            child['contact_name'] = str(contact_name)
            child['contact_url'] = str(contact_url)
            child['post_time'] = str(post_time)
            child['post_count_views'] = str(post_count_views)
            child['kind_of_activity'] = str(kind_of_activity)
            child['job_schedule'] = str(job_schedule)
            child['required_experience'] = str(required_experience)
            child['address'] = str(address)
            child['map_lat'] = float(map_lat)
            child['map_lon'] = float(map_lon)
            child['address_coordinates'] = f"{map_lat}, {map_lon}"
            child['contact_llc'] = str(contact_llc)
            child['contact_from'] = str(contact_from)
            child['count_finished_posts'] = str(count_finished_posts)
            child['raw_description'] = str(post_description)
            child['clean_description'] = str(clear_description)

            # ждем
            await asyncio.sleep(3)


if __name__ == '__main__':  # входная точка
    assert sys.version_info >= (3, 7), "Script requires Python 3.7+."
    try:
        parser = AvitoParser()  # инициализируем класс парсер
        uvloop.install()  # используем uvloop как цикл событий
        loop.run_until_complete(parser.start())  # вызываем функцию start внутри цикла событий
    except KeyboardInterrupt:
        pass
