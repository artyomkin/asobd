import random
import time
import uuid
import socket
import struct
import csv


class Click:
    def __init__(self,
                 type,
                 session_id,
                 ip,
                 user,
                 url,
                 referrer,
                 device_type,
                 user_agent,
                 payload):
        self.type = type
        self.created_at = time.time()
        self.session_id = session_id
        self.ip = ip
        self.user = user
        self.url = url
        self.referrer = referrer
        self.device_type = device_type
        self.user_agent = user_agent
        self.payload = payload

    def print(self):
        """Вывод информации о клике на экран"""
        print("=" * 50)
        print("CLICK INFORMATION")
        print("=" * 50)
        print(f"Type: {self.type}")
        print(f"Session ID: {self.session_id}")
        print(f"IP: {self.ip}")
        print(f"User: {self.user.print()}")
        print(f"URL: {self.url}")
        print(f"Referrer: {self.referrer}")
        print(f"Device Type: {self.device_type}")
        print(f"User Agent: {self.user_agent}")
        print("-" * 30)
        print("PAYLOAD:")
        if hasattr(self.payload, 'print'):
            self.payload.print()
        else:
            print(f"  Event: {self.payload.event_title}")
            print(f"  Element: {self.payload.element_id}")
            print(f"  Coordinates: ({self.payload.x}, {self.payload.y})")
        print("=" * 50)


class Payload:
    def __init__(self,
                 event_title,
                 element_id,
                 x,
                 y):
        self.event_title = event_title
        self.element_id = element_id
        self.x = x
        self.y = y

    def print(self):
        """Вывод информации о payload на экран"""
        print("  Event Title: {}".format(self.event_title))
        print("  Element ID: {}".format(self.element_id))
        print("  Coordinates: x={}, y={}".format(self.x, self.y))


class User:
    def __init__(self,
                 fio,
                 country):
        self.fio = fio
        self.country = country

    def print(self):
        """Вывод информации о пользователе на экран"""
        print("=" * 30)
        print("USER INFORMATION")
        print("=" * 30)
        print(f"Name: {self.fio}")
        print(f"Country: {self.country}")
        print("=" * 30)

elements = [
    {
        "name": "#hero-button",
        "event": "signup_click",
        "x-min": 400,
        "x-max": 550,
        "y-min": 300,
        "y-max": 340
    },
    {
        "name": "#email-input",
        "event": "newsletter_submit",
        "x-min": 350,
        "x-max": 500,
        "y-min": 450,
        "y-max": 480
    },
    {
        "name": "#feature-1-card",
        "event": "feature_expand",
        "x-min": 150,
        "x-max": 300,
        "y-min": 550,
        "y-max": 700
    },
    {
        "name": "#pricing-plan-1",
        "event": "plan_select",
        "x-min": 350,
        "x-max": 450,
        "y-min": 750,
        "y-max": 900
    },
    {
        "name": "#faq-item-3",
        "event": "faq_toggle",
        "x-min": 500,
        "x-max": 700,
        "y-min": 950,
        "y-max": 980
    },
    {
        "name": "#footer-contact",
        "event": "contact_click",
        "x-min": 50,
        "x-max": 200,
        "y-min": 1000,
        "y-max": 1020
    }
]

# type - view или click
type_values = ['view', 'click']

# url - 10 рандомных URL
url_values = [
    'https://shop.com/products/smartphone',
    'https://news.site/articles/tech-news',
    'https://social.net/profile/user123',
    'https://video.host/watch/abc123',
    'https://forum.site/topic/python-help',
    'https://blog.dev/how-to-code',
    'https://store.app/games/best-sellers',
    'https://travel.agency/tours/europe',
    'https://learning.edu/courses/data-science',
    'https://finance.bank/accounts/overview'
]

# referrer - 10 рандомных рефереров
referrer_values = [
    'https://google.com/search?q=test',
    'https://yandex.ru/search/?text=query',
    'https://facebook.com/feed',
    'https://twitter.com/home',
    'https://instagram.com/explore',
    'https://linkedin.com/feed',
    'https://reddit.com/r/programming',
    'https://tiktok.com/foryou',
    'https://vk.com/feed',
    'direct'
]

# device_type - 3 рандомных типа устройств
device_type_values = ['desktop', 'mobile', 'tablet']

# user_agent - 5 рандомных User-Agent строк
user_agent_values = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
    'Mozilla/5.0 (Android 11; Mobile; rv:89.0) Gecko/89.0 Firefox/89.0',
    'Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1'
]

# Списки имен, фамилий, отчеств и стран СНГ

# 10 мужских имен (можно использовать для всех)
names = [
    "Александр",
    "Дмитрий", 
    "Максим",
    "Сергей",
    "Андрей",
    "Алексей",
    "Иван",
    "Михаил",
    "Кирилл",
    "Никита"
]

# 10 фамилий
surnames = [
    "Иванов",
    "Петров",
    "Сидоров",
    "Смирнов",
    "Кузнецов",
    "Попов",
    "Васильев",
    "Павлов",
    "Семенов",
    "Голубев"
]

# 10 отчеств
patronymics = [
    "Александрович",
    "Дмитриевич",
    "Сергеевич",
    "Андреевич",
    "Алексеевич",
    "Иванович",
    "Михайлович",
    "Владимирович",
    "Олегович",
    "Юрьевич"
]

# 4 страны СНГ
cis_countries = [
    "Россия",
    "Казахстан",
    "Беларусь",
    "Узбекистан"
]

def gen_payload():
    NON_TARGET_CLICK_PROB = 0.1

    payload = None
    if (random.random() <= NON_TARGET_CLICK_PROB):
        x = random.randint(0, 1920)
        y = random.randint(0, 1080)
        event = "occasional_click"
        element_name = "body"
        for elem in elements:
            if x >= elem["x-min"] and x <= elem["x-max"] and y >= elem["y-min"] and y <= elem["y-max"]:
                event = elem["event"]
                element_name = elem["name"]

        payload = Payload(event,
                          element_name,
                          x,
                          y)
    else:
        element = random.choice(elements)
        x = random.randint(element["x-min"], element["x-max"])
        y = random.randint(element["y-min"], element["y-max"])

        payload = Payload(element["event"],
                          element["name"],
                          x,
                          y)
    return payload

def gen_user():
    name = random.choice(names)
    surname = random.choice(surnames)
    patronymic = random.choice(patronymics)
    fio = " ".join([surname, name, patronymic])
    country = random.choice(cis_countries)
    return User(fio, country)

def gen_click_series():
    type = random.choice(type_values)
    session_id = uuid.uuid4()
    ip = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
    user = gen_user()
    url = random.choice(url_values)
    referrer = random.choice(referrer_values)
    device_type = random.choice(device_type_values)
    user_agent = random.choice(user_agent_values)

    payloads = [gen_payload() for i in range(random.randint(1, 10))]
    click_series = [Click(type,
                          session_id,
                          ip,
                          user,
                          url,
                          referrer,
                          device_type,
                          user_agent,
                          payload) for payload in payloads]

    return click_series

def gen_entity(num_entities, gen_func):
    return [gen_func() for i in range(num_entities)]

def clicks_to_csv(clicks_batch, filename):
    with open(filename, mode='w') as f:
        writer = csv.writer(f, delimiter=',')
        for batch in clicks_batch:
            for c in batch:
                #writer.writerow([
                #    c.type, c.session_id, c.created_at, c.ip, c.user.fio, c.user.country, c.url, c.referrer,
                #    c.device_type, c.user_agent, c.payload.event_title,
                #    c.payload.element_id, c.payload.x, c.payload.y
                #])
                writer.writerow([
                    c.type, c.session_id
                ])

clicks_to_csv(gen_entity(20, gen_click_series), 'test2.csv')
#TODO http sender and broker sender