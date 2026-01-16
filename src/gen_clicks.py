import json
import random
import time
import uuid
import socket
import struct
import csv
from kafka import KafkaProducer
import json


class Event:
    def __init__(self,
                 type,
                 created_at,
                 session_id,
                 ip,
                 user,
                 url,
                 referrer,
                 device_type,
                 user_agent,
                 payload):
        self.type = type
        self.created_at = created_at
        self.session_id = session_id
        self.ip = ip
        self.user = user
        self.url = url
        self.referrer = referrer
        self.device_type = device_type
        self.user_agent = user_agent
        self.payload = payload

    def print(self):
        print("=" * 50)
        print("EVENT INFORMATION")
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
            print("no print method")

    def to_dict(self):
        return {
            "event_type": self.type,
            "created_at": self.created_at,
            "session_id": str(self.session_id),
            "ip": self.ip,
            "user": self.user.to_dict(),
            "url": self.url,
            "referrer": self.referrer,
            "device_type": self.device_type,
            "user_agent": self.user_agent,
            "payload": self.payload.to_dict()
        }

class Payload:
    def __init__(self,
                 payload_type,
                 event_title,
                 element_id,
                 x,
                 y):
        self.payload_type = payload_type
        self.event_title = event_title
        self.element_id = element_id
        self.x = x
        self.y = y

    def print(self):
        print("  Event Title: {}".format(self.event_title))
        print("  Element ID: {}".format(self.element_id))
        print("  Coordinates: x={}, y={}".format(self.x, self.y))

    def to_dict(self):
        return {
            "event_title": self.event_title,
            "element_id": self.element_id,
            "x": self.x,
            "y": self.y
        }


class User:
    def __init__(self,
                 id,
                 fio,
                 country):
        self.id = id
        self.fio = fio
        self.country = country

    def print(self):
        print("=" * 30)
        print("USER INFORMATION")
        print("=" * 30)
        print(f"ID: {self.id}")
        print(f"Name: {self.fio}")
        print(f"Country: {self.country}")
        print("=" * 30)

    def to_dict(self):
        return {
            "id": str(self.id),
            "fio": self.fio,
            "country": self.country
        }

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

# url - 10 рандомных URL
url_values = [
    'https://asobd.com/products/smartphone',
    'https://asobd.com/articles/tech-news',
    'https://asobd.com/profile/user123',
    'https://asobd.com/watch/abc123',
    'https://asobd.com/topic/python-help',
    'https://asobd.com/how-to-code',
    'https://asobd.com/games/best-sellers',
    'https://asobd.com/tours/europe',
    'https://asobd.com/courses/data-science',
    'https://asobd.com/accounts/overview'
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
device_type_values = ['desktop', 'mobile', 'tablet', 'TV', 'microwave oven', 'credit card', 'calculator']

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


def gen_occasional_click_payload():
    x = random.randint(0, 1920)
    y = random.randint(0, 1080)
    event = "occasional_click"
    element_name = "body"
    for elem in elements:
        if x >= elem["x-min"] and x <= elem["x-max"] and y >= elem["y-min"] and y <= elem["y-max"]:
            event = elem["event"]
            element_name = elem["name"]
    return Payload('click', event, element_name, x, y)

def gen_click_payload():
    element = random.choice(elements)
    x = random.randint(element["x-min"], element["x-max"])
    y = random.randint(element["y-min"], element["y-max"])
    return Payload('click', element["event"], element["name"], x, y)

def gen_view_payload():
    element = random.choice(elements)
    return Payload('view', element["event"], element["name"], 0, 0)

def gen_payload():
    CLICK_PROB = 0.02
    NON_TARGET_CLICK_PROB = 0.1

    payload = None
    if random.random() <= CLICK_PROB:
        if random.random() <= NON_TARGET_CLICK_PROB:
            payload = gen_occasional_click_payload()
        else:
            payload = gen_click_payload()
    else:
        payload = gen_view_payload()

    return payload

def gen_user():
    id = uuid.uuid4()
    name = random.choice(names)
    surname = random.choice(surnames)
    patronymic = random.choice(patronymics)
    fio = " ".join([surname, name, patronymic])
    country = random.choice(cis_countries)
    return User(id, fio, country)

def gen_session_interval():
    now = time.time()
    half_year_ago = now - 30 * 24 * 3600 * 6
    session_duration_min = 0
    session_duration_max = 60 * 60

    session_start = random.uniform(half_year_ago, now)
    session_duration = random.uniform(session_duration_min, session_duration_max)
    session_end = random.uniform(session_start, session_start + session_duration)
    return session_start, session_end


def gen_event_series():
    num_sessions = random.randint(1, 3)
    num_events = random.randint(1,3)

    events = []
    user = gen_user()

    for j in range(num_sessions):
        session_id = uuid.uuid4()
        ip = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
        device_type = random.choice(device_type_values)
        user_agent = random.choice(user_agent_values)
        session_start, session_end = gen_session_interval()

        for i in range(num_events):
            url = random.choice(url_values)
            referrer = random.choice(referrer_values)

            created_at = int(random.uniform(session_start, session_end))
            payload = gen_payload()
            event_type = payload.payload_type

            event = Event(event_type,
                    created_at,
                    session_id,
                    ip,
                    user,
                    url,
                    referrer,
                    device_type,
                    user_agent,
                    payload)

            events.append(event)
            events = add_errors(events)
    return events

def add_random_null(event):
    required_event_props = ['created_at', 'session_id', 'url', 'device_type']
    required_user_props = ['id']
    required_payload_props = ['event_title', 'element_id']

    rand = random.random()
    if rand <= 0.33:
        random_property = random.choice(required_event_props)
        setattr(event, random_property, None)
    elif rand <= 0.66:
        random_property = random.choice(required_user_props)
        setattr(event.user, random_property, None)
    else:
        random_property = random.choice(required_payload_props)
        setattr(event.payload, random_property, None)
    return event


def add_errors(events):
    DUPLICATE_PROB = 0.05
    NULL_PROB = 0.03

    ind = 0
    while ind < len(events):

        if random.random() <= DUPLICATE_PROB:
            events.insert(ind, events[ind])
            ind += 2
            continue

        if random.random() <= NULL_PROB:
            events[ind] = add_random_null(events[ind])

        ind += 1
    return events


def gen_entity(num_entities, gen_func):
    return [gen_func() for i in range(num_entities)]

def clicks_to_csv(clicks_batch, filename):
    with open(filename, mode='w') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow([
            'event_type',
            'session_id',
            'created_at',
            'ip',
            'user_id',
            'fio',
            'country',
            'url',
            'referrer',
            'device_type',
            'user_agent',
            'event_title',
            'element_id',
            'x',
            'y'
        ])
        for batch in clicks_batch:
            for c in batch:
                writer.writerow([
                    c.type, c.session_id, c.created_at, c.ip, c.user.id, c.user.fio, c.user.country, c.url, c.referrer,
                    c.device_type, c.user_agent, c.payload.event_title,
                    c.payload.element_id, c.payload.x, c.payload.y
                ])

def clicks_to_json(clicks_batch, filename):
    with open(filename, mode='w') as f:
        for b in clicks_batch:
            json.dump([c.to_dict() for c in b], f)


def clicks_to_kafka(clicks):
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for b in clicks:
        for c in b:
            producer.send('events-topic', c.to_dict())
    producer.flush()

clicks_to_csv(gen_entity(200000, gen_event_series), 'test.csv')
#clicks_to_json(gen_entity(10000, gen_event_series), 'test.json')
#clicks_to_kafka(gen_entity(100, gen_event_series))
