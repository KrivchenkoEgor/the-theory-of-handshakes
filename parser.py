import requests
import random
from bs4 import BeautifulSoup
import json
import re
import time
from urllib.parse import urlparse
from collections import defaultdict

# Глобальные настройки
SEARCH_EVERYWHERE = True  # Глубокий поиск по всем связям
INCLUDE_LIQUIDATED = False  # Включать ли ликвидированные компании (1 - да, 0 - нет)
MAX_DEPTH = 10  # Максимальная глубина рекурсивного поиска
LIQUIDATION_KEYWORDS = ['ликвидирован', 'прекращен', 'банкрот', 'исключен']

# Настройки рандомной задержки
MIN_REQUEST_DELAY = 1  # Минимальная задержка (секунды)
MAX_REQUEST_DELAY = 60  # Максимальная задержка (секунды)

# Глобальный кэш для избежания дублирования запросов
entity_cache = defaultdict(dict)
processed_entities = set()


def random_delay():
    """Генерирует случайную задержку и ожидает"""
    delay = random.randint(MIN_REQUEST_DELAY, MAX_REQUEST_DELAY)
    print(f"Ожидание {delay} секунд перед запросом...")
    time.sleep(delay)


def parse_entity(entity_id, entity_type, depth=0):
    """Рекурсивный парсинг юрлица или физлица с поиском связанных объектов"""
    if depth > MAX_DEPTH:
        return {}

    # Проверка кэша
    cache_key = f"{entity_type}_{entity_id}"
    if cache_key in processed_entities:
        return entity_cache.get(cache_key, {})

    processed_entities.add(cache_key)
    random_delay()

    if entity_type == "company":
        data = parse_company(entity_id, depth)
    elif entity_type == "person":
        data = parse_person(entity_id, depth)
    else:
        return {}

    entity_cache[cache_key] = data
    return data


def parse_company(ogrn, depth=0):
    """Парсинг компании с рекурсивным поиском связей"""
    url = f"https://checko.ru/company/tander-{ogrn}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code != 200:
            return {"error": f"Ошибка {response.status_code}", "ogrn": ogrn}

        soup = BeautifulSoup(response.text, 'lxml')
        result = {
            "ogrn": ogrn,
            "short_name": get_text(soup.find('h1', id='cn')),
            "full_name": get_text(soup.find('span', id='cfn')),
            "depth": depth
        }

        # Определение статуса компании
        status = "Действующая"
        liquidation_reason = ""

        # Поиск признаков ликвидации
        status_div = soup.find('div', class_='text-danger')
        if status_div:
            status_text = status_div.get_text(strip=True)
            if any(kw in status_text.lower() for kw in LIQUIDATION_KEYWORDS):
                status = "Ликвидирована"
                liquidation_reason = status_text

        result["status"] = status
        if liquidation_reason:
            result["liquidation_reason"] = liquidation_reason

        # Пропускаем ликвидированные компании если настроено исключение
        if not INCLUDE_LIQUIDATED and status == "Ликвидирована":
            result["message"] = "Пропущена из-за статуса ликвидации"
            return result

        # Парсинг реквизитов
        requisites = {}
        req_block = soup.select_one('div.mt-3.mb-3')
        if req_block:
            for item in req_block.find_all('div'):
                if item.find('strong', class_='fw-700'):
                    key = item.find('strong', class_='fw-700').text.strip()
                    value = item.find('strong', id=lambda x: x and x.startswith('copy-'))
                    if value:
                        requisites[key] = value.text.strip()
        result["requisites"] = requisites

        # Парсинг директоров
        directors = []
        director_container = soup.find('div', class_='flex-grow-1')
        if director_container:
            director_link = director_container.find('a', class_='link')
            if director_link:
                director_name = director_link.get_text(strip=True)
                director_url = director_link.get('href', '')

                # Поиск ИНН директора
                director_inn = None
                inn_span = director_container.find('span', id=lambda x: x and 'copy-' in x and 'inn' in x)
                if inn_span:
                    director_inn = inn_span.text.strip()
                else:
                    # Альтернативный поиск ИНН
                    inn_text = re.search(r'ИНН\s+(\d+)', director_container.get_text())
                    if inn_text:
                        director_inn = inn_text.group(1)

                director_data = {
                    "name": director_name,
                    "type": "person",
                    "inn": director_inn,
                    "url": f"https://checko.ru{director_url}" if director_url else None
                }
                directors.append(director_data)
        result["directors"] = directors

        # Парсинг учредителей
        founders = []
        seen_founder_ids = set()

        # Ищем все элементы с иконкой учредителя
        founder_icons = soup.find_all('span', class_='icon--4px')
        for icon in founder_icons:
            # Проверяем, что следующий текст содержит "Учредитель"
            next_text = icon.next_sibling
            if next_text and "Учредитель" in next_text:
                # Ищем ссылку на учредителя
                founder_link = icon.find_next('a', class_='link')
                if founder_link:
                    founder_name = founder_link.get_text(strip=True)
                    founder_url = founder_link.get('href', '')

                    if '/person/' in founder_url:
                        founder_id = founder_url.split('/')[-1]
                        founder_type = "person"
                    elif '/company/' in founder_url:
                        founder_id = founder_url.split('-')[-1]
                        founder_type = "company"
                    else:
                        founder_id = None
                        founder_type = "unknown"

                    # Проверяем дубликаты
                    if not founder_id or founder_id in seen_founder_ids:
                        continue

                    seen_founder_ids.add(founder_id)

                    founders.append({
                        "name": founder_name,
                        "type": founder_type,
                        "id": founder_id,
                        "url": f"https://checko.ru{founder_url}" if founder_url else None
                    })
        result["founders"] = founders

        # Рекурсивный поиск связей (только для действующих компаний)
        if SEARCH_EVERYWHERE and status != "Ликвидирована":
            # Для директоров
            for director in directors:
                if director.get('inn') and f"person_{director['inn']}" not in processed_entities:
                    director['related_entities'] = parse_entity(
                        director['inn'], "person", depth + 1
                    )

            # Для учредителей
            for founder in founders:
                if founder.get('id') and f"{founder['type']}_{founder['id']}" not in processed_entities:
                    founder['related_entities'] = parse_entity(
                        founder['id'], founder['type'], depth + 1
                    )

        return result

    except Exception as e:
        return {"error": str(e), "ogrn": ogrn}


def parse_person(inn, depth=0):
    """Парсинг физлица с поиском связанных компаний и их связей"""
    url = f"https://checko.ru/person/{inn}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
    }

    try:
        random_delay()
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code != 200:
            return {"error": f"Ошибка {response.status_code}", "inn": inn}

        soup = BeautifulSoup(response.text, 'lxml')
        result = {
            "inn": inn,
            "name": get_text(soup.find('title')).split(' - ')[0] if soup.find('title') else f"Физлицо {inn}",
            "depth": depth
        }

        # Парсинг ИП (если есть)
        entrepreneur_section = soup.find('section', id='entrepreneur')
        if entrepreneur_section:
            ip_link = entrepreneur_section.find('a', class_='link fw-700')
            result["entrepreneur"] = {
                "name": ip_link.get_text(strip=True) if ip_link else None,
                "ogrnip": get_text(entrepreneur_section.find('span', id=lambda x: x and 'ogrn' in x)),
                "status": get_text(entrepreneur_section.find('div', class_='check-icon').parent)
                if entrepreneur_section.find('div', class_='check-icon') else None,
                "registration_date": clean_text(
                    entrepreneur_section.find('div', string='Дата регистрации').find_next('div').text)
                if entrepreneur_section.find('div', string='Дата регистрации') else None
            }

        # Парсинг руководящих позиций
        result["leadership"] = parse_related_section(soup, 'leader')

        # Парсинг учредительских позиций
        result["ownership"] = parse_related_section(soup, 'founder')

        # Глубокий анализ связанных компаний
        if SEARCH_EVERYWHERE:
            # Анализ компаний, где лицо является руководителем
            for company in result["leadership"]:
                if "ogrn" in company:
                    # Всегда получаем полные данные компании
                    company_full_data = parse_entity(
                        company["ogrn"], "company", depth + 1
                    )
                    # Обновляем данные компании более полной информацией
                    company.update(company_full_data)

            # Анализ компаний, где лицо является учредителем
            for company in result["ownership"]:
                if "ogrn" in company:
                    # Всегда получаем полные данные компании
                    company_full_data = parse_entity(
                        company["ogrn"], "company", depth + 1
                    )
                    # Обновляем данные компании более полной информацией
                    company.update(company_full_data)

        return result

    except Exception as e:
        return {"error": str(e), "inn": inn}


def parse_related_section(soup, section_id):
    """Парсинг секций руководства и владения"""
    section = soup.find('section', id=section_id)
    if not section or section.find('p', string=lambda t: t and "не является" in t):
        return []

    companies = []
    rows = section.find_all('tr')

    for row in rows:
        if row.find('th'):
            continue

        company_data = {}
        name_link = row.find('a', class_='link fw-700')
        if name_link:
            company_data["name"] = name_link.get_text(strip=True)
            company_data["url"] = f"https://checko.ru{name_link.get('href')}" if name_link.get('href') else None

        # Извлечение ОГРН из URL
        if name_link and name_link.get('href'):
            href = name_link.get('href')
            if '/company/' in href:
                company_data["ogrn"] = href.split('-')[-1]

        # Статус компании
        status_div = row.find('div', class_='text-danger')
        if status_div:
            company_data["status"] = status_div.get_text(strip=True)
            # Проверка на ликвидацию
            if any(kw in company_data["status"].lower() for kw in LIQUIDATION_KEYWORDS):
                company_data["is_liquidated"] = True
                # Пропускаем ликвидированные если настроено исключение
                if not INCLUDE_LIQUIDATED:
                    continue
            else:
                company_data["is_liquidated"] = False
        else:
            status_div = row.find('div', class_='check-icon')
            if status_div:
                company_data["status"] = status_div.parent.get_text(strip=True)
                company_data["is_liquidated"] = False

        # Основные реквизиты
        requisites = row.find_all('span', class_='copy')
        if len(requisites) >= 3:
            company_data["ogrn"] = requisites[0].get_text(strip=True)
            company_data["inn"] = requisites[1].get_text(strip=True)
            company_data["kpp"] = requisites[2].get_text(strip=True)

        # Финансовые показатели
        finance_labels = row.find_all('div', class_='fw-700', string=True)
        for label in finance_labels:
            label_text = label.get_text(strip=True)
            next_div = label.find_next('div')
            if not next_div:
                continue

            value = next_div.get_text(strip=True)
            if 'Уставный капитал' in label_text:
                company_data["authorized_capital"] = value
            elif 'Выручка' in label_text:
                company_data["revenue"] = value
            elif 'Чистая прибыль' in label_text:
                company_data["net_profit"] = value

        companies.append(company_data)

    return companies


def clean_text(text):
    return re.sub(r'\s+', ' ', text).strip() if text else None


def get_text(element):
    return element.text.strip() if element else None


def save_results(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Результаты сохранены в {filename}")

    # Статистика результатов
    count_result = count_entities(data)
    print(f"Найдено: {count_result['companies']} компаний, {count_result['persons']} физлиц")
    print(f"Глубина связей: {MAX_DEPTH} уровней")
    print(f"Уникальных сущностей: {len(processed_entities)}")
    print(f"Включены ликвидированные: {'Да' if INCLUDE_LIQUIDATED else 'Нет'}")


def count_entities(data):
    """Рекурсивный подсчёт сущностей в результатах"""
    counters = {
        "companies": 0,
        "persons": 0
    }

    def _recursive_counter(item):
        if isinstance(item, dict):
            # Считаем компании
            if "ogrn" in item:
                counters["companies"] += 1

            # Считаем физлиц
            if "inn" in item and "type" in item and item.get("type") == "person":
                counters["persons"] += 1

            # Рекурсивно обрабатываем вложенные элементы
            for value in item.values():
                _recursive_counter(value)

        elif isinstance(item, list):
            for element in item:
                _recursive_counter(element)

    _recursive_counter(data)
    return counters


if __name__ == "__main__":
    # Настройки поиска
    SEARCH_EVERYWHERE = True  # Глубокий поиск по всем связям
    INCLUDE_LIQUIDATED = False  # Измените на True для включения ликвидированных

    # Стартовая компания (ПАО "МТС")
    start_ogrn = "1027700149124"

    # Инициализация кэшей перед запуском
    entity_cache.clear()
    processed_entities.clear()

    result = parse_entity(start_ogrn, "company")

    if "error" not in result:
        filename = f"company_network_{start_ogrn}.json"
        save_results(result, filename)
    else:
        print(f"Ошибка: {result['error']}")