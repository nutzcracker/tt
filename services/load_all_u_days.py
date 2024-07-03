import asyncio
from datetime import datetime, timedelta
import requests
from sqlalchemy import select
import logging
import sys

from db.models import Url, Metrics
from db.session import async_session
from api.actions.urls import _add_new_urls
from api.actions.metrics_url import _add_new_metrics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ACCESS_TOKEN = "y0_AgAEA7qkeLqBAAuw7AAAAAEDGzynAABr7pqZPg9NEb5O0OacK2wWzfFG2A"
USER_ID = "1130000065018497"
HOST_ID = "https:dn.ru:443"

date_format = "%Y-%m-%d"

# Формируем URL для запроса мониторинга поисковых запросов
URL = f"https://api.webmaster.yandex.net/v4/user/{USER_ID}/hosts/{HOST_ID}/query-analytics/list"

# Получаем количество дней из аргументов командной строки или используем значение по умолчанию
n_days = int(sys.argv[1]) if len(sys.argv) > 1 else 5
start_date = (datetime.now() - timedelta(days=n_days)).strftime(date_format)

async def add_data(data):
    async with async_session() as session:
        for query in data['text_indicator_to_statistics']:
            query_name = query['text_indicator']['value']

            # Check if the URL already exists
            stmt = select(Url).where(Url.url == query_name)
            result = await session.execute(stmt)
            existing_url = result.scalars().first()

            if not existing_url:
                new_url = [Url(url=query_name)]
                await _add_new_urls(new_url, async_session)

            metrics = []
            for el in query['statistics']:
                date = el["date"]
                if date < start_date:
                    continue

                stmt = select(Metrics).where(
                    Metrics.url == query_name,
                    Metrics.date == datetime.strptime(date, date_format)
                )
                result = await session.execute(stmt)
                existing_metric = result.scalars().first()

                if not existing_metric:
                    data_add = {
                        "date": date,
                        "ctr": 0,
                        "position": 0,
                        "impression": 0,
                        "demand": 0,
                        "clicks": 0,
                    }

                    field = el["field"]
                    if field == "IMPRESSIONS":
                        data_add["impression"] = el["value"]
                    elif field == "CLICKS":
                        data_add["clicks"] = el["value"]
                    elif field == "DEMAND":
                        data_add["demand"] = el["value"]
                    elif field == "CTR":
                        data_add["ctr"] = el["value"]
                    elif field == "POSITION":
                        data_add["position"] = el["value"]

                    metrics.append(Metrics(
                        url=query_name,
                        date=datetime.strptime(date, date_format),
                        ctr=data_add['ctr'],
                        position=data_add['position'],
                        impression=data_add['impression'],
                        demand=data_add['demand'],
                        clicks=data_add['clicks']
                    ))

            await _add_new_metrics(metrics, async_session)


async def get_data_by_page(page):
    body = {
        "offset": page,
        "limit": 500,
        "device_type_indicator": "ALL",
        "text_indicator": "URL",
        "region_ids": [],
        "filters": {}
    }

    for attempt in range(5):  # Попробовать до 5 раз
        try:
            response = requests.post(URL, json=body, headers={'Authorization': f'OAuth {ACCESS_TOKEN}',
                                                              "Content-Type": "application/json; charset=UTF-8"})
            response.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            logger.error(f"Error: {e}, retrying in {5 * (attempt + 1)} seconds...")
            time.sleep(5 * (attempt + 1))
    else:
        raise Exception("Max retries exceeded")

    logger.info(response.text[:100])
    data = response.json()

    await add_data(data)


async def get_all_data():
    body = {
        "offset": 0,
        "limit": 500,
        "device_type_indicator": "ALL",
        "text_indicator": "URL",
        "region_ids": [],
        "filters": {}
    }

    for attempt in range(5):  # Попробовать до 5 раз
        try:
            response = requests.post(URL, json=body, headers={'Authorization': f'OAuth {ACCESS_TOKEN}',
                                                              "Content-Type": "application/json; charset=UTF-8"})
            response.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            logger.error(f"Error: {e}, retrying in {5 * (attempt + 1)} seconds...")
            time.sleep(5 * (attempt + 1))
    else:
        raise Exception("Max retries exceeded")

    logger.info(response.text[:100])
    data = response.json()
    logger.info(response.text)
    count = data["count"]
    await add_data(data)
    for offset in range(500, count, 500):
        logger.info(f"[INFO] PAGE {offset} DONE!")
        await get_data_by_page(offset)


if __name__ == '__main__':
    asyncio.run(get_all_data())
