import logging
import requests as rq
import env_variables as env


def extract_movies(url: str, data: str, page: int) -> tuple[list, int]:
    try:
        header: dict = {
            'Authorization': f'Bearer {env.ACCESS_TOKEN}',
            'accept': 'application/json'
        }
        params: dict = {
            'page': page,
            'sort_by': 'popularity.desc'
        }

        response = rq.request(url=url, method='GET', headers=header, params=params)

        logging.info(f"Api response code : {response.status_code}")

        if str(response.status_code).startswith('20'):
            json_response = response.json()
            results = json_response.get(data)
            total_pages = json_response.get('total_pages', 1)
            return results, total_pages
        else:
            return [], 0
    except Exception as e:
        logging.exception(f'Data extraction error: {e}')


def extract_genre(url: str, data: str):
    try:
        header: dict = {
            'Authorization': f'Bearer {env.ACCESS_TOKEN}',
            'accept': 'application/json'
        }

        response = rq.request(url=url, method='GET', headers=header)

        logging.info(f"Api response code : {response.status_code}")

        return response.json().get(data) if str(response.status_code).startswith('20') else []
    except Exception as e:
        logging.exception(f'Data extraction error: {e}')
