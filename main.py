
import time
import aiohttp
import asyncio
from more_itertools import chunked
from models import CharacterModel, get_async_session


URL = 'https://swapi.dev/api/'
CATEGORIES = {
    'starships': 36,
    'vehicles': 39,
    'species': 37,
    'planets': 60,
    'films': 6
    }
COUNT_PEOPLE = 83
COUNT_STARSHIPS = 36
COUNT_FILMS = 6
COUNT_SPECIES = 37
COUNT_VEHICLES = 39
COUNT_PLANETS = 0
PARTITION = 10


class HTTPError(Exception):
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message


def _get_count_pages(count: int = None):
    """
    :param count: int (if get_characters()) | None (if get_categories())
    :return: int (if get_characters()) | dict (if get_categories())
    """
    if count:
        pages = count // 10 if count % 10 == 0 else count // 10 + 1
        return pages
    cats = {}
    for cat, count in CATEGORIES.items():
        pages = count // 10 if count % 10 == 0 else count // 10 + 1
        cats[cat] = pages
    return cats


def _unpack_json_data(json_data, url) -> dict:
    results = json_data.get('results')
    if not results:
        raise HTTPError(404, f'empty page results {url}')
    new_dict = {}
    for unit in json_data['results']:
        if 'films' in url:
            unit_dict = {unit['url']: unit['title']}
        else:
            unit_dict = {unit['url']: unit['name']}
        new_dict.update(unit_dict)
    return new_dict


def _unpacked_list(lst) -> dict:
    if lst:
        new_dict = {}
        for item in lst:
            for dct in item:
                new_dict.update(dct)
    else:
        raise HTTPError(404, "something wrong, can't unpack categories info")
    return new_dict


async def get_page_results(session, url, page: int) -> dict:
    url_page = f'{url}/?page={page}'
    async with session.get(url_page) as response:
        try:
            json_data = await response.json()
        except BaseException:
            raise HTTPError(404, f'bad request {url_page}')
        return _unpack_json_data(json_data, url_page)


async def get_category(category, session, pages):
    url = f'{URL}{category}'
    pages_res = [
        await get_page_results(session, url, page) for page in range(1, pages + 1)
    ]
    return pages_res


async def get_categories(session):
    count_pages = _get_count_pages()
    tasks = []
    for cat, pages in count_pages.items():
        new_task = asyncio.create_task(get_category(cat, session, pages))
        tasks.append(new_task)
    special_info_list = await asyncio.gather(*tasks)
    return _unpacked_list(special_info_list)


async def prepare_to_db(json_data, categories_info):
    if not json_data or not categories_info:
        raise HTTPError(404, "can't prepare to db")
    try:
        json_data.pop('created')
    except KeyError:
        return
    json_data.pop('edited')
    id = json_data.pop('url').split('/')[-2]
    json_data['id_in_swapi'] = int(id)
    for key, val in json_data.items():
        if key == 'homeworld':
            json_data[key] = categories_info[val]
        if isinstance(val, list):
            info_string = ','.join([categories_info[url] for url in val])
            json_data[key] = info_string
    return json_data


async def write_to_db(data, db_session):
    if data:
        await CharacterModel.create(data, db_session)


async def get_character(id: int, session, db_session, cats_info):
    url = f'{URL}/people/{id}/'
    async with session.get(url) as response:
        json_data = await response.json()
        character_data = await prepare_to_db(json_data, cats_info)
        task = asyncio.create_task(write_to_db(character_data, db_session))
        await task
        return True


async def get_characters(count, session, db_session, cats_info, partition=PARTITION):
    all_ids = range(1, count + 1)
    for chunked_ids in chunked(all_ids, partition):
        coros = [
            get_character(id, session, db_session, cats_info)
            for id in chunked_ids if id != 17
        ]
        await asyncio.gather(*coros)


async def main():
    async with aiohttp.ClientSession() as session:
        db_session = await get_async_session(True, True)
        cats_info_dict = await get_categories(session)
        await get_characters(COUNT_PEOPLE, session, db_session, cats_info_dict)


if __name__ == '__main__':
    start = time.time()
    asyncio.run(main())
    print(time.time() - start)
