import asyncio
from aiohttp import ClientSession
from datetime import datetime as dt
import more_itertools
from models import Persons, Session_db, Base, engine


# Максимальное количество получаемых героев (на 04.10.2022 сайт возвращает 82 значения)
PERS_COUNT = 100
# Порция для отправки ссылок
CHUNK_SIZE = 20


async def get_name(url: str, session: ClientSession, attr: str = 'name') -> str:
    """
    Получение атрибута (по умолчанию - 'name') из JSON ответа сайта (API)
    :param url: ссылка на API
    :param session: http-сессия
    :param attr: получаемый атрибут, по умолчанию 'name'
    :return: значение атрибута
    """
    async with session.get(url) as response:
        json = await response.json()
        return json[attr]


async def list_to_string(url_list: list, session: ClientSession, attr: str = 'name') -> str:
    """
    Преобразование списка ссылок в строку с разделителем ','
    :param url_list: список со ссылками на API (может быть пустым)
    :param session: http-сессия для проброса в get_name
    :param attr: атрибут для проброса в get_name
    :return: склеенные в строку значения атрибута по ссылкам API с разделителем запятая ','
    """
    results = []
    for url in url_list:
        res_name = await get_name(url, session, attr=attr)
        results.append(res_name)
    return ', '.join(results)


async def get_pers(id_pers: int, session: ClientSession) -> dict:
    """
    Получение информации по персонажу StarWars из SW-API
    :param id_pers: идентификатор персонажа
    :param session: http-сессия
    :return: словарь с характеристиками персонажа
    """
    async with session.get(f'https://swapi.dev/api/people/{id_pers}') as response:
        json = await response.json()
        if len(json.keys()) > 1:
            result = dict(json)
            # get homeworld
            result['homeworld'] = await get_name(result['homeworld'], session)
            # get species
            result['species'] = await list_to_string(result['species'], session)
            # get vehicles
            result['vehicles'] = await list_to_string(result['vehicles'], session)
            # get starships
            result['starships'] = await list_to_string(result['starships'], session)
            # get films
            result['films'] = await list_to_string(result['films'], session, attr='title')
            # set id
            result['id'] = id_pers
            # delete unused attributes
            for attr in ['created', 'edited', 'url']:
                result.pop(attr, None)
            return result


async def main():
    # Соединение с БД
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()
    # Запись в БД в сессии
    async with Session_db() as session_db:
        # Http-сессия для получения персонажей
        async with ClientSession() as session_http:
            # Генератор корутин с запросами к SW-API
            coros = (get_pers(i, session_http) for i in range(1, PERS_COUNT))
            # Получение порций запросов из генератора
            for coros_chunk in more_itertools.chunked(coros, CHUNK_SIZE):
                # Соединение порций запросов и их отправка
                result = await asyncio.gather(*coros_chunk)
                # Перебор списка ответов
                for res in result:
                    # Если персонаж существует, то вставка его в БД
                    if res:
                        session_db.add(Persons(res))
        # коммит БД
        await session_db.commit()


if __name__ == '__main__':
    # started
    start = dt.now()
    # from here https://stackoverflow.com/questions/45600579/asyncio-event-loop-is-closed-when-getting-loop
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    # entry point
    asyncio.run(main())
    # executing time
    print(dt.now() - start)
