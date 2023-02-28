import sqlite3

from loguru import logger


TABLE_NAME = 'ads'


def filter_urls(urls, category) -> list[dict]:
    """Сравнить новые объявления с существующими, удалить из списка те которые
    уже есть в бд."""
    _create_table()
    new_urls = [url['web_url'] for url in urls]
    cur, con = _get_cursor()
    res = cur.execute(f"""SELECT url FROM {TABLE_NAME} WHERE category="{category}" AND date > DATETIME('now', '-45 day')""")
    db_urls = [url[0] for url in res.fetchall()]
    new_urls_set, db_urls_set = set(new_urls), set(db_urls)
    urls_to_add = new_urls_set - db_urls_set
    depricated_url = db_urls_set - new_urls_set
    if urls_to_add:
        _save_ads_in_db(urls_to_add, category, cur, con)
    if depricated_url:
        _delete_depricated(depricated_url, cur, con)
    new_urls = [url for url in urls if url['web_url'] in urls_to_add]
    logger.info(f'Осуществлена фильтрация объявлений, количество к парсингу = {len(new_urls)}, было = {len(urls)}')
    con.close()
    return new_urls, depricated_url


def _save_ads_in_db(urls, category, cur, con) -> None:
    """Добавить спарсенные объявления(их урл) в локальную бд."""
    urls_to_insert = str(tuple([(url, category) for url in urls]))[1:-1]
    cur.execute(f"""INSERT OR IGNORE INTO {TABLE_NAME} (url, category) VALUES {urls_to_insert}""")
    con.commit()
    logger.info(f'Сохранено {len(urls)} объявлений в базу данных')

def _delete_depricated(depricated_url, cur, con) -> None:
    """Удалить устаревшие объявления."""
    cur.execute(f"""DELETE FROM {TABLE_NAME} WHERE url IN {tuple(depricated_url)}""")
    con.commit()
    logger.info(f'Удалено {len(depricated_url)} объявлений в базу данных')

def _get_cursor() -> sqlite3.Cursor:
    """Подлючится к бд."""
    con = sqlite3.connect('avito_ads.db')
    cur = con.cursor()
    return cur, con


def _create_table() -> None:
    """Создать таблицу с урл объявлений, если её еще не существует, создать курсор и подключение"""
    cur, _ = _get_cursor()
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME}(
        url TEXT PRIMARY KEY, category TEXT, date TEXT default CURRENT_DATE)""")
    logger.info(f'Создана таблица {TABLE_NAME}')


def _drop_table() -> None:
    """Очистить таблицу"""
    cur, con = _get_cursor()
    cur.execute(f"DROP TABLE {TABLE_NAME}")
    con.commit()
    logger.info(f'Очищена таблица {TABLE_NAME}')


if __name__ == '__main__':
    _drop_table()
