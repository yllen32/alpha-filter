import sqlite3

TABLE_NAME = 'ads'

def filter_ads(
        urls: list[str],
        category: str = 'default',
        obsolescence: int = 45
        ) -> tuple[list[str], list[str]]:
    """Сравнить новые объявления с существующими, удалить из списка те которые
    уже есть в бд. Вернуть список новых и устаревших урлов"""
    _create_table()
    new_urls = [url for url in urls]
    cur, con = _get_cursor()
    res = cur.execute(f"""SELECT url FROM {TABLE_NAME} WHERE category="{category}" AND date > DATETIME('now', '-{obsolescence} day')""")
    db_urls = [url[0] for url in res.fetchall()]
    new_urls_set, db_urls_set = set(new_urls), set(db_urls)
    urls_to_add = new_urls_set - db_urls_set
    deprecated_url = db_urls_set - new_urls_set
    if urls_to_add:
        _save_ads_in_db(urls_to_add, category, cur, con)
    if deprecated_url:
        _delete_deprecated(deprecated_url, cur, con)
    new_urls = [url for url in urls if url in urls_to_add]
    print(f'Осуществлена фильтрация объявлений, количество к парсингу = {len(new_urls)}, было = {len(urls)}')
    con.close()
    return new_urls, list(deprecated_url)


def _save_ads_in_db(urls, category, cur, con) -> None:
    """Добавить спарсенные объявления(их урл) в локальную бд."""
    urls_to_insert = str(tuple([(url, category) for url in urls]))[1:-1]
    cur.execute(f"""INSERT OR IGNORE INTO {TABLE_NAME} (url, category) VALUES {urls_to_insert}""")
    con.commit()
    print(f'Сохранено {len(urls)} объявлений в базу данных')


def _delete_deprecated(deprecated_url, cur, con) -> None:
    """Удалить устаревшие объявления."""
    if len(deprecated_url) == 1:
        q_deprecated_url = '("' + str(*deprecated_url) + '")'
    else:
        q_deprecated_url = tuple(deprecated_url)
    cur.execute(f"""DELETE FROM {TABLE_NAME} WHERE url IN {q_deprecated_url}""")
    con.commit()
    print(f'Удалено {len(deprecated_url)} объявлений в базу данных')


def _get_cursor() -> sqlite3.Cursor:
    """Подлючится к бд."""
    con = sqlite3.connect('ads.db')
    cur = con.cursor()
    return cur, con


def _create_table() -> None:
    """Создать таблицу с урл объявлений, если её еще не существует, создать курсор и подключение"""
    cur, _ = _get_cursor()
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME}(
        url TEXT PRIMARY KEY, category TEXT, date TEXT default CURRENT_DATE)""")
    print(f'Создана таблица {TABLE_NAME}')


def _drop_table() -> None:
    """Очистить таблицу"""
    cur, con = _get_cursor()
    cur.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    con.commit()
    print(f'Очищена таблица {TABLE_NAME}')


def __test_filter():
    """Тестирование основного функционала"""
    print('Начинается тестирование')
    _drop_table()
    category = 'test_category'
    urls1 = [f'https://www.example.com/{i}' for i in range(10)]

    new1, old1 = filter_ads(urls1, category)
    assert (len(new1) == 10) and (len(old1) == 0)

    new2, old2 = filter_ads(urls1, category)
    assert (len(new2) == 0) and (len(old2) == 0)

    urls2 = [f'https://www.example.com/{i}' for i in range(5, 15)]
    new3, old3 = filter_ads(urls2, category)
    assert (len(new3) == 5) and (len(old3) == 5)

    urls4 = [f'https://www.example.com/n{i}' for i in range(5, 15)]
    new4, old4 = filter_ads(urls4, 'new_category')
    assert (len(new4) == 10) and (len(old4) == 0)

    urls5 = [f'https://www.example.com/n{i}' for i in range(6, 15)]
    new5, old5 = filter_ads(urls5, 'new_category')
    assert (len(new5) == 0) and (len(old5) == 1)

    category_new = 'other'
    urls_new = ['https://www.example1.com/8', 'https://www.example1.com/9']
    new_urls, deprecated_urls = filter_ads(urls_new, category_new)
    assert new_urls == urls_new or new_urls == tuple(urls_new)
    assert deprecated_urls == []

    print('Тестирование создания и фильтрации прошло успешно!')
    _drop_table()


if __name__ == '__main__':
    from sys import argv
    try:
        _, arg = argv
        if arg.lower() == 'test': __test_filter()
    except ValueError:
        _drop_table()
