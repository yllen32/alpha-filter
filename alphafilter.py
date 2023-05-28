import sqlite3

TABLE_NAME = 'ads'
DATABASE_NAME = 'ads.db'


def filter_ads(
        urls: list[str],
        category: str = 'default',
        obsolescence: int = 45
        ) -> tuple[list[str], list[str]]:
    """Сравнить новые объявления с существующими, удалить из списка те которые
    уже есть в бд. Вернуть список новых и устаревших урлов"""
    if not is_table_exists():
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


def mark_as_processed(urls: list[str]) -> None:
    """Пометить урлы флагом 1 как обработанные."""
    cur, con = _get_cursor()
    for url in urls:
        cur.execute(
            f"""
                UPDATE {TABLE_NAME}
                SET is_processed = 1
                WHERE url = ?;
            """, (url,),
        )
    con.commit()
    print(f'Отмечено {len(urls)} как is_processed')
    cur.close()
    con.close()


def is_processed(url: str) -> bool:
    """Проверить помечено ли объявление как is_processed в базе данных"""
    cur, con = _get_cursor()
    cur.execute(f"SELECT is_processed FROM {TABLE_NAME} WHERE url = ?", (url,))
    result = cur.fetchone()
    cur.close()
    con.close()
    if not result:
        return False
    return result[0] == 1


def _save_ads_in_db(urls, category, cur, con) -> None:
    """Добавить спарсенные объявления(их урл) в локальную бд."""
    urls_to_insert = _serialize_urls_for_sqlite(urls, category)
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


def _get_cursor() -> tuple[sqlite3.Cursor]:
    """Подлючится к бд."""
    con = sqlite3.connect('ads.db')
    cur = con.cursor()
    return cur, con


def _create_table() -> None:
    """Создать таблицу с урл объявлений, если её еще не существует, создать курсор и подключение"""
    cur, _ = _get_cursor()
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME}(
        url TEXT PRIMARY KEY,
        category TEXT,
        date TEXT default CURRENT_DATE,
        is_processed INTEGER DEFAULT 0
    )""")
    print(f'Создана таблица {TABLE_NAME}')


def _drop_table() -> None:
    """Очистить таблицу"""
    cur, con = _get_cursor()
    cur.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    con.commit()
    print(f'Очищена таблица {TABLE_NAME}')


def is_table_exists(database_name: str = DATABASE_NAME, table_name: str = TABLE_NAME) -> bool:
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    if (table_name,) in tables:
        cursor.close()
        conn.close()
        return True
    else:
        cursor.close()
        conn.close()
        return False


def _serialize_urls_for_sqlite(urls: str, category: str) -> str:
    """Преобразовать урлы в строку пригодную для выполнения."""
    return str(tuple([(url, category) for url in urls]))[1:-1]


def __test_filter():
    """Тестирование основного функционала"""
    print('Начинается тестирование')
    global TABLE_NAME
    name_storage = TABLE_NAME
    TABLE_NAME = 'test_database'
    try:
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

        mark_as_processed(urls5)
        cur, con = _get_cursor()
        for url in urls5:
            cur.execute(f"SELECT is_processed FROM {TABLE_NAME} WHERE url = ?", (url,))
            result = cur.fetchone()
            assert result[0] == 1 
        assert is_processed(urls5[0]) == True
        assert is_processed(urls5[1]) == True
        assert is_processed(urls2[0]) == False
        assert is_processed("https://www.example1.com/no_exists") == False
        cur.close()
        con.close()

        print('Тестирование создания и фильтрации прошло успешно!')
        _drop_table()
    finally:
        TABLE_NAME = name_storage

if __name__ == '__main__':
    from sys import argv
    try:
        _, arg = argv
        if arg.lower() == 'test': __test_filter()
    except ValueError:
        _drop_table()
