import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id: str, client_id: str, seller_token: str) -> dict:
    """
    Получить список товаров магазина Ozon.

    Args:
        last_id (str): ID последнего полученного товара. Передайте пустую строку для начала загрузки с первого товара.
        client_id (str): ID клиента для продавца Ozon.
        seller_token (str): Токен API продавца Ozon.

    Returns:
        dict: Словарь, содержащий информацию о товарах, таких как items, total и last_id.

    Examples:
        >>> get_product_list("", "my_client_id", "my_seller_token")
        {'items': [...], 'total': 200, 'last_id': 'xyz'}

    Raises:
        >>> get_product_list(123, "my_client_id", "my_seller_token")
        TypeError: 'last_id' должен быть строкой.
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id: str, seller_token: str) -> list:
    """
    Получить артикулы товаров магазина Ozon.

    Args:
        client_id (str): ID клиента для продавца Ozon.
        seller_token (str): Токен API продавца Ozon.

    Returns:
        list: Список артикулов товаров.

    Examples:
        >>> get_offer_ids("my_client_id", "my_seller_token")
        ['offer_id_1', 'offer_id_2', ...]

    Raises:
        >>> get_offer_ids("", "")
        ValueError: Должны быть переданы Client ID и seller token.
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = [product.get("offer_id") for product in product_list]
    return offer_ids


def update_price(prices: list, client_id: str, seller_token: str) -> dict:
    """
    Обновить цены товаров в магазине Ozon.

    Args:
        prices (list): Список словарей с данными о ценах.
        client_id (str): ID клиента для продавца Ozon.
        seller_token (str): Токен API продавца Ozon.

    Returns:
        dict: Словарь с ответом от API.

    Examples:
        >>> update_price([{'offer_id': '123', 'price': '5990'}], "my_client_id", "my_seller_token")
        {'result': 'success'}

    Raises:
        >>> update_price([], "my_client_id", "my_seller_token")
        ValueError: Не указаны цены для обновления.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id: str, seller_token: str) -> dict:
    """
    Обновить остатки товаров в магазине Ozon.

    Args:
        stocks (list): Список словарей с данными об остатках.
        client_id (str): ID клиента для продавца Ozon.
        seller_token (str): Токен API продавца Ozon.

    Returns:
        dict: Словарь с ответом от API.

    Examples:
        >>> update_stocks([{'offer_id': '123', 'stock': 100}], "my_client_id", "my_seller_token")
        {'result': 'success'}

    Raises:
        >>> update_stocks([], "my_client_id", "my_seller_token")
        ValueError: Не указаны остатки для обновления.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock() -> list:
    """
    Скачать и распарсить файл с остатками с сайта Casio.

    Returns:
        list: Список данных об остатках часов в формате словаря.

    Examples:
        >>> download_stock()
        [{'Код': '123', 'Количество': '10', 'Цена': '5990'}, ...]

    Raises:
        >>> download_stock()
        FileNotFoundError: Не удалось скачать файл.
    """
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants: list, offer_ids: list) -> list:
    """
    Создать список для обновления остатков на основе данных об остатках и артикулов.

    Args:
        watch_remnants (list): Список данных об остатках часов.
        offer_ids (list): Список артикулов для сопоставления с остатками.

    Returns:
        list: Список словарей с offer_id и остатками товаров.

    Examples:
        >>> create_stocks([{'Код': '123', 'Количество': '10'}], ['123'])
        [{'offer_id': '123', 'stock': 10}]

    Raises:
        >>> create_stocks([], [])
        []
    """
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants: list, offer_ids: list) -> list:
    """
    Создать список для обновления цен на основе данных об остатках и артикулов.

    Args:
        watch_remnants (list): Список данных об остатках часов.
        offer_ids (list): Список артикулов для сопоставления с остатками.

    Returns:
        list: Список словарей с offer_id, ценами и валютой.

    Examples:
        >>> create_prices([{'Код': '123', 'Цена': '5990'}], ['123'])
        [{'offer_id': '123', 'price': '5990'}]

    Raises:
        >>> create_prices([], [])
        []
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """
    Преобразует строку с ценой в формате, удобном для использования в системе.

    Эта функция извлекает числовое значение из строки, удаляет все нецифровые символы и возвращает число в строковом формате.

    Args:
        price (str): Цена в строковом формате, например, "5'990.00 руб."

    Returns:
        str: Цена в числовом формате без разделителей и валютных символов, например, "5990".

    Examples:
        >>> price_conversion("5'990.00 руб.")
        '5990'
        >>> price_conversion("1.250,50 USD")
        '1250'
        >>> price_conversion("€ 1234.56")
        '1234'

    Raises:
        ValueError: Если входная строка не содержит цифр, функция может вызвать ошибку при попытке преобразования.
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """
    Разделить список на части по n элементов.

    Args:
    lst (list): Исходный список.
    n (int): Количество элементов в каждой части.

    Returns:
    generator: Части списка.

    Examples:
    >>> list(divide([1, 2, 3, 4], 2))
    [[1, 2], [3, 4]]

    Raises:
    - n больше длины списка.
    """


async def upload_prices(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
