import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """
    Получает список товаров с Яндекс Маркета для указанной страницы.

    Args:
        page (str): Токен страницы, который определяет набор товаров для загрузки.
        campaign_id (str): Идентификатор кампании Яндекс Маркета.
        access_token (str): Токен доступа для API.

    Returns:
        dict: JSON объект с результатами, включая товары и информацию о страницах.

    Examples:
    >>> get_product_list("", "123456", "your_token")
    {"result": {...}, "paging": {...}}

    Raises:
    >>> get_product_list("", "wrong_id", "invalid_token")
    requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """
    Обновляет информацию об остатках товаров на Яндекс Маркете.

    Args:
        stocks (list): Список SKU с количеством для обновления остатков.
        campaign_id (str): Идентификатор кампании Яндекс Маркета.
        access_token (str): Токен доступа для API.

    Returns:
        dict: JSON объект с результатами обновления остатков.

    Examples:
    >>> update_stocks([{"sku": "123", "items": [{"count": 10, "type": "FIT"}]}], "123456", "your_token")
    {"result": {...}}

    Raises:
    >>> update_stocks([], "wrong_id", "invalid_token")
    requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """
    Обновляет информацию о ценах товаров на Яндекс Маркете.

    Args:
        prices (list): Список товаров с новыми ценами.
        campaign_id (str): Идентификатор кампании Яндекс Маркета.
        access_token (str): Токен доступа для API.

    Returns:
        dict: JSON объект с результатами обновления цен.

    Examples:
    >>> update_price([{"id": "123", "price": {"value": 1000, "currencyId": "RUR"}}], "123456", "your_token")
    {"result": {...}}

    Raises:
    >>> update_price([], "wrong_id", "invalid_token")
    requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """
    Получает артикулы товаров с Яндекс Маркета для указанной кампании.

    Args:
        campaign_id (str): Идентификатор кампании Яндекс Маркета.
        market_token (str): Токен доступа для API.

    Returns:
        list: Список артикулов товаров (shopSku).

    Examples:
    >>> get_offer_ids("123456", "your_token")
    ["sku_1", "sku_2", ...]

    Raises:
    >>> get_offer_ids("wrong_id", "invalid_token")
    requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = [product.get("offer").get("shopSku") for product in product_list]
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """
    Создает список остатков товаров для обновления на Яндекс Маркете.

    Args:
        watch_remnants (list): Данные о остатках товаров.
        offer_ids (list): Список артикулов товаров (shopSku).
        warehouse_id (str): Идентификатор склада.

    Returns:
        list: Список остатков для обновления.

    Examples:
    >>> create_stocks([{"Код": "123", "Количество": "10"}], ["123"], "warehouse_id")
    [{"sku": "123", "items": [{"count": 10, "type": "FIT", "updatedAt": "2024-09-08T10:10:10Z"}]}]

    Raises:
    >>> create_stocks([], [], "warehouse_id")
    []
    """
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Создает список цен для обновления на Яндекс Маркете.

    Args:
        watch_remnants (list): Данные о ценах товаров.
        offer_ids (list): Список артикулов товаров (shopSku).

    Returns:
        list: Список цен для обновления.

    Examples:
    >>> create_prices([{"Код": "123", "Цена": "1000"}], ["123"])
    [{"id": "123", "price": {"value": 1000, "currencyId": "RUR"}}]

    Raises:
    >>> create_prices([], [])
    []
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    "currencyId": "RUR",
                },
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
