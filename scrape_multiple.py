# -*- coding: utf-8 -*-
"""
Created on Sun Mar 06 17:04:23 2022

@author: Arnaud Ralec

Scrapes data from multiple pages.
"""

import time
from itertools import cycle

import pandas as pd
import requests
from bs4 import BeautifulSoup
from lxml.html import fromstring

PROXIES_SOURCE = "https://free-proxy-list.net/"
BASE_URL = "https://www.marktplaats.nl/l/fietsen-en-brommers/fietsen-GENDER-GENDERfietsen/p/"


def get_proxies() -> set:
    """Returns a set of proxies.

    When sending multiple requests from the same IP, for instance when
    scrapping a website through HTTP protocol, the website may detect
    and block one's scraper.

    To avoid being blocked, several options are aviable.

    This programm chooses to rotate IP address over a proxy pool obtained
    from this function.

    Returns:
        set: proxies
    """
    res = requests.get(PROXIES_SOURCE)
    parser = fromstring(res.text)

    proxies = set()
    for el in parser.xpath("//tbody/tr"):
        # Checks if the corresponding IP address is of type HTTPS:
        if el.xpath('.//td[7][contains(text(),"yes")]'):
            proxy = ":".join(
                [
                    el.xpath(".//td[1]/text()")[0],  # IP address
                    el.xpath(".//td[2]/text()")[0],  # Port
                ]
            )
            proxies.add(proxy)

    return proxies


def scrape(url: str, **kwargs) -> pd.DataFrame:
    """Returns a Pandas DataFrame object from given URL.

    Args:
        url (str): URL from which to scrape data.

    Returns:
        pd.DataFrame: Data extracted from given URL.
    """
    html = BeautifulSoup(requests.get(url, **kwargs).content.decode("utf-8"), "html.parser")
    data = {
        "link": [a.text.strip() for a in html.find_all("a", class_="mp-Listing-coverLink")],
        "bike_name": [h3.text.strip() for h3 in html.find_all("h3", class_="mp-Listing-title")],
        "price": [span.text.strip() for span in html.find_all("span", class_="mp-Listing-price mp-text-price-label")],
        "condition": [div.text.strip() for div in html.find_all("div", class_="mp-Listing-attributes")],
    }

    return pd.DataFrame(data)


def scrape_multiple(base_url: str, page_count: int, timeout: int = 30) -> pd.DataFrame:
    """Scrapes multiple pages from given URL and returns a Pandas Dataframe object.

    Args:
        base_url (str): URL from which to scrape data.
        page_count (int): Total amount of pages to be scraped.
        timeout (int, optional): Waiting time, given in seconds, in between requests. Defaults to 5.

    Returns:
        pd.DataFrame: Data extracted from given URL.
    """
    df = pd.DataFrame()
    pool = get_proxies()
    rotating_proxies = cycle(pool)
    for i in range(page_count):
        current_proxy = next(rotating_proxies)
        url = f"{base_url}{i}/"
        _ = scrape(url, proxies={"http": current_proxy, "https": current_proxy})
        df = pd.concat([df, _], ignore_index=True)
        time.sleep(timeout)

    return df


if __name__ == "__main__":
    for g in "heren", "dames":  # Men, women
        scrape_multiple(BASE_URL.replace("GENDER", g), 100, 30).to_csv(f"{g}.csv")
