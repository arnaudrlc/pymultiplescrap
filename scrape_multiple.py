# -*- coding: utf-8 -*-
"""
Created on Sun Mar 06 17:04:23 2022

@author: Arnaud Ralec

Scrapes data from multiple pages.
"""

import time
import requests
import pandas as pd
from bs4 import BeautifulSoup


BASE_URL = "https://www.marktplaats.nl/l/fietsen-en-brommers/fietsen-heren-herenfietsen/p/"


def scrape(url: str) -> pd.DataFrame:
    """ Returns a Pandas DataFrame object from given URL.

    Args:
        url (str): URL from which to scrape data.

    Returns:
        pd.DataFrame: Data extracted from given URL.
    """
    html = BeautifulSoup(requests.get(url).content.decode('utf-8'), "html.parser")
    data = {
        "link": [a.text.strip() for a in html.find_all("a", class_="mp-Listing-coverLink")],
        "bike_name": [h3.text.strip() for h3 in html.find_all("h3", class_="mp-Listing-title")],
        "price": [span.text.strip() for span in html.find_all("span", class_="mp-Listing-price mp-text-price-label")],
        "condition": [div.text.strip() for div in html.find_all("div", class_="mp-Listing-attributes")]
    }

    return pd.DataFrame(data)


def scrape_multiple(base_url: str, page_count: int, timeout: int = 5) -> pd.DataFrame:
    """Scrapes multiple pages from given URL and returns a Pandas Dataframe object.

    Args:
        base_url (str): URL from which to scrape data.
        page_count (int): Total amount of pages to be scraped.
        timeout (int, optional): Waiting time, given in seconds, in between requests. Defaults to 5.

    Returns:
        pd.DataFrame: Data extracted from given URL.
    """
    df = pd.DataFrame()
    for i in range(page_count):
        url = f"{base_url}{i}/"
        _ = scrape(url)
        df = pd.concat([df, _], ignore_index=True)
        time.sleep(timeout)

    return df


if __name__ == "__main__":
    scrape_multiple(BASE_URL, 3).to_csv("data.csv")
