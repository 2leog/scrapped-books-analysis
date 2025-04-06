import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

base_url = "https://books.toscrape.com/catalogue/page-{}.html"
book_base_url = "https://books.toscrape.com/catalogue/"
page = 1

books_data = {
    "Title": [],
    "Price": [],
    "Rating": [],
    "Availability": [],
    "Category": []
}

# Function to scrape individual book detail page and get category
def get_book_info(book):
    title = book.h3.a["title"]
    price = book.select_one(".price_color").text
    rating_tag = book.select_one("p.star-rating")
    rating = rating_tag["class"][1] if rating_tag and len(rating_tag["class"]) > 1 else "Not Rated"
    availability = book.select_one(".availability").get_text(strip=True)
    relative_url = book.h3.a["href"]
    detail_url = urljoin(book_base_url, relative_url)

    # Fetch detail page
    detail_resp = requests.get(detail_url)
    detail_soup = BeautifulSoup(detail_resp.text, "html.parser")
    breadcrumb = detail_soup.select("ul.breadcrumb li a")
    category = breadcrumb[2].text if len(breadcrumb) > 2 else "Unknown"

    return {
        "Title": title,
        "Price": price,
        "Rating": rating,
        "Availability": availability,
        "Category": category
    }

# Threaded scraping loop
while True:
    url = base_url.format(page)
    response = requests.get(url)
    if response.status_code != 200:
        break

    soup = BeautifulSoup(response.text, "html.parser")
    books = soup.select(".product_pod")
    if not books:
        break

    # Scrape book details in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_book_info, book) for book in books]
        for future in as_completed(futures):
            book_info = future.result()
            for key in books_data:
                books_data[key].append(book_info[key])

    print(f"Finished page {page}")
    page += 1

# Create DataFrame
df = pd.DataFrame(books_data)

df.to_csv("books.csv", sep=',', index=False)