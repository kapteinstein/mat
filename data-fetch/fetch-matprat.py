from bs4 import BeautifulSoup
import requests
import sqlite3
import datetime
import time
import random
import json


def init_db(db):
    cursor = db.cursor()
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS recipe(
        id INTEGER PRIMARY KEY,
        url TEXT UNIQUE,
        last_modified DATETIME,
        last_fetched DATETIME,
        document_id TEXT,
        data TEXT
        );"""
    )
    db.commit()


def update_urls(db):
    """Fetch the latest sitemap from matprat.no and update database"""
    url = "https://matprat.no/sitemap.xml"
    cursor = db.cursor()
    current_urls = cursor.execute("SELECT url, last_modified FROM recipe").fetchall()
    current_urls = set(current_urls)
    response = requests.get(url)
    soup = BeautifulSoup(response.content, features="xml")
    urls = set()
    for item in soup.find_all("url"):
        url = item.loc.text
        if not url.startswith("https://www.matprat.no/oppskrifter/"):
            continue
        last_modified = str(datetime.datetime.fromisoformat(item.lastmod.text))
        urls.add((url, last_modified))

    print(current_urls - urls)

    # remove dead urls
    counter = 0
    for url, _ in current_urls - urls:
        counter += 1
        cursor.execute("DELETE FROM recipe WHERE url = (?)", (url,))
    print(f"deleted: {counter} urls")

    # add new urls or update last modified field on existing urls
    counter = 0
    for url, last_modified in urls - current_urls:
        counter += 1
        sql = """INSERT INTO recipe(url, last_modified, last_fetched) VALUES(:url, :last_modified, :last_fetched)
            ON CONFLICT(url) DO UPDATE SET last_modified=:last_modified WHERE url=:url"""
        cursor.execute(
            sql,
            {
                "url": url,
                "last_modified": last_modified,
                "last_fetched": datetime.datetime.fromtimestamp(0),
            },
        )
    print(f"upsert: {counter} urls")
    db.commit()


def fetch_data(db, delay=1):
    """fetch data from the given urls in the database and store the collectec recipe"""
    cursor = db.cursor()
    stale_urls = cursor.execute(
        "SELECT id, url, last_modified, last_fetched FROM recipe WHERE last_fetched < last_modified"
    ).fetchall()
    for id, url, last_modified, last_fetched in stale_urls:
        print(f"{id}: {url}")
        time.sleep(delay + random.random())
        response = requests.get(url)
        if response.status_code != 200:
            print(f"warning: response status code = {response.status_code}")
            continue
        soup = BeautifulSoup(response.content, "html.parser")
        item = soup.find("script", {"type": "application/ld+json"})
        text = item.text.strip()
        text = text.replace("&#160;", "")
        data = json.loads(text)

        document_id = soup.find(id="planner-from-recipe-root")
        if document_id == None:
            print("warning: no document_id")
            continue
        document_id = document_id.attrs["data-recipe-id"]
        response_2 = requests.get(
            f"https://www.matprat.no/api/WeeklyMenuPlanner/GetRecipe?id={document_id}"
        )
        if response_2.status_code != 200:
            print(f"warning: response status code = {response_2.status_code}")
            continue
        data_2 = json.loads(response_2.content)
        data["ingredients"] = data_2.get("ingredients")
        data["linkUrl"] = data_2.get("linkUrl")
        data["difficulty"] = data_2.get("difficulty")
        data["preparationTime"] = data_2.get("preparationTime")

        response_3 = requests.get(
            f"https://www.matprat.no/api/RecipeInspirations/Get?recipeId={document_id}"
        )
        if response_3.status_code != 200:
            print(f"warning: response status code = {response_3.status_code}")
            continue
        if len(response_3.content) > 0:
            data_3 = json.loads(response_3.content)
        else:
            data_3 = {}
        data["recipeCategories"] = data_3.get("recipeCategories")
        data["recipeSubCategories"] = data_3.get("recipeSubCategories")
        data["recipeCommodities"] = data_3.get("recipeCommodities")
        data["recipeFoodTypes"] = data_3.get("recipeFoodTypes")
        data["recipeFoodSubTypes"] = data_3.get("recipeFoodSubTypes")

        cursor.execute(
            "UPDATE recipe SET data=?, last_fetched=?, document_id=?  WHERE id=?",
            (json.dumps(data), datetime.datetime.now(), document_id, id),
        )
        db.commit()


def dump_data(db):
    """Dump all data into a json file for easier access"""
    cursor = db.cursor()
    data = cursor.execute("SELECT data FROM recipe WHERE data IS NOT NULL").fetchall()
    data = [json.loads(elem[0]) for elem in data]
    with open("data.json", "w") as f:
        json.dump(data, f, indent=4)


def main():
    db = sqlite3.connect("recipe.sqlite")
    init_db(db)
    update_urls(db)
    fetch_data(db, delay=0)  # the delay should probably be set by an argument
    dump_data(db)
    db.close()


if __name__ == "__main__":
    main()
