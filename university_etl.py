import requests
import pandas as pd
from sqlalchemy import create_engine


def extract():
    API_URL = "http://universities.hipolabs.com/search?country=United+States"
    data = requests.get(API_URL).json()
    return data


def transform(data):
    df = pd.DataFrame(data)
    print(f"Total number of universities from API {len(data)}")
    df = df[df["name"].str.contains("Washington")]
    print(f"Number of universities in Washington {len(df)}")
    df["web_pages"] = [",".join(map(str, l)) for l in df["web_pages"]]
    df = df.reset_index(drop=True)
    return df[["country", "name", "web_pages"]]


def load(df: pd.DataFrame):
    disk_engine = create_engine("sqlite:///univ_store.db")
    df.to_sql("universities", disk_engine, if_exists="replace")


data = extract()
df = transform(data)
load(df)
