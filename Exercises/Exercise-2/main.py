import requests
from bs4 import BeautifulSoup
import pandas as pd

def extract_file(url: str, csv_link: str):
    full_url = url + csv_link
    csv_response = requests.get(full_url)

    with open("test.csv", "w") as fd:
        fd.write(csv_response.text)

def create_dataframe(file_name: str) -> pd.DataFrame:
    # read into pandas Dataframe
    with open(file_name, "r") as fd:
        df = pd.read_csv(fd, dtype=object)

    # invalid values placed in the HourlyDryBulbTemperature column
    df["HourlyDryBulbTemperature"] = pd.to_numeric(
        df["HourlyDryBulbTemperature"], errors="coerce"
    )
    max_temp = df["HourlyDryBulbTemperature"].max()
    filtered_df = df[df["HourlyDryBulbTemperature"] == max_temp]

    return filtered_df

def main():
    url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
    response = requests.get(url)

    html_doc = response.text
    soup = BeautifulSoup(html_doc, "html.parser")
    links = soup.find_all("tr")
    csv_link = None
    for link in links:
        for child in link.children:
            if child.get_text(strip=True) == "2022-02-07 14:03":
                csv_link = link.find("a").get("href")
                break
    
    extract_file(url, csv_link)
    # # check that the modified column was found and make next API call
    # assert csv_link
    # full_url = url + csv_link
    # csv_response = requests.get(full_url)

    # with open("test.csv", "w") as fd:
    #     fd.write(csv_response.text)

    # # read into pandas Dataframe
    # with open("test.csv", "r") as fd:
    #     df = pd.read_csv(fd, dtype=object)

    # # invalid values placed in the HourlyDryBulbTemperature column
    # df["HourlyDryBulbTemperature"] = pd.to_numeric(
    #     df["HourlyDryBulbTemperature"], errors="coerce"
    # )
    # max_temp = df["HourlyDryBulbTemperature"].max()
    # filtered_df = df[df["HourlyDryBulbTemperature"] == max_temp]
    filtered_df = create_dataframe('test.csv')

    print(filtered_df)



if __name__ == "__main__":
    main()
