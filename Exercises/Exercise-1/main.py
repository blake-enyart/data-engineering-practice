import requests
from zipfile import ZipFile
import io, os
import aiohttp
import asyncio

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

def build_dir(folder_name: str):
    cwd_path = os.getcwd()
    downloads_path = os.path.join(cwd_path, folder_name)
    try:
        os.mkdir(downloads_path)
    except:
        pass
    return downloads_path

def main():
    downloads_path = build_dir("downloads")

    for url in download_uris:
        filename = url.split("/")[-1].split(".")[0]
        response = requests.get(url=url)
        if response.status_code != 200:
            continue
        zip_file = io.BytesIO(response.content)
        with ZipFile(zip_file, "r") as file:
            file.extract(member=f"{filename}.csv", path=downloads_path)

async def fetch(session, url):
    async with session.get(url) as response:
        if response.status != 200:
            return (None, url)
        response = await response.read()
        return (response, url)


async def fetch_all(urls, loop):
    async with aiohttp.ClientSession(loop=loop) as session:
        results = await asyncio.gather(*[fetch(session, url) for url in urls], return_exceptions=True)
        return results

if __name__ == "__main__":
    main()
    downloads_path = build_dir("async_downloads")
    loop = asyncio.get_event_loop()
    urls = download_uris
    csvs = loop.run_until_complete(fetch_all(urls, loop))
    for zip_file in csvs:
        if zip_file[0]:
            bytes_result = io.BytesIO(zip_file[0])
            with ZipFile(bytes_result, "r") as file:
                file.extract(member=f'{zip_file[1].split("/")[-1].split(".")[0]}.csv', path=downloads_path)
