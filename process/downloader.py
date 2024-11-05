"""
The module downloads the YouTube videos to local video repository and handles management of video metadata including
extracting information from transcripts and storing metadata in a SQLite database. It integrates with the Gemini 1.5
Flash LLM for analyzing video transcripts and employs SQLAlchemy for database interactions.
"""

from google.ai import get_response
from google.iam import read_api_key
from google.youtube import (download_video_as_mp4, get_video_duration, get_video_id,
                            get_video_transcript_en, is_video_corrupted)
from os import environ, path, remove
from pandas import DataFrame, read_csv
from re import search
from sqlalchemy import Boolean, create_engine, Column, DateTime, Integer, MetaData, select, String, Table, update
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.row import Row
from typing import Set, Sequence

DOWNLOAD_PATH = environ.get("DOWNLOAD_PATH")
GEMINI_DEVELOPER_API_KEY_PATH = read_api_key(environ.get("GEMINI_DEVELOPER_API_KEY_PATH"))
YOUTUBE_VIDEO_REPOSITORY_PATH = environ.get("YOUTUBE_VIDEO_REPOSITORY_PATH")

def create_metadata_database(engine: Engine) -> None:

    """
    Create a `metadata` database and the associated `metadata` table with the following columns:
        date (DateTime):                            The feed date as date of record.
        url (String):                               The URL of the video.
        video_id (String, Primary Key):             The unique identifier for the video serving as a primary key.
        info (String):                              Additional information related to the video.
        is_downloaded (Boolean, default=False):     A flag indicating whether the video has been downloaded.
        file_size (Integer, default=0):             The size of the video file in bytes.
        video_duration (Integer, default=0):        The duration of the video in second.
        is_uploaded (Boolean, default=False):       A flag indicating whether the video has been uploaded.
        shared_url (String, default=""):            The URL for sharing the video.
        file_id (String, default=""):               The file ID of the shared video.

    args:
        engine (Engine): An instance of SQLAlchemy's Engine to connect to the database.

    returns:
        None

    raises:
        SQLAlchemyError: If there is an error in creating the database or table.
    """

    print("Creating database and table.")
    database_metadata: MetaData = MetaData()
    Table("metadata", database_metadata,
          Column("date", DateTime),
          Column("url", String),
          Column("video_id", String, primary_key=True),
          Column("info", String),
          Column("is_downloaded", Boolean, default=False),
          Column("file_size", Integer, default=0),
          Column("video_duration", Integer, default=0),
          Column("is_uploaded", Boolean, default=False),
          Column("shared_url", String, default=""),
          Column("file_id", String, default=""))
    database_metadata.create_all(bind=engine)

def download_video(engine: Engine) -> None:

    """
    Downloads the video that has the is_downloaded flag marked as False in the metadata table.

    args:
        engine (Engine): An instance of SQLAlchemy's Engine to connect to the database.

    returns:
        None

    raises:
        yt_dlp.utils.DownloadError: If there is an error during the download process.
        yt_dlp.utils.ExtractorError: If there is an error extracting the video information.
    """

    metadata: Table = Table("metadata", MetaData(), autoload_with=engine)
    with engine.connect() as connection:
        rows: Sequence[Row] = connection.execute(select(metadata.c.url, metadata.c.video_id)
                                                 .where(metadata.c.is_downloaded == False)).fetchall()

        if not rows:
            print("No videos to download.")
            return

        for row in rows:
            video_url = row["url"]
            video_id = row["video_id"]
            download_video_as_mp4(download_path=DOWNLOAD_PATH,
                                  file_name=video_id,
                                  url=video_url)

            video_file_path: str = f"{DOWNLOAD_PATH}/{video_id}.mp4"
            if path.exists(video_file_path):
                if is_video_corrupted(video_file_path=video_file_path):
                    print(f"Corrupted video removed: {video_file_path}")
                    remove(path=video_file_path)
                else:
                    file_size: int = path.getsize(filename=video_file_path)
                    video_duration: int = int(round(get_video_duration(video_file_path=video_file_path)))

                    connection.execute((update(metadata)
                                        .where(metadata.c.video_id == video_id)
                                        .values(is_downloaded=True,
                                                file_size=file_size,
                                                video_duration=video_duration)))
                    print(f"Downloaded the video {video_url} successfully.")
            else:
                print(f"Failed to download the video {video_url}.")

def downloader(feed: str) -> None:

    """
    Reads a CSV feed containing YouTube video URLs, filters duplicates, updates metadata and downloads videos if needed.

    args:
        feed (str): The file path to the CSV feed containing YouTube video URLs.

    returns:
        None

    raises:
        FileNotFoundError: If the specified feed CSV file is not found.
        sqlalchemy.exc.SQLAlchemyError: If there is an issue with the database connection or query.
        yt_dlp.utils.DownloadError: If there is an error during the video download process.
        yt_dlp.utils.ExtractorError: If there is an error extracting the video information.
    """

    url: DataFrame = read_csv(filepath_or_buffer=feed)
    engine: Engine = create_engine(f"sqlite:///{YOUTUBE_VIDEO_REPOSITORY_PATH}metadata.db")
    if not path.exists(f"{YOUTUBE_VIDEO_REPOSITORY_PATH}metadata.db"):
        create_metadata_database(engine=engine)
    update_metadata(date=search(pattern=r"\d{4}-\d{2}-\d{2}", string=feed).group(0),
                    engine=engine,
                    url=filter_duplicates(engine=engine,
                                          url=url.assign(video_id=lambda x: x["url"].apply(get_video_id))))
    download_video(engine=engine)

def extract_video_info(transcript: str) -> str:

    """
    Extracts topics and their scope discussed in the video based on the transcript by interacting with the Gemini 1.5
    Flash LLM.

    args:
        transcript (str): The full transcript of the video content.

    returns:
        str: The text content of the first response part from the API, stripped of surrounding whitespace.
    """

    prompt = f"""
    Objective:
    Analyze the provided video transcript to identify and list the key topics being taught. 
    The goal is to understand the topics covered and assess their scope and depth.

    Task:
    Extract and list the topics along with their scope, as discussed in the transcript, in comma-separated format.

    Instructions:

    1. Identify the main topics covered in the video.
    2. Highlight the depth upto which the topic is discussed.
    3. Keep the response brief and to the point.
    4. Focusing only on relevant details without regard for complete sentences.

    Output Format:

    1. The response should be in plain text, no formating, no new lines.
    2. The format and style of the response should resemble with the given sample:

    \"Data Storage for Analysis and Machine Learning: Introduction to data storage concepts, including databases, data 
    warehouses, data lakes and Lakehouses. Spreadsheets: Limitations of spreadsheets for large datasets and complex 
    analysis. Relational Databases: Overview of its structure, brief introduction to normalization, use cases for 
    transactional processing (CRUD operations), basic query language. Data Warehouses: Overview, introduction to 
    structured design (star schema and snowflake schema), de-normalization for analytical purposes, advantages for 
    historical data tracking and complex queries. Data Lakes: Introduction to unstructured data storage, metadata 
    extraction and management, schema on read approach, use cases for machine learning and data exploration. 
    Lakehouses: Concept of integration of structured and unstructured data, three-layer architecture (storage, metadata,
    consumption), hybrid approach combining features of warehouses and lakes. Data Engineering: Overview of related 
    concepts, including data sourcing, data streaming and data transformation.\"

    Input:
    {transcript}
    """

    print("Getting information about the video based on the provided transcript from Gemini 1.5 Flash LLM.")
    return (get_response(api_key=GEMINI_DEVELOPER_API_KEY_PATH, prompt=prompt)
            .json().get("candidates")[0].get("content").get("parts")[0].get("text").strip())

def filter_duplicates(engine: Engine, url: DataFrame) -> DataFrame:

    """
    Filter out duplicate URLs based on the video_id from a given DataFrame based on existing entries in the database,
    returning a DataFrame that contains only the new, unique URLs.

    args:
        engine (Engine): An instance of SQLAlchemy's Engine to connect to the database.
        metadata (Table): The SQLAlchemy Table object representing the metadata table in the database.
        url (DataFrame): A pandas DataFrame containing the columns "url" and "video_id".

    returns:
        DataFrame: A DataFrame containing only the unique URLs that are not already present in the database.

    raises:
        SQLAlchemyError: If there is an error querying the database.
    """

    metadata: Table = Table("metadata", MetaData(), autoload_with=engine)
    with engine.connect() as connection:
        existing_video_ids: Set[str] = {row[0] for row in connection.execute(select(metadata.c.video_id))}
        duplicate_video_ids: DataFrame = url[url["video_id"].isin(existing_video_ids)]
        if not duplicate_video_ids.empty:
            print(f"Removed {len(duplicate_video_ids)} video_ids already present in the database:\n"
                f"{duplicate_video_ids["video_id"].to_list()}")

    return url[~url["video_id"].isin(existing_video_ids)]

def update_metadata(date: str, engine: Engine, url: DataFrame) -> None:

    """
    Updates the `metadata` table in the database by appending metadata of new YouTube video URLs. The metadata contains:
        date (DateTime):            feed date as date of record.
        video_id (str):             extracted from each URL.
        info (str):                 video information extracted by processing the video transcript.
        is_downloaded (boolean):    default value False
        file_size (int):            default value 0
        video_duration (int):       default value 0
        is_uploaded (boolean):      default value False
        shared_url (str):           default value ""
        file_id (str):              default value ""

    args:
        engine (Engine): A SQLAlchemy engine object used to connect to the database.
        url (DataFrame): A DataFrame containing an "url" column of new YouTube video URLs.

    returns:
        None
    """

    if url.empty:
        print("No data to update.")
        return

    print("Updating the metadata.")
    with engine.connect() as connection:
        (DataFrame({"url": url["url"],
                    "video_id": url["url"].apply(get_video_id)})
        .assign(date=date)
        .assign(info=lambda x: (x["video_id"]
                                .apply(get_video_transcript_en)
                                .apply(extract_video_info)))
        .assign(is_downloaded=False, file_size=0, video_duration=0, is_uploaded=False, shared_url="", file_id="")
        [["date",
          "url",
          "video_id",
          "info",
          "is_downloaded",
          "file_size",
          "video_duration",
          "is_uploaded",
          "shared_url",
          "file_id"]]
         .to_sql(name="metadata", con=connection, if_exists="append", index=False, chunksize=1000))
