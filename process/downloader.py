"""
The module downloads the YouTube video to local video repository and handles management of video metadata including
extracting information from transcripts and storing metadata in a SQLite database. It utilizes concurrent futures for
downloading the videos parallely, integrates with the Gemini 1.5 Flash LLM for analyzing video transcripts and employs
SQLAlchemy for database interactions.

constants:
    - DOWNLOAD_PATH: The path where downloaded videos will be stored, retrieved from the environment.
    - GEMINI_DEVELOPER_API_KEY_PATH: The API key for accessing the Gemini API, retrieved from the environment.
    - YOUTUBE_VIDEO_REPOSITORY_PATH: The path for the YouTube video repository, retrieved from the environment.

dependencies:
    - utilities.google.ai
    - utilities.google.youtube
    - pandas
    - sqlalchemy
"""

from concurrent import futures
from datetime import date
from google.ai import get_api_key, get_response
from google.youtube import download_video_as_mp4, get_video_id, get_video_transcript_en
from os import environ, path
from pandas import DataFrame, read_csv
from sqlalchemy import Boolean, create_engine, Column, DateTime, MetaData, select, String, Table
from sqlalchemy.engine.base import Engine
from typing import Set

DOWNLOAD_PATH = environ.get("DOWNLOAD_PATH")
GEMINI_DEVELOPER_API_KEY_PATH = get_api_key(environ.get("GEMINI_DEVELOPER_API_KEY_PATH"))
YOUTUBE_VIDEO_REPOSITORY_PATH = environ.get("YOUTUBE_VIDEO_REPOSITORY_PATH")

def create_metadata_database(engine: Engine) -> None:

    """
    Create a `metadata` database and the associated `metadata` table with the following columns:
        date (DateTime): The date and time associated with the metadata entry.
        url (String): The URL of the video.
        video_id (String, Primary Key): The unique identifier for the video serving as a primary key.
        info (String): Additional information related to the video.
        is_downloaded (Boolean, default=False): A flag indicating whether the video has been downloaded.
        is_uploaded (Boolean, default=False): A flag indicating whether the video has been uploaded.
        shared_url (String): A URL for sharing the video, if applicable.

    args:
        engine (Engine): An instance of SQLAlchemy's Engine to connect to the database.

    returns:
        None

    raises:
        SQLAlchemyError: If there is an error in creating the database or table.
    """

    print("Creating database and table.")
    database_metadata: MetaData = MetaData()
    Table('metadata', database_metadata,
          Column('date', DateTime),
                Column('url', String),
                Column('video_id', String, primary_key=True),
                Column('info', String),
                Column('is_downloaded', Boolean, default=False),
                Column('is_uploaded', Boolean, default=False),
                Column('shared_url', String))
    database_metadata.create_all(bind=engine)

def downloader(feed: str) -> None:

    """
    to be continued...
    """

    url: DataFrame = read_csv(filepath_or_buffer=feed)
    engine: Engine = create_engine(f"sqlite:///{YOUTUBE_VIDEO_REPOSITORY_PATH}metadata.db")
    if not path.exists(f"{YOUTUBE_VIDEO_REPOSITORY_PATH}metadata.db"):
        create_metadata_database(engine=engine)
    else:


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
        url (DataFrame): A pandas DataFrame containing the columns 'url' and 'video_id'.

    returns:
        DataFrame: A DataFrame containing only the unique URLs that are not already present in the database.

    raises:
        SQLAlchemyError: If there is an error querying the database.
    """

    metadata: Table = Table('metadata', MetaData(), autoload_with=engine)
    with engine.connect() as connection:
        existing_video_ids: Set[str] = {row[0] for row in connection.execute(select(metadata.c.video_id))}
        duplicate_video_ids: DataFrame = url[url['video_id'].isin(existing_video_ids)]
        if not duplicate_video_ids.empty:
            print(f"Removed {len(duplicate_video_ids)} video_ids already present in the database:\n"
                f"{duplicate_video_ids['video_id'].to_list()}")

    return url[~url['video_id'].isin(existing_video_ids)]