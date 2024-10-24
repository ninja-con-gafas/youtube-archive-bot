#!/bin/bash

export AIRFLOW_HOME="/home/atharv/Documents/Repositories/youtube-archive-bot/airflow/"
export AIRFLOW__CORE__DAGS_FOLDER="/home/atharv/Documents/Repositories/youtube-archive-bot/dags/"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

export YOUTUBE_VIDEO_REPOSITORY_PATH="/mnt/226AE8B26AE883BF/YouTube Archives/"
export DOWNLOAD_PATH="$YOUTUBE_VIDEO_REPOSITORY_PATH/videos/"
export FEED_PATH="$YOUTUBE_VIDEO_REPOSITORY_PATH/feed/"
export GEMINI_DEVELOPER_API_KEY_PATH="$YOUTUBE_VIDEO_REPOSITORY_PATH/keys/gemini_developer_api.key"

export PYTHONPATH="$PYTHONPATH:/home/atharv/Documents/Repositories/utilities/"

if airflow standalone & then
    echo "Airflow standalone started successfully."
else
    echo "Failed to start Airflow standalone." >&2
    exit 1
fi

if airflow scheduler; then
    echo "Airflow scheduler started successfully."
else
    echo "Failed to start Airflow scheduler." >&2
    exit 1
fi
