"""
TrendScope - Data Loader Module
Handles loading, caching, cleaning, category mapping, and dynamic file ingestion.
"""

import os
import json
import glob
import pandas as pd
import streamlit as st

try:  # imported as `src.data_loader`
    from .paths import DATA_RAW_DIR, DATA_PROCESSED_DIR, resolve
except ImportError:  # imported as flat `data_loader` (flattened repo)
    from paths import DATA_RAW_DIR, DATA_PROCESSED_DIR, resolve

REQUIRED_UPLOAD_COLUMNS = ["title", "views", "likes", "comment_count"]

MAPREDUCE_FILES = [
    ("mapreduce_job1_keywords.csv", ["keyword", "frequency"]),
    ("mapreduce_job2_category_stats.csv", ["category_id", "video_count", "total_views"]),
    ("mapreduce_job3_persistence.csv", ["video_id", "title", "days_trending"]),
]

HIVE_FILES = [
    ("hive_top_channels.csv", ["channel_title", "total_views", "total_likes", "total_comments", "avg_engagement_rate"]),
    ("hive_regional_summary.csv", ["country", "total_views", "total_likes", "total_comments", "avg_engagement_rate"]),
]


def _read_table(directory, filename, columns):
    """Read a precomputed CSV, returning an empty frame with the expected
    columns instead of raising when the file is missing on the host."""
    path = resolve(directory, filename)
    if os.path.exists(path):
        try:
            return pd.read_csv(path)
        except Exception as exc:  # corrupt/unreadable file
            st.warning(f"Could not read {filename}: {exc}")
    return pd.DataFrame(columns=columns)

COUNTRY_NAMES = {
    "US": "United States",
    "IN": "India",
    "GB": "United Kingdom",
    "CA": "Canada",
    "FR": "France"
}

@st.cache_data(show_spinner=False)
def load_category_mappings(country_code="US"):
    """
    Loads category id-to-title dictionary from category JSON file.
    """
    json_path = resolve(DATA_RAW_DIR, f"{country_code}_category_id.json")
    if not os.path.exists(json_path):
        json_path = resolve(DATA_RAW_DIR, "US_category_id.json")
        
    category_map = {}
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for item in data.get('items', []):
                    cat_id = int(item['id'])
                    cat_title = item['snippet']['title']
                    category_map[cat_id] = cat_title
        except Exception:
            pass
            
    # Default fallback mappings if file not fully populated
    default_map = {
        1: "Film & Animation", 2: "Autos & Vehicles", 10: "Music", 15: "Pets & Animals",
        17: "Sports", 19: "Travel & Events", 20: "Gaming", 22: "People & Blogs",
        23: "Comedy", 24: "Entertainment", 25: "News & Politics", 26: "Howto & Style",
        27: "Education", 28: "Science & Technology", 29: "Nonprofits & Activism"
    }
    for k, v in default_map.items():
        if k not in category_map:
            category_map[k] = v
            
    return category_map

@st.cache_data(show_spinner=False)
def load_country_data(country_code="US"):
    """
    Loads raw CSV for a specific country and cleans data.
    """
    csv_path = resolve(DATA_RAW_DIR, f"{country_code}videos.csv")
    if not os.path.exists(csv_path):
        return pd.DataFrame()
        
    try:
        df = pd.read_csv(csv_path, encoding='utf-8', on_bad_lines='skip')
        df['country'] = country_code
        df['country_name'] = COUNTRY_NAMES.get(country_code, country_code)
        
        # Numeric conversions
        for col in ['views', 'likes', 'dislikes', 'comment_count']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
                
        if 'category_id' in df.columns:
            df['category_id'] = pd.to_numeric(df['category_id'], errors='coerce').fillna(24).astype(int)
            cat_map = load_category_mappings(country_code)
            df['category_name'] = df['category_id'].map(cat_map).fillna("Entertainment")
            
        # Engagement rate = (likes + comments) / views * 100
        df['engagement_rate'] = (
            ((df['likes'] + df['comment_count']) / df['views'].replace(0, 1)) * 100
        ).round(2)
        
        # Like ratio = likes / (likes + dislikes) * 100
        df['like_ratio'] = (
            (df['likes'] / (df['likes'] + df['dislikes']).replace(0, 1)) * 100
        ).round(2)
        
        # Parse Dates
        if 'publish_time' in df.columns:
            df['publish_time'] = pd.to_datetime(df['publish_time'], errors='coerce')
            df['publish_hour'] = df['publish_time'].dt.hour
            df['publish_day'] = df['publish_time'].dt.day_name()
            
        return df
    except Exception as e:
        st.error(f"Error loading {country_code} data: {e}")
        return pd.DataFrame()

@st.cache_data(show_spinner=False)
def load_all_countries_data():
    """
    Loads and concatenates dataset across all available regions.
    """
    dfs = []
    for c in ["US", "IN", "GB", "CA", "FR"]:
        df_c = load_country_data(c)
        if not df_c.empty:
            dfs.append(df_c)
            
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()

@st.cache_data(show_spinner=False)
def load_precomputed_mapreduce():
    """
    Loads precomputed Hadoop MapReduce job outputs.
    """
    mr1, mr2, mr3 = (
        _read_table(DATA_PROCESSED_DIR, filename, columns)
        for filename, columns in MAPREDUCE_FILES
    )
    return mr1, mr2, mr3

@st.cache_data(show_spinner=False)
def load_precomputed_hive():
    """
    Loads precomputed Apache Hive analytics outputs.
    """
    h_channels, h_regional = (
        _read_table(DATA_PROCESSED_DIR, filename, columns)
        for filename, columns in HIVE_FILES
    )
    return h_channels, h_regional

def process_uploaded_csv(uploaded_file, country_code="CUSTOM"):
    """
    Parses and sanitizes user-uploaded YouTube trending CSV file.
    """
    try:
        df = pd.read_csv(uploaded_file, encoding='utf-8-sig', on_bad_lines='skip')
        df.columns = [str(c).strip() for c in df.columns]

        missing = [c for c in REQUIRED_UPLOAD_COLUMNS if c not in df.columns]
        if missing:
            st.error(
                "Uploaded CSV is missing required column(s): "
                + ", ".join(missing)
                + ". Expected the YouTube trending schema "
                "(video_id, trending_date, title, channel_title, category_id, "
                "publish_time, tags, views, likes, dislikes, comment_count, "
                "thumbnail_link, comments_disabled, ratings_disabled, "
                "video_error_or_removed, description)."
            )
            return pd.DataFrame()

        df['country'] = country_code
        df['country_name'] = f"Custom Dataset ({country_code})"

        for col in ['views', 'likes', 'dislikes', 'comment_count']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
            else:
                df[col] = 0
                
        if 'category_id' in df.columns:
            df['category_id'] = pd.to_numeric(df['category_id'], errors='coerce').fillna(24).astype(int)
            cat_map = load_category_mappings("US")
            df['category_name'] = df['category_id'].map(cat_map).fillna("Entertainment")
            
        df['engagement_rate'] = (
            ((df['likes'] + df['comment_count']) / df['views'].replace(0, 1)) * 100
        ).round(2)

        df['like_ratio'] = (
            (df['likes'] / (df['likes'] + df['dislikes']).replace(0, 1)) * 100
        ).round(2)

        if 'video_id' not in df.columns:
            df['video_id'] = [f"CUSTOM_{i:05d}" for i in range(len(df))]
        if 'channel_title' not in df.columns:
            df['channel_title'] = "Unknown Channel"
        if 'tags' not in df.columns:
            df['tags'] = ""

        if 'publish_time' in df.columns:
            df['publish_time'] = pd.to_datetime(df['publish_time'], errors='coerce')
            df['publish_hour'] = df['publish_time'].dt.hour
            df['publish_day'] = df['publish_time'].dt.day_name()
            
        return df
    except Exception as e:
        st.error(f"Failed to process uploaded CSV: {e}")
        return pd.DataFrame()
