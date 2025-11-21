import praw
from datetime import datetime, timedelta
from pathlib import Path
import json
import requests
import os
import logging
import pandas as pd
import csv
from io import StringIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage

####################################################
# CONFIGURATION
####################################################

DALLE_SUBR = "dalle2"
MIDJ_SUBR = "midjourney"
AIART_SUBR = "aiArt"
SUBREDDITS = [DALLE_SUBR, MIDJ_SUBR, AIART_SUBR]

# GCS Configuration
GCS_BUCKET = "art-bkt" 
GCS_PROJECT = "eecs6893-471617"

# Reddit API Configuration
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID', '')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET', '')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT', 'mac:eecs6893Project:v1.0 (by u/Superb-Cap8523)')

# Content type mapping - convert any jpg to jpeg
CONTENT_TYPE_MAP = {
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.png': 'image/png'
}

# Flairs
FLAIR_DICT = {DALLE_SUBR: set(['DALL·E 2', 'DALL·E 3']),
              MIDJ_SUBR: set(['AI Showcase - Midjourney']),
              AIART_SUBR: set(['Image - DALL E 3 :a2:',
                              'Image - Midjourney :a2:'])
            }

AIART_MAP = {'Image - DALL E 3 :a2:': DALLE_SUBR,
            'Image - Midjourney :a2:': MIDJ_SUBR}

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

####################################################
# HELPER FUNCTIONS
####################################################

def get_gcs_submission_ids(subr):
    """ Get existing submission IDs from GCS CSV metadata """
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(f"{subr}/metadata.csv")
    
    try:
        if blob.exists():
            csv_data = blob.download_as_string().decode('utf-8')
            df = pd.read_csv(StringIO(csv_data))
            return set(df['submission_id'].tolist())
        else:
            logger.warning(f"No metadata.csv found for r/{subr} in GCS")
            return set()
    except Exception as e:
        logger.warning(f"Could not read metadata.csv for r/{subr}: {e}")
        return set()

def get_file_extension(url):
    """ extract file extension from URL """
    from urllib.parse import urlparse
    
    parsed_url = urlparse(url)
    _, ext = os.path.splitext(parsed_url.path)
    
    return ext.lower() if ext else '.jpg'

def upload_new_images_to_gcs(**context):
    """ collect new images from a subreddit """
    
    subr = context['params']['subr']
    flair = FLAIR_DICT[subr]
    
    logger.info(f"\n{'='*70}")
    logger.info(f"Collecting new images from r/{subr}")
    logger.info(f"{'='*70}")
    
    # Initialize Reddit client
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT
    )
    
    # get existing image IDs from GCS
    existing_ids = get_gcs_submission_ids(subr)
    
    # collect new posts
    subreddit = reddit.subreddit(subr)
    new_images = []
    stats = {
        'subreddit': subr,
        'checked': 0,
        'passed_flair': 0,
        'passed_image': 0,
        'passed_nsfw': 0,
        'new_images': 0,
        'download_errors': 0,
        'images': []
    }
    
    image_extensions = ('.jpg', '.jpeg', '.png')

    # get GCS bucket
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    
    # Check most recent 1000 posts
    for submission in subreddit.new(limit=1000):
        stats['checked'] += 1
        
        # check if post has flair and if so if it is relevant
        if not submission.link_flair_text or submission.link_flair_text not in flair:
            continue
        stats['passed_flair'] += 1
        
        # check if URL is image
        if not submission.url.endswith(image_extensions):
            continue
        stats['passed_image'] += 1
        
        # skip NSFW
        if submission.over_18:
            continue
        stats['passed_nsfw'] += 1
        
        # check if NEW (not already downloaded)
        if submission.id in existing_ids:
            continue
        
        # download new images
        try:
            response = requests.get(submission.url, timeout=10)
            
            # get original extension
            original_ext = get_file_extension(submission.url)
            
            # determine target subreddit and folder
            if subr == AIART_SUBR:
                target_subr = AIART_MAP[submission.link_flair_text]
            else:
                target_subr = subr
            
            filename = f"img_{target_subr}_{submission.id}{original_ext}"

            # upload images to gcs
            blob = bucket.blob(f"{target_subr}/{filename}")
            content_type = CONTENT_TYPE_MAP.get(original_ext, 'image/jpeg')
            blob.upload_from_string(
                response.content,
                content_type = content_type
            )

            
            stats['new_images'] += 1
            new_images.append({
                'submission_id': submission.id,
                'filename': filename,
                'url': submission.url,
                'subreddit': subr,
                'date': datetime.fromtimestamp(submission.created_utc).isoformat(),
                'target_subr': target_subr, # need this bc we need to write metadata to target_subr folder
            })
            logger.info(f"Downloaded new image: {filename}")
        
        except Exception as e:
            stats['download_errors'] += 1
            logger.error(f"Error downloading {submission.url}: {e}")
    
    stats['images'] = new_images
    
    # Log summary
    logger.info(f"\nProcessing summary for r/{subr}:")
    logger.info(f"  Total posts checked: {stats['checked']}")
    logger.info(f"  Passed flair filter: {stats['passed_flair']}")
    logger.info(f"  Passed image filter: {stats['passed_image']}")
    logger.info(f"  Passed NSFW filter: {stats['passed_nsfw']}")
    logger.info(f"  New images: {stats['new_images']}")
    logger.info(f"  Download errors: {stats['download_errors']}")
    
    # Push stats and images to XCom for next task
    context['task_instance'].xcom_push(key=f'{subr}_stats', value=stats)
    context['task_instance'].xcom_push(key=f'{subr}_images', value=new_images)
    
    return stats

def update_metadata_csv(**context):
    """ update metadata.csv in GCS """
    
    # get
    subr = context['params']['subr']
    new_images = context['task_instance'].xcom_pull(key=f'{subr}_images')
    
    if not new_images:
        logger.info(f"No new images for r/{subr}, skipping metadata update")
        return
    
    try:
        # group images by target subreddit (dalle2 or midjourney)
        images_by_target = {}
        for img in new_images:
            target_subr = img['target_subr']
            if target_subr not in images_by_target:
                images_by_target[target_subr] = []
            images_by_target[target_subr].append(img)
        
        # update metadata.csv in GCS for each target subreddit
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET)
        
        for target_subr, images in images_by_target.items():
            blob = bucket.blob(f"{target_subr}/metadata.csv")
            
            # read existing CSV from GCS if it exists
            if blob.exists():
                csv_data = blob.download_as_string().decode('utf-8')
                df_existing = pd.read_csv(StringIO(csv_data))
            else:
                df_existing = None
            
            # Create DataFrame from new images
            df_new = pd.DataFrame([{
                'submission_id': img['submission_id'],
                'filename': img['filename'],
                'url': img['url'],
                'subreddit': img['subreddit'],
                'date': img['date']
            } for img in images])
            
            # Combine with existing data
            if df_existing is not None:
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            else:
                df_combined = df_new
            
            # Upload to GCS
            csv_string = df_combined.to_csv(index=False)
            blob.upload_from_string(csv_string, content_type='text/csv')
            
            logger.info(f"✓ Updated metadata.csv in GCS for {target_subr} with {len(images)} entries")
    
    except Exception as e:
        logger.error(f"Error updating metadata for r/{subr}: {e}")


def create_daily_log(**context):
    """ create and upload daily log to GCS """
    
    all_stats = []
    
    # Pull stats from all subreddit tasks
    for subr in SUBREDDITS:
        stats = context['task_instance'].xcom_pull(task_ids=f'collect_images_{subr}', key=f'{subr}_stats')
        if stats:  # Only append if stats exist
           all_stats.append(stats)
    
    # Create daily log
    log_data = {
        'date': datetime.now().isoformat(),
        'subreddit_stats': all_stats,
        'total_new_images': sum(s['new_images'] for s in all_stats),
        'total_errors': sum(s['download_errors'] for s in all_stats),
    }
    
    # Upload log to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    
    log_filename = f"logs/daily_collection_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    blob = bucket.blob(log_filename)
    blob.upload_from_string(json.dumps(log_data, indent=4), content_type='application/json')
    
    logger.info("\n" + "="*70)
    logger.info("DAILY IMAGE COLLECTION SUMMARY")
    logger.info("="*70)
    for stats in all_stats:
        logger.info(f"r/{stats['subreddit']}: {stats['new_images']} new images")
    logger.info(f"\nTotal new images: {log_data['total_new_images']}")
    logger.info(f"Total errors: {log_data['total_errors']}")
    logger.info(f"Log uploaded to GCS: gs://{GCS_BUCKET}/{log_filename}")
    logger.info("="*70 + "\n")

####################################################
# AIRFLOW DAG
####################################################

default_args = {
    'owner': 'gy2354',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['gy2354@ecolumbia.edu'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'reddit_image_scraper',
    default_args=default_args,
    description='scrape images daily from r/dalle2, r/midjourney, and r/aiArt',
    schedule_interval='00 16 * * *',  # run daily at 11 am
    catchup=False,
    tags=['reddit', 'data-collection', 'gcs'],
)

# Create tasks for each subreddit
collect_tasks = {}
metadata_tasks = {}

for subr in SUBREDDITS:
    # collect and upload images
    collect_tasks[subr] = PythonOperator(
        task_id=f'collect_images_{subr}',
        python_callable=upload_new_images_to_gcs,
        params={'subr': subr},
        dag=dag,
    )
    
    # Update metadata CSV
    metadata_tasks[subr] = PythonOperator(
        task_id=f'update_metadata_{subr}',
        python_callable=update_metadata_csv,
        params={'subr': subr},
        dag=dag,
    )
    
    # Set dependencies
    collect_tasks[subr] >> metadata_tasks[subr] 

# Create daily log after all uploads complete
create_log = PythonOperator(
    task_id='create_daily_log',
    python_callable=create_daily_log,
    dag=dag,
)

# All upload tasks must complete before log creation
for subr in SUBREDDITS:
    metadata_tasks[subr] >> create_log