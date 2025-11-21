import praw
from datetime import datetime
from pathlib import Path
import json
import requests
import os
import logging
import pandas as pd
import csv

####################################################
# CONFIGURATION
####################################################

DALLE_SUBR = "dalle2"
MIDJ_SUBR = "midjourney"
AIART_SUBR = "aiArt"
SUBREDDITS = [DALLE_SUBR, MIDJ_SUBR, AIART_SUBR]

# Local data directory
DATA_DIR = Path("data_collection/data")

# Reddit API Configuration
REDDIT_CLIENT_ID = "JT_iiB-NFwFyoknGwv5fYA"
REDDIT_CLIENT_SECRET = "aVY6bitDc5BS8j4LX1cWusWjlgCaVQ"
REDDIT_USER_AGENT = "mac:eecs6893Project:v1.0 (by u/Superb-Cap8523)"

# Flairs from your extract_data.ipynb
# flair for each subreddit we are scrapping
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

def get_existing_image_ids_local(subr):
    """ get a set of submission ids """

    subr_dir = DATA_DIR / subr
    
    if not subr_dir.exists():
        logger.warning(f"Directory {subr_dir} does not exist yet")
        return set()
    
    # csv file with all the id
    metadata_path = subr_dir / "metadata.csv"
    existing_ids = set(pd.read_csv(metadata_path)["submission_id"])

    logger.info(f"Found {len(existing_ids)} existing images for r/{subr}")
    return existing_ids

def get_file_extension(url):
    """ extract file extension from UR """
    from urllib.parse import urlparse
    
    parsed_url = urlparse(url)
    _, ext = os.path.splitext(parsed_url.path)
    
    return ext.lower() if ext else '.jpg'

def collect_new_images_from_subreddit_local(subr, flair):
    """ collect new images from a subreddit locally """
    
    logger.info(f"\n{'='*70}")
    logger.info(f"Collecting new images from r/{subr}")
    logger.info(f"{'='*70}")
    
    # Initialize Reddit client
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT
    )
    
    # Get existing image IDs
    existing_ids = get_existing_image_ids_local(subr)
    
    # Collect new posts
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
    
    # Check most recent 1000 posts
    for submission in subreddit.new(limit=1000):
        stats['checked'] += 1
        
        # check flair
        if submission.link_flair_text not in flair:
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
            
            # save to local folder
            target_dir = DATA_DIR / target_subr
            target_dir.mkdir(parents=True, exist_ok=True)
            filepath = target_dir / filename
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            stats['new_images'] += 1
            new_images.append({
                'submission_id': submission.id,
                'filename': filename,
                'url': submission.url,
                'subreddit': subr,
                'date': datetime.fromtimestamp(submission.created_utc).isoformat(),
                'target_subr': target_subr,
                'flair': submission.link_flair_text,
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
    logger.info(f"  ❌ Download errors: {stats['download_errors']}")
    
    return stats

def update_metadata_csv_local(subr, new_images):
    """ update metadata.csv for dalle2 and midjourney with submission_id, filename, url, subreddit, date """
    
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
        
        # write to metadata.csv for each target subreddit
        for target_subr, images in images_by_target.items():
            target_dir = DATA_DIR / target_subr
            csv_path = target_dir / "metadata.csv"
            
            file_exists = csv_path.exists()
            
            with open(csv_path, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['submission_id', 'filename', 'url', 'subreddit', 'date'])
                
                # write header only if file is new
                if not file_exists:
                    writer.writeheader()
                
                for img in images:
                    writer.writerow({
                        'submission_id': img['submission_id'],
                        'filename': img['filename'],
                        'url': img['url'],
                        'subreddit': img['subreddit'],
                        'date': img['date']
                    })
            
            logger.info(f"✓ Updated metadata.csv in {target_subr} with {len(images)} entries")
    
    except Exception as e:
        logger.error(f"Error updating metadata for r/{subr}: {e}")



def collect_all_subreddit_images_local():
    """
    Main function: Collect images from all subreddits locally
    Run this script directly without Airflow
    """
    
    logger.info("\n" + "="*70)
    logger.info(f"LOCAL IMAGE COLLECTION STARTED - {datetime.now()}")
    logger.info("="*70)
    
    all_stats = []
    
    # Collect from each subreddit
    for subr in SUBREDDITS:
        stats = collect_new_images_from_subreddit_local(subr, FLAIR_DICT[subr])
        all_stats.append(stats)
        
        # Update metadata CSV for dalle2 and midjourney
        update_metadata_csv_local(subr, stats['images'])
    
    # Create daily log
    log_data = {
        'date': datetime.now().isoformat(),
        'subreddit_stats': all_stats,
        'total_new_images': sum(s['new_images'] for s in all_stats),
        'total_errors': sum(s['download_errors'] for s in all_stats),
    }
    
    # save log locally
    logs_dir = DATA_DIR / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    
    log_filename = f"daily_collection_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    log_path = logs_dir / log_filename
    
    with open(log_path, 'w') as f:
        json.dump(log_data, f, indent=4)
    
    logger.info("\n" + "="*70)
    logger.info("DAILY IMAGE COLLECTION SUMMARY")
    logger.info("="*70)
    for stats in all_stats:
        logger.info(f"r/{stats['subreddit']}: {stats['new_images']} new images")
    logger.info(f"\nTotal new images: {log_data['total_new_images']}")
    logger.info(f"Total errors: {log_data['total_errors']}")
    logger.info(f"Log saved to: {log_path}")
    logger.info("="*70 + "\n")


if __name__ == "__main__":
    collect_all_subreddit_images_local()