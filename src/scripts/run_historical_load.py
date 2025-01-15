# historical_load.py
import time
import os
from datetime import datetime, timedelta
import yaml
from dotenv import load_dotenv
from src.etl.extract import extract_data
from src.etl.transform import transform_data
from src.etl.load import load_data
from src.etl.database import get_db_connection
from src.etl.utils.logger import get_logger


load_dotenv()

logger = get_logger()



def check_if_date_loaded(conn, bestsellers_date):
    """Check if data for the bestsellers date has already been loaded."""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT status, updated_at 
            FROM load_status 
            WHERE bestsellers_date = %s
            ORDER BY updated_at DESC
            LIMIT 1;
        """, (bestsellers_date,))
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"❌ Error checking load status: {e}")
        conn.rollback()
        return None

def update_load_status(conn, requested_date, bestsellers_date, status, error_message=None):
    """Update load status with error tracking."""
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO load_status (
                requested_date, 
                bestsellers_date, 
                status, 
                error_message,
                updated_at
            )
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (requested_date, bestsellers_date) 
            DO UPDATE SET
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message,
                updated_at = NOW();
        ''', (requested_date, bestsellers_date, status, error_message))
        conn.commit()
        cursor.close()
    except Exception as e:
        logger.error(f"❌ Error updating load status: {e}")
        conn.rollback()

def extract_with_retry(date, max_retries=3, initial_delay=5):
    """Enhanced retry logic with exponential backoff."""
    for attempt in range(max_retries):
        try:
            logger.info(f"🔍 Extracting data for {date} (Attempt {attempt + 1})")
            data = extract_data(date)
            if not data.get('results'):
                raise ValueError("No results in API response")
            return data
        except Exception as e:
            delay = initial_delay * (2 ** attempt)
            if attempt < max_retries - 1:
                logger.warning(f"⚠️ Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                logger.error(f"❌ All attempts failed for {date}: {e}")
                return None

def process_date(conn, target_date):
    """Process a single date with comprehensive error handling."""
    bestsellers_date = None
    try:
        # Extract
        raw_data = extract_with_retry(target_date.strftime('%Y-%m-%d'))
        if not raw_data:
            raise Exception("Data extraction failed")

        # Get bestsellers date
        bestsellers_date = raw_data.get('results', {}).get('bestsellers_date')
        if not bestsellers_date:
            raise ValueError("No bestsellers date in response")

        bestsellers_date = datetime.strptime(bestsellers_date, '%Y-%m-%d').date()
        
        # Check existing status
        current_status = check_if_date_loaded(conn, bestsellers_date)
        if current_status == 'COMPLETED':
            logger.info(f"✅ Data already loaded for {bestsellers_date}")
            return True
        
        # Process data
        update_load_status(conn, target_date, bestsellers_date, 'IN_PROGRESS')
        transformed_data = transform_data(raw_data)
        load_data(transformed_data, conn)
        
        update_load_status(conn, target_date, bestsellers_date, 'COMPLETED')
        logger.info(f"✅ Successfully processed {bestsellers_date}")
        return True

    except Exception as e:
        error_msg = str(e)
        logger.error(f"❌ Error processing {target_date}: {error_msg}")
        if bestsellers_date:
            update_load_status(conn, target_date, bestsellers_date, 'FAILED', error_msg)
        return False

def historical_load():
    """Run historical load process."""
    # # Load configuration
    # with open('config/config.yml', 'r') as file:
    #     config = yaml.safe_load(file)
    # load_dotenv()

    conn = get_db_connection()
    logger.info("🚀 Starting historical load")
    
    try:
        # start_date = datetime.strptime(config['load']['start_date'], '%Y-%m-%d').date()
        # end_date = datetime.strptime(config['load']['end_date'], '%Y-%m-%d').date()

        start_date = datetime.strptime(os.getenv('start_date'), '%Y-%m-%d').date()
        end_date = datetime.strptime(os.getenv('end_date'), '%Y-%m-%d').date()
        
        current_date = start_date
        success_count = fail_count = 0

        while current_date <= end_date:
            if process_date(conn, current_date):
                success_count += 1
            else:
                fail_count += 1
            current_date += timedelta(days=1)
            time.sleep(1)  # Rate limiting

        logger.info(f"🎉 Historical load completed. Successes: {success_count}, Failures: {fail_count}")

    finally:
        conn.close()
        logger.info("🔒 Database connection closed.")

if __name__ == "__main__":
    historical_load()