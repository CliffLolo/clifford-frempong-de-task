# # ============================
# # Historical Load Script
# # ============================

# import time
# from datetime import datetime, timedelta
# from src.etl.extract import extract_data
# from src.etl.transform import transform_data
# from src.etl.load import load_data
# from src.etl.database import get_db_connection
# from src.etl.utils.logger import get_logger
# import yaml

# # Initialize logger
# logger = get_logger()

# # Load configuration
# with open('/Users/clifflolo/Desktop/Hubtel/nytttttttt/config/config.yml', 'r') as file:
#     config = yaml.safe_load(file)


# def check_if_date_loaded(conn, bestsellers_date):
#     """Check if data for the bestsellers date has already been loaded."""
#     cursor = conn.cursor()
#     cursor.execute("""
#         SELECT 1 
#         FROM load_status 
#         WHERE bestsellers_date = %s 
#           AND status = 'COMPLETED';
#     """, (bestsellers_date,))
#     result = cursor.fetchone()
#     cursor.close()
#     return result is not None




# def update_load_status(conn, requested_date, bestsellers_date, status):
#     """Update the load status in the load_status table."""
#     cursor = conn.cursor()
#     cursor.execute('''
#         INSERT INTO load_status (requested_date, bestsellers_date, status, updated_at)
#         VALUES (%s, %s, %s, NOW())
#         ON CONFLICT (requested_date, bestsellers_date) DO UPDATE SET
#             status = EXCLUDED.status, 
#             updated_at = NOW();
#     ''', (requested_date, bestsellers_date, status))
#     conn.commit()
#     cursor.close()




# def extract_data_with_retry(date, retries=3, delay=5):
#     """Retry logic for API extraction."""
#     for attempt in range(1, retries + 1):
#         try:
#             logger.info(f"🔍 Attempting to extract data for {date} (Attempt {attempt})...")
#             return extract_data(date)
#         except Exception as e:
#             logger.warning(f"⚠️ Attempt {attempt} failed for {date}: {e}")
#             if attempt < retries:
#                 logger.info(f"⏳ Retrying after {delay} seconds...")
#                 time.sleep(delay)
#             else:
#                 logger.error(f"❌ Failed to extract data for {date} after {retries} attempts.")
#                 return None
# def historical_load():
#     """Main function for the historical data load process."""
#     conn = get_db_connection()
#     logger.info("🚀 Starting historical data load process.")
    
#     # Load start and end dates from configuration
#     start_date = datetime.strptime(config['load']['start_date'], '%Y-%m-%d').date()
#     end_date = datetime.strptime(config['load']['end_date'], '%Y-%m-%d').date()

#     current_date = start_date
#     logger.info(f"📅 Loading data from {current_date} to {end_date}.")

#     try:
#         while current_date <= end_date:
#             logger.info(f"📦 Processing data for {current_date.strftime('%Y-%m-%d')}...")

#             # Extract data with retry logic
#             raw_data = extract_data_with_retry(current_date.strftime('%Y-%m-%d'))
#             if raw_data is None:
#                 logger.error(f"❌ Extraction failed for {current_date}. Skipping to next date.")
#                 current_date += timedelta(days=1)
#                 continue

#             # Extract the bestsellers_date from the API response
#             bestsellers_date = raw_data.get('results', {}).get('bestsellers_date')
#             if not bestsellers_date:
#                 logger.warning(f"⚠️ No bestsellers data found for {current_date}. Skipping...")
#                 current_date += timedelta(days=1)
#                 continue

#             bestsellers_date = datetime.strptime(bestsellers_date, '%Y-%m-%d').date()
#             logger.info(f"📅 API returned data for bestsellers date: {bestsellers_date}")

#             # # Check if the bestsellers data was already loaded
#             # if check_if_date_loaded(conn, current_date, bestsellers_date):
#             #     logger.info(f"✅ Data for bestsellers date {bestsellers_date} already loaded. Skipping...")
#             #     current_date += timedelta(days=1)
#             #     continue

#             if check_if_date_loaded(conn, bestsellers_date):
#                 logger.info(f"✅ Data for requested date {current_date} and bestsellers date {bestsellers_date} already loaded. Skipping...")
#                 current_date += timedelta(days=1)
#                 continue


#             # Mark load status as IN_PROGRESS
#             update_load_status(conn, current_date, bestsellers_date, 'IN_PROGRESS')
#             logger.info(f"📝 Marked requested date {current_date} and bestsellers date {bestsellers_date} as IN_PROGRESS.")

#             # Transform and load the data
#             transformed_data = transform_data(raw_data)
#             load_data(transformed_data, conn)
#             logger.info(f"📦 Data successfully loaded for {bestsellers_date}.")

#             # After successful load, mark as COMPLETED
#             update_load_status(conn, current_date, bestsellers_date, 'COMPLETED')
#             logger.info(f"✅ Marked requested date {current_date} and bestsellers date {bestsellers_date} as COMPLETED.")

#             # Move to the next requested date
#             current_date += timedelta(days=1)
#             time.sleep(1)  # Optional delay between loads

#     finally:
#         # Always close the database connection
#         conn.close()
#         logger.info("🔒 Database connection closed.")
#         logger.info("🎉 Historical data load process completed.")


# if __name__ == "__main__":
#     historical_load()


# File: scripts/historical_load.py
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