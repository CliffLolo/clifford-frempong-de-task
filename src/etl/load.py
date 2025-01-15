#load.py
import psycopg2
from datetime import datetime
import yaml
from src.etl.utils.logger import get_logger

logger = get_logger()
def load_data(transformed_data, conn):
    cursor = conn.cursor()
    logger.info("Starting data load...")

    try:
        # Load dim_date with enhanced fields
        for date in transformed_data['dates']:
            cleaned_date_values = tuple(value if value != '' else None for value in date.values())
            cursor.execute('''
                INSERT INTO dim_date (
                    date_key, full_date, year, quarter, quarter_name, month, month_name,
                    week_of_year, week_start_date, week_end_date, bestsellers_date,
                    published_date, previous_published_date, next_published_date
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date_key) DO UPDATE SET
                    bestsellers_date = EXCLUDED.bestsellers_date,
                    previous_published_date = EXCLUDED.previous_published_date,
                    next_published_date = EXCLUDED.next_published_date
            ''', cleaned_date_values)

        # Load dim_publisher with SCD Type 2
        for publisher in transformed_data['publishers']:
            cursor.execute('''
                INSERT INTO dim_publisher (
                    publisher_name, effective_start_date, is_current
                )
                VALUES (%s, %s, TRUE)
                ON CONFLICT ON CONSTRAINT uk_publisher_name_dates DO NOTHING
            ''', (publisher['publisher_name'], datetime.now().date()))

        # Load dim_list with SCD Type 2
        for book_list in transformed_data['lists']:
            cursor.execute('''
                INSERT INTO dim_list (
                    list_id, list_name, display_name, update_frequency,
                    list_image_url, effective_start_date, is_current
                )
                VALUES (%s, %s, %s, %s, %s, %s, TRUE)
                ON CONFLICT ON CONSTRAINT uk_list_id_dates DO NOTHING
            ''', tuple(book_list.values()))

        # Load dim_book with enhanced fields and SCD Type 2
        for book in transformed_data['books']:
            cursor.execute('''
                WITH publisher_key AS (
                    SELECT publisher_key 
                    FROM dim_publisher 
                    WHERE publisher_name = %s AND is_current = TRUE
                )
                INSERT INTO dim_book (
                    title, author, contributor, contributor_note, age_group,
                    publisher_key, primary_isbn13, primary_isbn10, description,
                    created_date, updated_date, effective_start_date, is_current
                )
                SELECT 
                    %s, %s, %s, %s, %s, pk.publisher_key, %s, %s, %s, %s, %s, %s, TRUE
                FROM publisher_key pk
                ON CONFLICT ON CONSTRAINT uk_isbn13_dates DO NOTHING
            ''', (
                book['publisher'], book['title'], book['author'],
                book['contributor'], book['contributor_note'], book['age_group'],
                book['primary_isbn13'], book['primary_isbn10'], book['description'],
                book['created_date'], book['updated_date'], book['effective_start_date']
            ))

        # Load fact_book_rankings with price
        for ranking in transformed_data['rankings']:
            cursor.execute('''
                INSERT INTO fact_book_rankings (
                    date_key, book_key, list_key, rank, price
                )
                SELECT 
                    d.date_key, b.book_key, l.list_key, %s, %s
                FROM dim_book b
                JOIN dim_list l ON l.list_id = %s AND l.is_current = TRUE
                JOIN dim_date d ON d.full_date = %s
                WHERE b.primary_isbn13 = %s AND b.is_current = TRUE
            ''', (
                ranking['rank'], ranking['price'], ranking['list_id'],
                ranking['published_date'], ranking['isbn13']
            ))

        # Load fact_publisher_performance with list dimension
        cursor.execute('''
            INSERT INTO fact_publisher_performance (
                publisher_key, date_key, list_key, quarter, year,
                total_points, books_in_top_5,
                rank_1_count, rank_2_count, rank_3_count, rank_4_count, rank_5_count
            )
            SELECT 
                p.publisher_key, d.date_key, f.list_key,
                d.quarter, d.year,
                SUM(CASE WHEN f.rank <= 5 THEN (6 - f.rank) ELSE 0 END) AS total_points,
                COUNT(CASE WHEN f.rank <= 5 THEN 1 END),
                COUNT(CASE WHEN f.rank = 1 THEN 1 END),
                COUNT(CASE WHEN f.rank = 2 THEN 1 END),
                COUNT(CASE WHEN f.rank = 3 THEN 1 END),
                COUNT(CASE WHEN f.rank = 4 THEN 1 END),
                COUNT(CASE WHEN f.rank = 5 THEN 1 END)
            FROM fact_book_rankings f
            JOIN dim_book b ON f.book_key = b.book_key
            JOIN dim_publisher p ON b.publisher_key = p.publisher_key
            JOIN dim_date d ON f.date_key = d.date_key
            GROUP BY p.publisher_key, d.date_key, f.list_key, d.quarter, d.year
            ON CONFLICT ON CONSTRAINT uk_publisher_date_list DO UPDATE SET
                total_points = EXCLUDED.total_points,
                books_in_top_5 = EXCLUDED.books_in_top_5,
                rank_1_count = EXCLUDED.rank_1_count,
                rank_2_count = EXCLUDED.rank_2_count,
                rank_3_count = EXCLUDED.rank_3_count,
                rank_4_count = EXCLUDED.rank_4_count,
                rank_5_count = EXCLUDED.rank_5_count
        ''')

        conn.commit()
        logger.info("Data load completed successfully.")
    
    except Exception as e:
        logger.error(f"Data load failed: {e}")
        conn.rollback()
        raise
    
    finally:
        cursor.close()