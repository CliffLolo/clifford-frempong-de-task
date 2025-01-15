# transform.py
# from datetime import datetime, timedelta
# from src.etl.utils.logger import get_logger

# logger = get_logger()

# def transform_data(raw_data):
#     logger.info("Starting data transformation...")
#     try:
#         results = raw_data.get('results', {})
#         transformed = {
#             'books': [],
#             'publishers': [],
#             'lists': [],
#             'rankings': [],
#             'dates': []
#         }

#         published_date = results.get('published_date')
#         if published_date:
#             date_obj = datetime.strptime(published_date, '%Y-%m-%d')
#             transformed['dates'].append({
#                 'date_key': int(date_obj.strftime('%Y%m%d')),
#                 'full_date': published_date,
#                 'year': date_obj.year,
#                 'quarter': (date_obj.month - 1) // 3 + 1,
#                 'quarter_name': f"Q{(date_obj.month - 1) // 3 + 1}",
#                 'month': date_obj.month,
#                 'month_name': date_obj.strftime('%B'),
#                 'week_of_year': date_obj.isocalendar()[1],
#                 'week_start_date': (date_obj - timedelta(days=date_obj.weekday())).strftime('%Y-%m-%d'),
#                 'week_end_date': (date_obj + timedelta(days=6 - date_obj.weekday())).strftime('%Y-%m-%d')
#             })

#         for book_list in results.get('lists', []):
#             transformed['lists'].append({
#                 'list_id': book_list['list_id'],
#                 'list_name': book_list['list_name'],
#                 'display_name': book_list['display_name'],
#                 'update_frequency': book_list['updated'],
#                 'list_image': book_list.get('list_image', '')
#             })

#             for book in book_list.get('books', []):
#                 transformed['publishers'].append({'publisher_name': book['publisher']})
#                 transformed['books'].append({
#                     'isbn13': book['primary_isbn13'],
#                     'isbn10': book['primary_isbn10'],
#                     'title': book['title'],
#                     'author': book['author'],
#                     'publisher': book['publisher'],
#                     'description': book.get('description', ''),
#                     'rank': book['rank']
#                 })
#                 transformed['rankings'].append({
#                     'isbn13': book['primary_isbn13'],
#                     'list_id': book_list['list_id'],
#                     'rank': book['rank'],
#                     'published_date': published_date
#                 })

#         logger.info("Data transformation completed successfully.")
#         return transformed

#     except Exception as e:
#         logger.error(f"Data transformation failed: {e}")
#         raise



# transform.py
from datetime import datetime, timedelta
from src.etl.utils.logger import get_logger

logger = get_logger()

def clean_date(value):
    return value if value else None


def transform_data(raw_data):
    logger.info("Starting data transformation...")
    try:
        results = raw_data.get('results', {})
        transformed = {
            'books': [],
            'publishers': set(),  # Using set to avoid duplicates
            'lists': [],
            'rankings': [],
            'dates': []
        }

        # Transform dates
        date_fields = [
            'published_date',
            'bestsellers_date',
            'previous_published_date',
            'next_published_date'
        ]

        
        
        for field in date_fields:
            if date_str := results.get(field):
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                if field == 'published_date':  # Only create date dimension for published_date
                    transformed['dates'].append({
                        'date_key': int(date_obj.strftime('%Y%m%d')),
                        'full_date': date_str,
                        'year': date_obj.year,
                        'quarter': (date_obj.month - 1) // 3 + 1,
                        'quarter_name': f"Q{(date_obj.month - 1) // 3 + 1} {date_obj.year}",
                        'month': date_obj.month,
                        'month_name': date_obj.strftime('%B'),
                        'week_of_year': date_obj.isocalendar()[1],
                        'week_start_date': (date_obj - timedelta(days=date_obj.weekday())).strftime('%Y-%m-%d'),
                        'week_end_date': (date_obj + timedelta(days=6 - date_obj.weekday())).strftime('%Y-%m-%d'),
                        'bestsellers_date': clean_date(results.get('bestsellers_date')),
                        'published_date': clean_date(date_str),
                        'previous_published_date': clean_date(results.get('previous_published_date')),
                        'next_published_date': clean_date(results.get('next_published_date'))
                    })

        for book_list in results.get('lists', []):
            # Transform lists
            transformed['lists'].append({
                'list_id': book_list['list_id'],
                'list_name': book_list['list_name'],
                'display_name': book_list['display_name'],
                'update_frequency': book_list['updated'],
                'list_image_url': book_list.get('list_image', ''),
                'effective_start_date': datetime.now().date()
            })

            for book in book_list.get('books', []):
                # Transform publishers
                transformed['publishers'].add(book['publisher'])
                
                # Transform books
                transformed['books'].append({
                    'title': book['title'],
                    'author': book['author'],
                    'contributor': book['contributor'],
                    'contributor_note': book['contributor_note'],
                    'age_group': book['age_group'],
                    'publisher': book['publisher'],
                    'primary_isbn13': book['primary_isbn13'],
                    'primary_isbn10': book['primary_isbn10'],
                    'description': book.get('description', ''),
                    'created_date': datetime.strptime(book['created_date'], '%Y-%m-%d %H:%M:%S'),
                    'updated_date': datetime.strptime(book['updated_date'], '%Y-%m-%d %H:%M:%S'),
                    'effective_start_date': datetime.now().date()
                })

                # Transform rankings
                transformed['rankings'].append({
                    'isbn13': book['primary_isbn13'],
                    'list_id': book_list['list_id'],
                    'rank': book['rank'],
                    'price': book['price'],
                    'published_date': results['published_date']
                })

        # Convert publishers set to list of dicts
        transformed['publishers'] = [{'publisher_name': p} for p in transformed['publishers']]
        
        logger.info("Data transformation completed successfully.")
        return transformed

    except Exception as e:
        logger.error(f"Data transformation failed: {e}")
        raise
