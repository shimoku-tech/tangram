from dateutil import parser
from datetime import datetime, timedelta

def parse_date(date_string : str):
    try:
        parsed_date = parser.parse(date_string)
        return parsed_date
    except (ValueError, TypeError) as e:
        return f"Error parsing date: {e}"

def generate_date_range(start_date : datetime, end_date : datetime, inclusive: bool =True):
    date_array = []
    current_date = start_date
    
    while current_date < end_date if not inclusive else current_date <= end_date:
        date_array.append(current_date)
        current_date += timedelta(days=1)
    
    return date_array

def format_date(date : datetime):
    return date.strftime("%Y-%m-%d %H:%M:%S")