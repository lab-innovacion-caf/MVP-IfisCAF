import pytz
from datetime import datetime

def get_epoch_time(timezone:str):
    timezone_tz = pytz.timezone(timezone)
    time = datetime.now(timezone_tz)
    epoch_time = int(time.timestamp())
    return epoch_time