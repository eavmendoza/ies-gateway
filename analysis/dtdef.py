from datetime import datetime, timedelta

DATE_FORMAT_PRIORITY = '%Y-%m-%d'
DATE_FORMAT_OPT = [DATE_FORMAT_PRIORITY, '%Y%m%d', '%y%m%d']

class DatetimeDefinition:
    def __init__(self, date_str:str=None, date_dt:datetime=None):
        if date_str:
            # print("Getting date format")
            for fmt in DATE_FORMAT_OPT:
                try:
                    self.dt = datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
                
                self.string = self.dt.strftime(DATE_FORMAT_PRIORITY)
        elif date_dt:
            self.dt = date_dt
            self.string =  date_dt.strftime(DATE_FORMAT_PRIORITY)
        else: 
            raise ValueError("No date values cannot be empty")


class DateWindow:

    def __init__(self, days_span:int=None, start_day:str=None, end_day:str=None):
        if days_span and not end_day:
            self.end = DatetimeDefinition(date_dt=datetime.now())
            self.start = DatetimeDefinition(date_dt=datetime.now()-timedelta(days=days_span))

        elif days_span and end_day:
            self.end= DatetimeDefinition(date_str=end_day)
            self.start = DatetimeDefinition(date_dt=self.end.dt-timedelta(days=days_span))

        elif end_day and start_day:
            self.end = DatetimeDefinition(date_str=end_day)
            self.start = DatetimeDefinition(date_str=start_day)          

        else:
            raise ValueError("Unable to generate window generation")

def today():
    return DatetimeDefinition(date_dt=datetime.today()).string


def lag_from_today(lag):
    return DatetimeDefinition(date_dt=datetime.now()-timedelta(hours=lag)).string