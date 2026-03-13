import os
import pandas as pd
import logging
import datetime as dt

def resolve_path(directory):
    output_dir = os.getenv(directory)

    if output_dir is None:
        if os.getenv("CI"):  
            output_dir = directory  
        elif os.path.exists("/"+directory):  
            output_dir = "/"+directory
        else:
            output_dir = directory
            
    os.makedirs(output_dir, exist_ok=True)
    return output_dir

def _get_time_from_reader(agg, lst, time_type = None):
    types = ['start', 'end']
    if time_type in types:
        if isinstance(lst, list):
            new_lst = []
            for element in lst:
                reader_times = {'start':element.start_time,
                                'end': element.end_time}
                target = reader_times[time_type]
                if isinstance(target, dt.datetime) or isinstance(element,  dt.datetime):
                    new_lst.append(target)
            if agg == 'Max':
                return max(new_lst)
            elif agg == 'Min':
                return min(new_lst)
            else:
                logging.warning(f'Aggregation {agg} is not supported')
        else:
            reader_times = {'start':lst.start_time,
                            'end': lst.end_time}
            return reader_times[time_type]
    else:
        logging.warning(f'Time type {time_type} is unsupported')
        return

def prepare_time(time, reader = None, time_type = 'start'):
    placeholder = {'start':dt.datetime.now(),
                   'end':dt.datetime.now() + dt.timedelta(days = 2)}
    if isinstance(time, (pd.Timestamp, dt.datetime)):
        return time
    elif isinstance(time, (int, str)):
        try:
            time = pd.to_datetime(time)
            return time
        except:
            logging.warning(f'Unable to transform {time} start time into pandas Timesamp.')
            time = None
    elif time is None and reader is not None and (time_type in placeholder.keys()):
        Aggregations = {'start':'Min',
                        'end':'Max'}
        return _get_time_from_reader(Aggregations[time_type], reader, time_type)
    else:
        logging.error(f'Incorrect time input {time}. Returning placeholder')
        return placeholder[time_type]