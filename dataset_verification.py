from case_study_tool import PrepareTime
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def check_start_end(ds, start, end) -> bool:
    logging.info(f"Checking time interval [{start}, {end}]")
    try:
        ds_start = ds.time.values[0]
        ds_end = ds.time.values[-1]
        # Check if dataset covers the requested interval (forward or backward)
        if (ds_start <= start <= ds_end and ds_start <= end <= ds_end) or \
           (ds_end <= start <= ds_start and ds_end <= end <= ds_start):
            logging.info(f"Dataset time interval valid: [{ds_start}, {ds_end}]")
            return True
        else:
            logging.warning(f"Dataset time interval [{ds_start}, {ds_end}] doesn't cover requested [{start}, {end}]")
            return False
    except:
        logging.error("Dataset doesn't have valid time dimension")
        return False

def DatasetTimeValid(dataset, start_t, end_t) -> bool:
    start_t = PrepareTime(start_t)
    end_t = PrepareTime(end_t)
    if type(dataset) == list:
        flags = []
        for ds in dataset:
            flags.append(check_start_end(ds, start_t, end_t))
        flag = all(flags)
    else:
        flag = check_start_end(dataset, start_t, end_t)
    return flag 