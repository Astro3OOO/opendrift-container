from case_study_tool import PrepareTime
import logging
import os
from pathlib import Path
import xarray as xr

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
'''
    Dataset time validation
'''
def CheckStartEnd(ds, start, end) -> bool:
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
            flags.append(CheckStartEnd(ds, start_t, end_t))
        flag = all(flags)
    else:
        flag = CheckStartEnd(dataset, start_t, end_t)
    return flag 


'''
    Dataset selection
'''
def CheckStructure(folder) -> str:
    pth = Path(folder)
    content = [sub for sub in pth.iterdir()]
    if all([c.is_file() for c in content]):
        return 'files'
    elif all([c.is_dir() for c in content]):
        return 'dirs'
    else:
        return 'mixed'

def ReturnTimeInterval(file) -> dict:
    interval = {}
    file = Path(file)
    if file.is_file():
        if file.suffix == '.grib':
            with xr.open_dataset(file) as ds:
                t0 = ds.time.values + ds.step[0].values
                t1 = ds.time.values + ds.step[-1].values
            interval[file] = [t0,t1]
        elif file.suffix == '.nc':
            with xr.open_dataset(file) as ds:
                t0 = ds.time[0].values
                t1 = ds.time[-1].values
            interval[file] = [t0,t1]
        else:
            logging.error(f'{file.suffix} files are not currently supported.') 
    else:
        logging.error(f'{file} is not a single file. Check the folder structure!') 
    return interval

def ReadRootDir(root) -> dict:
    pth = Path(root)
    result = {}
    struct = CheckStructure(pth)
    if struct == 'files':
        for file in pth.iterdir():
            result.update(ReturnTimeInterval(file))
    elif struct == 'dirs':
        for folder in pth.iterdir():
            for file in folder.iterdir():
                result.update(ReturnTimeInterval(file))
    else:
        logging.error('Mixed structure files + dirs is unsupported.')    
    return result

def Matching(start_t, end_t, all_paths) -> list:    
    matching_paths= []
    start_t = PrepareTime(start_t)
    end_t = PrepareTime(end_t)
    
    if end_t < start_t:
        #swap if reverse
        buf = start_t
        start_t = end_t
        end_t = buf

    for path, (t0, t1) in all_paths.items():
        # select when in
        if (t0 <= start_t <= t1) or (t0 <= end_t <= t1):
            matching_paths.append(path) 
        # select when out
        if (start_t < t0) and (t1 < end_t):
            matching_paths.append(path) 
    return matching_paths

def SelectDataSet(start_t, end_t, folder) -> dict:
    changes = {}
    start_t = PrepareTime(start_t)
    end_t = PrepareTime(end_t)
    files = ReadRootDir(folder)
    
    
    
    return changes 