from case_study_tool import PrepareTime, ResolvePath
from file_clusterization import ClusterFiles
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

def DatasetValid(dataset, start_t, end_t, allow_empty_ds = False) -> bool:
    if (dataset == [] or dataset is None) and not allow_empty_ds:
        logging.error('Dataset is empty or None, cannot run simulation!')
        return False
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
    # Reads metadata and return {path : [t_first, t_last]}
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

# Based on folder structure, reads all files (lazy)
def ReadRootDir(root) -> dict:
    pth = Path(root)
    result = {}
    # checks whetere folder is flat or nested 
    struct = CheckStructure(pth)
    # append all files with their intervals
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

# Function select files that intersect requiered time interval 
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

# Switch the root dir to /SELECTED and symlink files to it
def ReRoot(paths):
    select_dir = ResolvePath("SELECTED")
    if paths == []:
        return select_dir
    
    new_root = Path(select_dir)
    new_root.mkdir(exist_ok=True, parents=True)

    common = Path(os.path.commonpath(paths))

    if any([file.parent.relative_to(common) == Path('') for file in paths]):
        rel_paths = ClusterFiles(paths)
    else:
        rel_paths = [p.relative_to(common) for p in paths]

    destinations = {file.name : new_root / file for file in rel_paths}
    
    for p in paths:
        dest = destinations[p.name]
        dest.parent.mkdir(parents=True, exist_ok=True)
        
        # create symlink (skip if exists)
        if not dest.exists():
            dest.symlink_to(p)
    return select_dir

# Select files from folder that intersect given time and restructure dataset directory
# Function reads time metadata of all files, selects matching, makes new directory and symlink files to it  
def SelectDataSet(start_t, end_t, folder) -> dict:
    changes = {}
    start_t = PrepareTime(start_t)
    end_t = PrepareTime(end_t)
    # Read all files in folder. Return dict {path:[t_first, t_last], ... }
    files = ReadRootDir(folder)
    # Select files that has overlaping time interval with requested time. Return list [path1, path2, ... ]
    requested = Matching(start_t, end_t, files)
    print(requested)
    # Re-root selected files with symlink to new folder 'SELECTED' 
    new_folder = ReRoot(requested)
    changes['folder'] = new_folder
    if CheckStructure(new_folder) == 'dirs':
            changes['concatenation'] = True
    return changes 