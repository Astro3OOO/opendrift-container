from general_tools import prepare_time, resolve_path
from file_clusterization import cluster_files
import logging
import os
from pathlib import Path
import xarray as xr

'''
    Dataset selection
'''
def check_folder_structure(folder) -> str:
    pth = Path(folder)
    content = [sub for sub in pth.iterdir()]
    if all([c.is_file() for c in content]):
        return 'files'
    elif all([c.is_dir() for c in content]):
        return 'dirs'
    else:
        return 'mixed'

def return_time_interval(file) -> dict:
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
def read_root_directory(root) -> dict:
    pth = Path(root)
    result = {}
    # checks whetere folder is flat or nested 
    struct = check_folder_structure(pth)
    # append all files with their intervals
    if struct == 'files':
        for file in pth.iterdir():
            result.update(return_time_interval(file))
    elif struct == 'dirs':
        for folder in pth.iterdir():
            for file in folder.iterdir():
                result.update(return_time_interval(file))
    else:
        logging.error('Mixed structure files + dirs is unsupported.')    
    return result

# Function select files that intersect requiered time interval 
def filter_files_by_time_interval(start_t, end_t, all_paths) -> list:    
    matching_paths= []
    start_t = prepare_time(start_t)
    end_t = prepare_time(end_t)
    
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
def symlink_selected_files(paths):
    select_dir = resolve_path("SELECTED")
    if paths == []:
        return select_dir
    
    new_root = Path(select_dir)
    new_root.mkdir(exist_ok=True, parents=True)

    common = Path(os.path.commonpath(paths))

    if any([file.parent.relative_to(common) == Path('') for file in paths]):
        rel_paths = cluster_files(paths)
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
def select_dataset(start_t, end_t, folder) -> dict:
    changes = {}
    start_t = prepare_time(start_t)
    end_t = prepare_time(end_t)
    # Read all files in folder. Return dict {path:[t_first, t_last], ... }
    files = read_root_directory(folder)
    # Select files that has overlaping time interval with requested time. Return list [path1, path2, ... ]
    requested = filter_files_by_time_interval(start_t, end_t, files)
    print(requested)
    # Re-root selected files with symlink to new folder 'SELECTED' 
    new_folder = symlink_selected_files(requested)
    changes['folder'] = new_folder
    if check_folder_structure(new_folder) == 'dirs':
            changes['concatenation'] = True
    return changes 