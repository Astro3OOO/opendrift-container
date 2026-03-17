
import os
import logging
import xarray as xr
from src.general_tools import prepare_time

REQ_VARS_WAVE = ['VTM02', 'VHM0_WW', 'VHM0', 'VTM01_SW1', 'VMDR_SW1',
                 'VTPK', 'VSDX', 'VMDR_WW', 'VSDY', 'VHM0_SW1', 'VTM01_WW']
REQ_VARS_PHYS = ['uo', 'thetao', 'so', 'mlotst', 'siconc', 'sla', 'vo']

def cut_dataset(dataset, t0, t1):
    
    # drop vars
    for vars in [REQ_VARS_PHYS, REQ_VARS_WAVE]:
        if all(r in dataset.data_vars for r in vars):
            dataset = dataset[vars]
    
    # select time range   
    if t0 != None and t1 != None:     
        dataset = dataset.sel(time = slice(t0,t1))
    
    # select depth (sea-level) 
    if 'depth' in dataset._dims.keys():
        dataset = dataset.sel(depth = dataset.depth[0])
    
    return dataset
 
def _open_concatenate_datasets(fp=None, file=None, wind_bool = False, ecmwf = [],
                  wind = [], netcdf = [], start_t=None, end_t=None):
    '''
    File can be given as: 1) file = path/to/file.format 2) fp = path/to/file.format 3) file = file.format; fp = path/to/
    '''
    if os.path.exists(fp) and file:
        full_path = os.path.join(fp,file)
    elif os.path.exists(fp):
        full_path = fp
    elif os.path.exists(file):
        full_path = file
    else:
        logging.error(f'Given file {file} anp path {fp} are invalid. Provide valid paths.')
        return wind_bool, ecmwf, wind, netcdf 
    
    if os.path.isfile(full_path):
        if file.endswith('.grib'):
            with xr.open_dataset(full_path, engine='cfgrib') as ds:
                ds = ds.assign_coords(time=ds['time'] + ds['step'])
                ds = ds.swap_dims({'step': 'time'})
                ds = cut_dataset(ds, start_t, end_t)
                if ds.sizes.get("time", 1) > 0 and len(ds.data_vars) > 0:
                    ecmwf.append(ds)
                if 'u10' in ds.data_vars:
                    wind.append(xr.Dataset({'u10' : ds['u10'],
                                        'v10': ds['v10']}))
                    wind_bool = True
 
            logging.info(f'Readed GRIB file {full_path}')
        elif file.endswith('.nc'):
            with xr.open_dataset(full_path, engine='netcdf4') as ds:   
                ds = cut_dataset(ds, start_t, end_t)
                if ds.sizes.get("time", 1) > 0 and len(ds.data_vars) > 0:
                    netcdf.append(ds)

            logging.info(f'Readed NetCDF file {full_path}')
        else:
            logging.warning(f'Unknow file type {file}. Only .grib and .nc are currently supported.')
    else:
        logging.error(f'Given file {file} is not valid. provide a single file.')
    return wind_bool, ecmwf, wind, netcdf 

def _read_folder(path_to, wind_bool=False, start_t=None, end_t=None):
    ecmwf = []
    wind = []
    netcdf = []
    if os.path.isdir(path_to):
        for file in os.listdir(path_to):
            wind_bool, ecmwf, wind, netcdf = _open_concatenate_datasets(path_to, file, wind_bool, ecmwf, wind, netcdf, start_t, end_t)
    elif os.path.isfile(path_to):
        wind_bool, ecmwf, wind, netcdf = _open_concatenate_datasets(path_to, None, wind_bool, ecmwf, wind, netcdf, start_t, end_t)
    else:
        logging.error(f'Given path {path_to} is not valid. provide a single file or path to folder.')
    return ecmwf, netcdf, wind, wind_bool

def prepare_dataset(start_t, end_t, folder = None, concatenation =False, vocabulary = None):
    wind = False
    # Lists of datasets that will be used in Reader.
    # List may consist of singe datstets (eg atmoshperic model, wind model) 
    # or combined datasets (atmo combined, wind combined)
    ds_ecmwf = []
    ds_netcdf = []
    ds_wind = []
    
    start_t = prepare_time(start_t)
    end_t = prepare_time(end_t)
    
    if folder != None:
        if concatenation:
            for subdir in os.listdir(folder):
                full_path = os.path.join(folder, subdir)
                if os.path.isdir(full_path):
                    buffer_ecmwf, buffer_netcdf, buffer_wind, wind = _read_folder(full_path, wind, start_t, end_t)

                    buffers = {'ecmwf': buffer_ecmwf, 'netcdf': buffer_netcdf, 'wind': buffer_wind}
                    targets = {'ecmwf': ds_ecmwf, 'netcdf': ds_netcdf, 'wind': ds_wind}

                    for key in ['ecmwf','netcdf','wind']:
                        buf = buffers[key]
                        if buf:
                            merged = xr.concat(buf, dim='time')
                            merged = merged.sortby('time')
                            merged = merged.drop_duplicates(dim='time')
                            targets[key].append(merged) 
                else:
                    logging.error(f'{full_path} Is not a valid directory.')

        
        else:
            ds_ecmwf, ds_netcdf, ds_wind, wind = _read_folder(folder, wind, start_t, end_t)
    else:
        logging.error(f'No folder is provided. Returning empty dataset!')
        return []
  
    if wind:
        if len(ds_netcdf)>0:
            ds_netcdf += ds_wind
    
    result = []
    if vocabulary == 'ECMWF':
        result += ds_ecmwf
        logging.info('Returnng ECMWF dataset')
    elif (vocabulary == 'Copernicus'):
        if len(ds_netcdf) > 0:
            result += ds_netcdf
            logging.info('Returnng Copernicus NetCDF dataset')

    elif len(ds_netcdf) > 0:
        result += ds_netcdf
        logging.info('Returnng unspecified NetCDF dataset')
    else:
        logging.error('No dataset to return.')

    return result