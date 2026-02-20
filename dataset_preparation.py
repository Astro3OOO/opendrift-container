
import os
import logging
import xarray as xr
import zoneinfo
from general_tools import prepare_time

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

def prepare_dataset(start_t, end_t, border = [54, 62, 13, 30],
                   folder = None, concatenation =False, copernicus = False,
                   user = None, pword = None, vocabulary = None):
    wind = False
    # Lists of datasets that will be used in Reader.
    # List may consist of singe datstets (eg atmoshperic model, wind model) 
    # or combined datasets (atmo combined, wind combined)
    ds_ecmwf = []
    ds_netcdf = []
    ds_copernicus = []
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
            
    if copernicus:
        import copernicusmarine
        if user is None or pword is None:
            logging.error('No login credentials provided.')
        else:
            try:
                ds_1 = copernicusmarine.open_dataset(dataset_id='cmems_mod_bal_phy_anfc_PT1H-i', chunk_size_limit=0,
                                                    username=user, password = pword,
                                                    minimum_latitude=border[0], maximum_latitude=border[1],
                                                    minimum_longitude=border[2], maximum_longitude=border[3],
                                                    minimum_depth=0.5016462206840515, maximum_depth=0.5016462206840515,
                                                    start_datetime=start_t.replace(tzinfo=zoneinfo.ZoneInfo('UTC')),
                                                    end_datetime=end_t.replace(tzinfo=zoneinfo.ZoneInfo('UTC')))
                ds_copernicus.append(ds_1)
                ds_1.close()
                
                ds_2 = copernicusmarine.open_dataset(dataset_id='cmems_mod_bal_wav_anfc_PT1H-i', chunk_size_limit=0,
                                                    username = user,  password = pword,
                                                    minimum_latitude=border[0], maximum_latitude=border[1],
                                                    minimum_longitude=border[2], maximum_longitude=border[3],
                                                    start_datetime=start_t.replace(tzinfo=zoneinfo.ZoneInfo('UTC')),
                                                    end_datetime=end_t.replace(tzinfo=zoneinfo.ZoneInfo('UTC')))
                ds_copernicus.append(ds_2)
                ds_2.close()
                
            except:
                logging.warning('No data found in Copernicus Baltic. Searching in copernicus global...')
                # ds_copernicus = []
                try:
                    ds_1 = copernicusmarine.open_dataset(dataset_id='cmems_mod_glo_phy_anfc_0.083deg_PT1H-m', chunk_size_limit=0,
                                                        username=user, password = pword, 
                                                        minimum_latitude=border[0], maximum_latitude=border[1],
                                                        minimum_longitude=border[2], maximum_longitude=border[3],
                                                        minimum_depth=0.49402499198913574, maximum_depth=0.49402499198913574,
                                                        start_datetime=start_t.replace(tzinfo=zoneinfo.ZoneInfo('UTC')),
                                                        end_datetime=end_t.replace(tzinfo=zoneinfo.ZoneInfo('UTC')))
                    ds_copernicus.append(ds_1)
                    ds_1.close()

                    ds_2 = copernicusmarine.open_dataset(dataset_id='cmems_mod_glo_wav_anfc_0.083deg_PT3H-i', chunk_size_limit=0, 
                                                        username=user, password = pword,
                                                        minimum_latitude=border[0], maximum_latitude=border[1],
                                                        minimum_longitude=border[2], maximum_longitude=border[3],
                                                        start_datetime=start_t.replace(tzinfo=zoneinfo.ZoneInfo('UTC')),
                                                        end_datetime=end_t.replace(tzinfo=zoneinfo.ZoneInfo('UTC')))
                    ds_copernicus.append(ds_2)
                    ds_2.close()

                except:
                    logging.warning('No requested data in Copernicus Global.')
            ds_3 = copernicusmarine.open_dataset(dataset_id='cmems_mod_bal_wav_anfc_static', chunk_size_limit=0,
                                                username = user, password = pword,
                                                minimum_latitude=border[0], maximum_latitude=border[1],
                                                minimum_longitude=border[2], maximum_longitude=border[3])
            ds_copernicus.append(ds_3)
            ds_3.close()
                
    if wind:
        if len(ds_netcdf)>0:
            ds_netcdf += ds_wind
        if len(ds_copernicus)>0:
            ds_copernicus += ds_wind
    
    result = []
    if vocabulary == 'ECMWF':
        result += ds_ecmwf
        logging.info('Returnng ECMWF dataset')
    elif (vocabulary == 'Copernicus') or (vocabulary == 'Copernicus_edited'):
        if len(ds_netcdf) > 0:
            result += ds_netcdf
            logging.info('Returnng Copernicus NetCDF dataset')
        if len(ds_copernicus) > 0:
            result += ds_copernicus
            logging.info('Returnng Copernicus requested dataset')
    elif len(ds_netcdf) > 0:
        result += ds_netcdf
        logging.info('Returnng unspecified NetCDF dataset')
    else:
        logging.error('No dataset to return.')

    return result