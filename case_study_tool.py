import xarray as xr
from opendrift.models.oceandrift import OceanDrift
from opendrift.models.leeway import Leeway
from opendrift.models.shipdrift import ShipDrift
from opendrift.models.openoil import OpenOil
import datetime as dt
import zoneinfo
import pandas as pd
import os
import copernicusmarine
from opendrift.readers.reader_netCDF_CF_generic import Reader
import logging

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
# )
logger_od = logging.getLogger('opendrift') 
logger_od.setLevel(logging.CRITICAL)

logger_cop = logging.getLogger('copernicusmarine') 
logger_cop.setLevel(logging.WARNING)

MODEL_DICT = {'OceanDrift':OceanDrift,
              'Leeway':Leeway,
              'ShipDrift':ShipDrift,
              'OpenOil': OpenOil}
REQ_VARS_WAVE = ['VTM02', 'VHM0_WW', 'VHM0', 'VTM01_SW1', 'VMDR_SW1',
                 'VTPK', 'VSDX', 'VMDR_WW', 'VSDY', 'VHM0_SW1', 'VTM01_WW']
REQ_VARS_PHYS = ['uo', 'thetao', 'so', 'mlotst', 'siconc', 'sla', 'vo']

def ResolvePath(directory):
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

def get_time_from_reader(agg, lst, time_type = None):
    types = ['start', 'end']
    if time_type in types:
        if type(lst) == list:
            new_lst = []
            for element in lst:
                reader_times = {'start':element.start_time,
                                'end': element.end_time}
                target = reader_times[time_type]
                if (type(target) == dt.datetime or 
                    type(element) == pd._libs.tslibs.timestamps.Timestamp):
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

def PrepareTime(time, reader = None, time_type = None):
    placeholder = {'start':dt.datetime.now(),
                   'end':dt.datetime.now() + dt.timedelta(days = 2)}
    if isinstance(time, dt.datetime):
        return time
    elif isinstance(time, pd._libs.tslibs.timestamps.Timestamp):
        return time
    elif isinstance(time, int) or isinstance(time, str):
        try:
            time = pd.to_datetime(time)
            return time
        except:
            logging.warning(f'Unable to transform {time} start time into pandas Timesamp.')
            time = None
    elif time is None and reader is not None and (time_type in placeholder.keys()):
        Aggregations = {'start':'Min',
                        'end':'Max'}
        return get_time_from_reader(Aggregations[time_type], reader, time_type)
    else:
        logging.error(f'Incorrect end time input {time}. Returning placeholder')
        return placeholder[time_type]
    
def CutDataset(dataset, t0, t1):
    
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
 
def OpenAndAppend(fp=None, file=None, wind_bool = False, ecmwf = [],
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
                ds = CutDataset(ds, start_t, end_t)
                if ds.sizes.get("time", 1) > 0 and len(ds.data_vars) > 0:
                    ecmwf.append(ds, start_t, end_t)
                if 'u10' in ds.data_vars:
                    wind.append(xr.Dataset({'u10' : ds['u10'],
                                        'v10': ds['v10']}))
                    wind_bool = True
 
            logging.info(f'Readed GRIB file {full_path}')
        elif file.endswith('.nc'):
            with xr.open_dataset(full_path, engine='netcdf4') as ds:   
                ds = CutDataset(ds)
                if ds.sizes.get("time", 1) > 0 and len(ds.data_vars) > 0:
                    netcdf.append(ds)

            logging.info(f'Readed NetCDF file {full_path}')
        else:
            logging.warning(f'Unknow file type {file}. Only .grib and .nc are currently supported.')
    else:
        logging.error(f'Given file {file} is not valid. provide a single file.')
    return wind_bool, ecmwf, wind, netcdf 

def ReadFolder(path_to, wind_bool=False, start_t=None, end_t=None):
    ecmwf = []
    wind = []
    netcdf = []
    if os.path.isdir(path_to):
        for file in os.listdir(path_to):
            wind_bool, ecmwf, wind, netcdf = OpenAndAppend(path_to, file, wind_bool, ecmwf, wind, netcdf, start_t, end_t)
    elif os.path.isfile(path_to):
        wind_bool, ecmwf, wind, netcdf = OpenAndAppend(path_to, None, wind_bool, ecmwf, wind, netcdf, start_t, end_t)
    else:
        logging.error(f'Given path {path_to} is not valid. provide a single file or path to folder.')
    return ecmwf, netcdf, wind, wind_bool

def PrepareDataSet(start_t, end_t, border = [54, 62, 13, 30],
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
    
    start_t = PrepareTime(start_t)
    end_t = PrepareTime(end_t)
    
    if folder != None:
        if concatenation:
            for subdir in os.listdir(folder):
                full_path = os.path.join(folder, subdir)
                if os.path.isdir(full_path):
                    buffer_ecmwf, buffer_netcdf, buffer_wind, wind = ReadFolder(full_path, wind, start_t, end_t)

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
            ds_ecmwf, ds_netcdf, ds_wind, wind = ReadFolder(folder, wind, start_t, end_t)
            
    if copernicus:
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

def Seed(o, model, lw_obj, start_position, start_t, num, rad, ship, wdf, seed_type, orientation, oil_type, shpfile=None):
    params = dict(
        lat = start_position[0],
        lon = start_position[1],
        number = num,
        radius=rad,
        time = start_t
    )
            
    if model == OceanDrift:
        params.update(wind_drift_factor = wdf)
    elif model == Leeway:
        params.update(object_type = lw_obj)
    elif model == ShipDrift:
        length, beam, height, draft = ship
        o.set_config('seed:orientation', orientation)
        params.update(length = length, beam = beam, height = height, draft = draft)
    elif model == OpenOil:
        params.update( oil_type=oil_type)
    else:
        logging.error(f'Model {model} is not implemented yet.')
        return o
    
    match seed_type:
        case 'elements':
            o.seed_elements(**params)
        case 'cone':
            o.seed_cone(**params)
        case _:
            logging.error('Unsupported seed type')
    return o

def simulation(lw_obj=1, model='OceanDrift', start_position=None, start_t=None,
               end_t=None, datasets=None, std_names=None, num=100, selection = None,
               rad=0, ship=[62, 8, 10, 5], wdf=0.02, orientation = 'random',
               delay=False, multi_rad=False, seed_type=None, time_step = None, vocabulary = None,
               configurations = None, file_name = None, oil_type='GENERIC BUNKER C', shpfile=None):
    
    # Check main requirments
    if start_position == None:
        logging.error('Start position is required')
        return
    if datasets == None:
        logging.error('At least one dataset is required')
        return
    if seed_type == None:
        seed_type = 'elements'
        
    # if seed_type == 'shapefile':
    #     if shpfile == None:
    #         logging.error('Seed type is selected as seeding from shape, but no shape file was provided')
    #         return    
        
    if model not in MODEL_DICT.keys():
        logging.error(f'Model {model} is not supported. Choose one of the following: {list(MODEL_DICT.keys())}')
        return
    model = MODEL_DICT[model]   
    
    
    # Create readers
    if type(datasets) == list:
        reader = [Reader(ds, standard_name_mapping=std_names) for ds in datasets]
    else:
        reader = Reader(datasets, standard_name_mapping=std_names)
        
    # Prepare start and end times
    start_t = PrepareTime(start_t, reader, 'start')
    end_t = PrepareTime(end_t, reader, 'end')
    
    if file_name == None:
        m = str(model).split('.')[-1][:-2]
        t_now = dt.datetime.now().strftime("%Y-%m-%d_%H%M")
        t_strt = start_t.strftime("%Y-%m-%d_%H%M")
        file_name = f'{m}_{t_strt}_{t_now}.nc'
    
    # Make correct OUTPUT dir (abs/rel path) depending on where the code is running
    output_dir = ResolvePath("OUTPUT")
      
    file_name = os.path.join(output_dir, file_name)
    # Create a model and add readers
    o = model(loglevel = 50)
    if configurations is not None:
        for key, value in configurations.items():
            o.set_config(key, value)
    o.add_reader(reader)
    # Seed

    o = Seed(o=o, model=model, lw_obj=lw_obj, num = num, rad = rad, start_t = start_t, 
             start_position=start_position, ship=ship, wdf = wdf, seed_type=seed_type,
             orientation=orientation, oil_type=oil_type, shpfile=shpfile)
    # Run
    if time_step is None:
        o.run(end_time=end_t, outfile = file_name)
    else:
        o.run(end_time=end_t, time_step=time_step, time_step_output=time_step, outfile = file_name)
            
    return o
