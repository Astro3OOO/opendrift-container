from opendrift.models.oceandrift import OceanDrift
from opendrift.models.leeway import Leeway
from opendrift.models.shipdrift import ShipDrift
from opendrift.models.openoil import OpenOil
import datetime as dt
import pandas as pd
import numpy as np
import os
from opendrift.readers.reader_netCDF_CF_generic import Reader
import logging
from general_tools import prepare_time, resolve_path


logger_cop = logging.getLogger('copernicusmarine') 
logger_cop.setLevel(logging.INFO)

MODEL_DICT = {'OceanDrift':OceanDrift,
              'Leeway':Leeway,
              'ShipDrift':ShipDrift,
              'OpenOil': OpenOil}
 
def seed(o, model, lw_obj, start_position, start_t, num, rad, ship, wdf, seed_type, orientation, oil_type, shpfile=None):
    params = dict(
        lat = start_position[0],
        lon = start_position[1],
        number = num,
        radius = rad,
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

def _transform_forcings(configurations, windir=0, windspeed=0, currentdir=0, currentspeed=0):
    
    x_wind = windspeed * np.sin(np.deg2rad(windir))
    y_wind = windspeed * np.cos(np.deg2rad(windir))
    x_sea = currentspeed * np.sin(np.deg2rad(currentdir))
    y_sea = currentspeed * np.cos(np.deg2rad(currentdir))
    
    configurations.update({
                            'environment:fallback:x_sea_water_velocity': x_sea,
                            'environment:fallback:y_sea_water_velocity': y_sea,
                            'environment:fallback:x_wind': x_wind,
                            'environment:fallback:y_wind': y_wind
                        })
    logging.info('Forcings transformed: [winddir, windspeed] - > [x_wind, y_wind] \n [currentdir, currentspeed] - > [x_sea_water_velocity, y_sea_water_velocity]')
    return configurations

def update_start(o):
    if o.result != None:
        res = o.result.sel(time = o.result.time[-1])
        
        start_position = [res.lat.values, res.lon.values]
        start_t = prepare_time(str(res.time.values))
        logging.info('Start conditions updated!')
        return start_position, start_t
    else:
        logging.error('No result of prerun were provided. Fallback to original values.')
        return None, None

def run_sim(model, configurations, start_position, start_t, num, rad, 
           seed_type, ship, wdf, orientation, oil_type, lw_obj, shpfile, time_step,
           duration = None, reader = [], file_name = None, end_t=None):
    
    o = model(loglevel = 20)
        
    if configurations is not None:
        for key, value in configurations.items():
            o.set_config(key, value)
            logging.info(f'Configuration used: {key} with value = {value}')
            
    o.add_reader(reader)        
    logging.info(f'Reader used : {reader}')
    
    o = seed(o=o, model=model, lw_obj=lw_obj, num = num, rad = rad, start_t = start_t, 
            start_position=start_position, ship=ship, wdf = wdf, seed_type=seed_type,
            orientation=orientation, oil_type=oil_type, shpfile=shpfile)
    logging.info(f'Seeding {model} {num} particles at {start_t} ')
    
    # duration OR end_time is given
    if duration is None and end_t is not None:
        duration = end_t - start_t
    logging.info(f'Simulation duration {duration}')
    
    # as minimal as possible if simulation is short 
    if duration < pd.Timedelta(seconds = time_step):
            time_step = 60
            
    if duration:  
        logging.info('Run started.')      
        o.run(duration = duration, time_step=time_step, time_step_output=time_step, outfile = file_name) 
        logging.info('Run ended.')      
    else:
        logging.error(f'Unable to run simulation with duration: {duration}')
    
    return o    

# Check main requirments
def _check_requirments(start_position, datasets, model):
    flag = True
    
    if start_position == None:
        logging.error('Start position is required')
        flag = False
    if datasets == None:
        logging.error('At least one dataset is required')
        flag = False
    if model not in MODEL_DICT.keys():
        logging.error(f'Model {model} is not supported. Choose one of the following: {list(MODEL_DICT.keys())}')
        flag = False
        
    return flag
    

def simulation(lw_obj=1, model='OceanDrift', start_position=None, start_t=None,
               end_t=None, datasets=None, std_names=None, num=100, prerun = False,
               rad=0, ship=[62, 8, 10, 5], wdf=0.02, orientation = 'random', forcings = [0,0,0,0],
               seed_type='elements', time_step = 3600, duration = None,
               configurations = None, file_name = None, oil_type='GENERIC BUNKER C', shpfile=None):
    
    if not _check_requirments(start_position, datasets, model):
        raise Exception('Required parametrs missing. ') 
       
    model = MODEL_DICT[model]   
    
    # Create readers
    if type(datasets) == list:
        reader = [Reader(ds, standard_name_mapping=std_names) for ds in datasets]
    else:
        reader = Reader(datasets, standard_name_mapping=std_names)
        
    # Prepare start and end times
    start_t = prepare_time(start_t, reader, 'start')
    end_t = prepare_time(end_t, reader, 'end')
    
    if file_name == None:
        m = str(model).split('.')[-1][:-2]
        t_now = dt.datetime.now().strftime("%Y-%m-%d_%H%M")
        t_strt = start_t.strftime("%Y-%m-%d_%H%M")
        file_name = f'{m}_{t_strt}_{t_now}.nc'
    
    # Make correct OUTPUT dir (abs/rel path) depending on where the code is running
    output_dir = resolve_path("OUTPUT")
      
    file_name = os.path.join(output_dir, file_name)
    logging.info(f'Filename for final result {file_name}')
    # Create a model and add readers
    
    constant_params = dict(
        model=model,
        seed_type=seed_type,
        ship=ship, 
        wdf=wdf, 
        lw_obj=lw_obj,
        orientation=orientation, 
        oil_type=oil_type,
        shpfile=shpfile,
        time_step=time_step,
        num=num,
        rad=rad
    )
    
    if prerun:
        logging.info('Prerun started.')
        cfgs = _transform_forcings(configurations,
                                 windir = forcings[0], 
                                 windspeed = forcings[1],
                                 currentdir = forcings[2],
                                 currentspeed = forcings[3])
        cfgs.update(configurations)
        o = run_sim(configurations=cfgs, start_position=start_position,
                   start_t=start_t, duration=duration, **constant_params)    
        res = update_start(o)
        if all(r != None for r in res):
            start_position, start_t = res
            logging.info('Prerun completed, success!')
        else:
            logging.warning('Prerun didnot complete successfully, fallback to original values')
            

    o = run_sim(configurations=configurations, start_position=start_position,
               start_t=start_t, end_t=end_t, reader=reader, file_name=file_name, 
               **constant_params)  
    return o, file_name
