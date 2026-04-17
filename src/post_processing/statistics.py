import pandas as pd
from pyproj import Geod
import numpy  as np

"""
    Trajectory statistics
"""
def export_statistics(traj, file_name):
    
    traj_length, _, speed = traj.get_trajectory_lengths()

    geod = Geod(ellps="WGS84")

    lon1 = traj.result.lon.isel(time=0).values
    lat1 = traj.result.lat.isel(time=0).values
    lon2 = traj.result.lon.isel(time=-1).values
    lat2 = traj.result.lat.isel(time=-1).values

    azimuths, _, displacement = geod.inv(lon1, lat1, lon2, lat2)
    
    dist_stat = {'Mean': displacement.mean(),
                 'Max': displacement.max(),
                 'Min': displacement.min(),
                 'Spread': displacement.max() - displacement.min()}
    len_stat = {'Mean': traj_length.mean(),
                 'Max': traj_length.max(),
                 'Min': traj_length.min(),
                 'Spread': traj_length.max() - traj_length.min()}
    
    rads = np.deg2rad(90 - azimuths)
    mean_sin = np.sin(rads).mean()
    mean_cos = np.cos(rads).mean()
    spread = azimuths.max() - azimuths.min()
    
    angle_stat = {'Mean': 90 - np.rad2deg(np.arctan2(mean_sin, mean_cos)),
                 'Max': azimuths.max(),
                 'Min': azimuths.min(),
                 'Spread': spread if spread < 180 else 360 - spread,
                 'R (Directional coherence)': np.sqrt(mean_sin**2 + mean_cos**2)}
    
    df = pd.DataFrame({
        'Displacement (m)':dist_stat,
        'Trajectory length (m)':len_stat,
        'Angle to North ':angle_stat
        })
    
    # file_name = file_name.replace('.nc', '_statistics.xlsx') 
    # df.to_excel(file_name)
    return df
