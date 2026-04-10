import pandas as pd
from pyproj import Geod

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
    
    df = pd.DataFrame({
        'Displacement (m)':displacement,
        'Trajectory length (m)':traj_length,
        'Angle to North (degrees)':azimuths
        })
    stats = df.describe()
    
    file_name = file_name.replace('.nc', '_statistics.xlsx') 
    stats.to_excel(file_name)
    return 
