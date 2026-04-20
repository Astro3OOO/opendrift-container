import pandas as pd
from pyproj import Geod
import numpy  as np
from src.post_processing.general_tools import extract_points

"""
    Trajectory statistics
"""
def export_statistics(traj):
    # Projection
    geod = Geod(ellps="WGS84")

    # Start & End points
    lat1, lon1 = extract_points(traj, traj.result.time.values[0])
    
    lat2, lon2 = extract_points(traj, traj.result.time.values[-1])

    # Displacement & trajectory lenght calculation + statistics
    traj_length, _, _ = traj.get_trajectory_lengths()
    _, _, displacement = geod.inv(lon1, lat1, lon2, lat2)
    
    dist_stat = {'Mean': displacement.mean(),
                 'Max': displacement.max(),
                 'Min': displacement.min(),
                 'Spread': displacement.max() - displacement.min()
                }
    
    len_stat = {'Mean': traj_length.mean(),
                'Max': traj_length.max(),
                'Min': traj_length.min(),
                'Spread': traj_length.max() - traj_length.min()
                }
    
    # select all points except start
    lat3, lon3 = extract_points(traj, slice(traj.result.time.values[1],
                                            traj.result.time.values[-1]))    
    
    # expand and reshape start point 
    m = len(lon3)//len(lon1)
    lon1_ext = lon1.repeat(m)
    lat1_ext = lat1.repeat(m)
    
    # Calculate directions + statistics
    azimuths, _, _ = geod.inv(lon1_ext, lat1_ext, lon3, lat3)
    
    rads = np.deg2rad(90 - azimuths)
    mean_sin = np.sin(rads).mean()
    mean_cos = np.cos(rads).mean()
    spread = azimuths.max() - azimuths.min()
    
    angle_stat = {'Mean': 90 - np.rad2deg(np.arctan2(mean_sin, mean_cos)),
                 'Max': azimuths.max(),
                 'Min': azimuths.min(),
                 'Spread': spread if spread < 180 else 360 - spread,
                 'R (Directional coherence)': np.sqrt(mean_sin**2 + mean_cos**2)}
    
    # Return statistics dataframe
    df = pd.DataFrame({
            'Displacement (m)':dist_stat,
            'Trajectory length (m)':len_stat,
            'Angle to North ':angle_stat
        })
    
    # file_name = file_name.replace('.nc', '_statistics.xlsx') 
    # df.to_excel(file_name)
    return df
