import numpy as np

"""
    Trajectory statistics
"""

def export_statistics(traj):
    
    traj_length, _, speed = traj.get_trajectory_lengths()
    start_poz = np.array([traj.result.sel(time = traj.result.time[0].values).lon.values,
                          traj.result.sel(time = traj.result.time[0].values).lat.values])
    end_poz = np.array([traj.result.sel(time = traj.result.time[-1].values).lon.values,
                          traj.result.sel(time = traj.result.time[-1].values).lat.values])
    return
