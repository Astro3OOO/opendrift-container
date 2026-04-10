"""
    General purpose function
"""
def extract_points(traj, plot_time = None) :
    if plot_time:
        res = traj.result.sel(time = plot_time)
    else:
        res = traj.result.isel(time = -1)
    
    lats = res.lat.values.flatten()
    lons = res.lon.values.flatten()
    return lats, lons