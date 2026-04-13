from src.post_processing.general_tools import extract_points
import geopandas as gpd
from shapely.geometry import Polygon
import pandas as pd
import numpy as np

"""
    Probability trapezoid
"""
def _create_trapezoid(traj, plot_time=None):
    lats, lons = extract_points(traj, plot_time)
    
    max_lat_idx, min_lat_idx =  np.argmax(lats), np.argmin(lats)
    max_lon_idx, min_lon_idx =  np.argmax(lons), np.argmin(lons)  
    
    coords = [(lons[i],lats[i]) for i in [max_lat_idx, max_lon_idx, min_lat_idx, min_lon_idx, max_lat_idx]]
 
    return Polygon(coords)

def export_trapezoids(traj, file_name):
    times = []
    trapezoids = []
    
    for time in traj.result.time.values[1:]:
        if pd.to_datetime(time).minute == 0:    # optional, ensure we select only round hours
            plot_time = slice(traj.result.time.values[0], time)
            times.append(time)
            trapezoids.append(_create_trapezoid(traj, plot_time))
    
    gdf = gpd.GeoDataFrame({'time':times, 'geometry':trapezoids}, crs="EPSG:4326") 
            
    file_name = file_name.replace('.nc', '_trapezoids.geojson')  
    gdf.to_file(file_name, driver="GeoJSON")
    return 