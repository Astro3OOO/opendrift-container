from src.post_processing.general_tools import extract_points
import geopandas as gpd
from shapely.geometry import Polygon
import pandas as pd

"""
    Probability rectangles
"""
def create_rectangle(lats, lons):    
    max_lat, min_lat = lats.max(), lats.min()
    max_lon, min_lon = lons.max(), lons.min()
    
    coords = [(min_lon, min_lat), (max_lon, min_lat),
              (max_lon, max_lat), (min_lon, max_lat), (min_lon, min_lat)]
    
    return Polygon(coords)

def export_rectangles(traj, file_name):
    times = []
    rectangles = []
    
    for time in traj.result.time.values[1:]:
        if pd.to_datetime(time).minute == 0:    # optional, ensure we select only round hours
            plot_time = slice(traj.result.time.values[0], time)
            times.append(time)
            lats, lons = extract_points(traj, plot_time)
            rectangles.append(create_rectangle(lats, lons))
    
    gdf = gpd.GeoDataFrame({'time':times, 'geometry':rectangles}, crs="EPSG:4326") 
            
    file_name = file_name.replace('.nc', '_rectangles.geojson')  
    gdf.to_file(file_name, driver="GeoJSON")
    return
