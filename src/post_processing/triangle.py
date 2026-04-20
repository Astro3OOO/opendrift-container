from pyproj import Geod
import geopandas as gpd

import numpy as np
from src.post_processing.general_tools import extract_points
from src.post_processing.statistics import export_statistics
from shapely import points, Polygon


def export_triangle(traj, file_name):
    
    # ========= extract statistics for triangles ======= 
    stats = export_statistics(traj)
    a_max = stats['Angle to North ']['Max']
    a_min = stats['Angle to North ']['Min']
    dist = stats['Displacement (m)']['Max']

    geod = Geod(ellps="WGS84")
    lats, lons = extract_points(traj, traj.result.time.values[0])
    lat1, lon1 = lats[0], lons[0]
    
    cos = np.cos(np.deg2rad((a_max - a_min)/2))
    scale = 1/cos if abs(cos) > 0.1 else 10

    lon2, lat2, _ = geod.fwd(lon1, lat1, a_max, scale*dist)
    lon3, lat3, _ = geod.fwd(lon1, lat1, a_min, scale*dist)
    
    coords = points([lon1, lon2, lon3, lon1],[lat1, lat2, lat3, lat1])
    
    gdf = gpd.GeoDataFrame({'geometry':[Polygon(coords)]}, crs="EPSG:4326")
    
    file_name = file_name.replace('.nc', '_triangle.geojson')  
    gdf.to_file(file_name, driver="GeoJSON")
    return