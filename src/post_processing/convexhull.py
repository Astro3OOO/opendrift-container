from src.post_processing.general_tools import extract_points
from shapely.geometry import Point, MultiPoint
import geopandas as gpd
from shapely import convex_hull

"""
    Convex hull polygon
"""
def export_convex_hull(traj, plot_time = None):
    lats, lons = extract_points(traj, plot_time)
    
    points = []
    
    for lat, lon in zip(lats, lons):
        points.append(Point((lon, lat)))
        
    pol = convex_hull(MultiPoint(points))
    
    gdf = gpd.GeoDataFrame({'geometry':[pol]}, crs="EPSG:4326")
    return gdf
