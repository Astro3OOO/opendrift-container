from src.post_processing.general_tools import extract_points
from shapely import convex_hull, points, MultiPoint
import geopandas as gpd

"""
    Convex hull polygon
"""
def export_convex_hull(traj, plot_time = None):
    lats, lons = extract_points(traj, plot_time)
        
    geom = MultiPoint(points(lons, lats))    
    pol = convex_hull(geom)
    
    gdf = gpd.GeoDataFrame({'geometry':[pol]}, crs="EPSG:4326")
    return gdf
