import geopandas as gpd
from pyproj import Transformer

from shapely import points, Polygon, MultiPoint
from src.post_processing.general_tools import extract_points


# =============== Transformers ============
# forward: 4326 -> 3857
fwd = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)

# inverse: 3857 -> 4326
inv = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)


def export_rotated(traj, file_name):
    from shapely.affinity import rotate
    from src.post_processing.rectangles import create_rectangle
    from src.post_processing.statistics import export_statistics
    import numpy as np
    
    stats = export_statistics(traj)
    mean_angle = stats['Angle to North ']['Mean']
    
    times = []
    rectangles = []
    
    lat0, lon0 = extract_points(traj, traj.result.time.values[0])
    start = fwd.transform(lon0[0], lat0[0])
    
    for time in traj.result.time.values[1:]:
        lats, lons = extract_points(traj, time)
        
        x, y = fwd.transform(lons, lats)
        
        rotated = rotate(MultiPoint(list(zip(x,y))), mean_angle, origin=points(start))
        
        coords_rotated = np.array([(p.x, p.y) for p in rotated.geoms])
        rect = create_rectangle(coords_rotated[:,1], coords_rotated[:,0])
        
        rect_rotated = rotate(rect, -mean_angle, origin=points(start))
        coords_rect = inv.transform(rect_rotated.exterior.xy[0], rect_rotated.exterior.xy[1]) 
        
        times.append(time)
        rectangles.append(Polygon(points(coords_rect[0], coords_rect[1])))

    gdf = gpd.GeoDataFrame({'time':times, 'geometry':rectangles}, crs="EPSG:4326") 
            
    file_name = file_name.replace('.nc', '_rotated_rectangle.geojson')  
    gdf.to_file(file_name, driver="GeoJSON")
    return


def export_minimal(traj, file_name):
    from shapely import minimum_rotated_rectangle
    
    times = []
    rectangles = []
    
    for time in traj.result.time.values[1:]:
        lats, lons = extract_points(traj, time)
        
        x, y = fwd.transform(lons, lats)
        rect = minimum_rotated_rectangle(MultiPoint(points(x, y))) 
        
        coords = inv.transform(rect.exterior.xy[0],
                               rect.exterior.xy[1])
        
        times.append(time)
        rectangles.append(Polygon(points(coords[0], coords[1])))
    
    gdf = gpd.GeoDataFrame({'time':times, 'geometry':rectangles}, crs="EPSG:4326") 

    file_name = file_name.replace('.nc', '_minimal_rectangle.geojson')  
    gdf.to_file(file_name, driver="GeoJSON")
    return