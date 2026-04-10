from src.post_processing.convexhull import export_convex_hull
import pandas as pd

"""
    Plume polygons
"""
def export_plume_polygons(traj, file_name):
    gdfs = {}
    
    for time in traj.result.time.values[1:]:
        if pd.to_datetime(time).minute == 0:    # optional, ensure we select only round hours
            plot_time = [traj.result.time.values[0], time]
            gdfs.update({time:export_convex_hull(traj, plot_time)})
            
    gdf = pd.concat(gdfs,  names=["time"]).reset_index()
    gdf = gdf[['time', 'geometry']]
    
    file_name = file_name.replace('.nc', '_plume_triangles.geojson')  
    gdf.to_file(file_name, driver="GeoJSON")
    return 