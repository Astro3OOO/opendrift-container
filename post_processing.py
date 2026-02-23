import os
import json
import numpy as np
from shapely.ops import unary_union
import geopandas as gpd
from shapely.geometry import Point, Polygon
import logging
from general_tools import resolve_path


'''
    Probability of containtement rectangles
'''
def _merge_polygons_by_level(gdf, colorscale = None):
    
    if not colorscale:
        return
    
    levels = colorscale.get("levels")
    colors = colorscale.get("colors")
    
    if not levels or not colors:
        return

    try:
        values = gdf['poc']
        polygons = gdf['geometry']
    except Exception as e:
        logging.error(f'Error occure: {e}')
        return
    
    level_to_color = {i: colors[i] for i in range(len(levels))}
    n_levels = len(levels) - 1
    
    level_indices = np.full(values.shape, -1) # prevent NaN error

    level_indices[values <= levels[0]] = 0
    for i in range(n_levels):
        mask = (values >= levels[i]) & (values < levels[i + 1])
        level_indices[mask] = i + 1
    
    polygons_list = []

    for i in set(level_indices.tolist()):
        lvl = polygons[level_indices == i]
        merged = unary_union(lvl)
        polygons_list.append({'geometry': merged, 'level': i})

    gdf_merged = gpd.GeoDataFrame(polygons_list, crs="EPSG:4326")
    gdf_merged['color'] = gdf_merged['level'].map(level_to_color)
    gdf_merged = gdf_merged[['geometry', 'color']]
    
    return gdf_merged

def _compute_poc(df):   
    # calc area of each cell
    area = df.to_crs(epsg=3857)['geometry'].area
    # find relative value to cell size
    dc = df['values'] / area
    # find absolute value to all are size
    dg = sum(df['values']) / sum(area)
    # calc the quoient relative/absolute
    r = dc/dg 
    return 1 - np.exp(-r)


def _build_poc_grid(lat_t, lon_t, n_bins = 10):
    
    #define boarder
    max_lat, min_lat = lat_t.max(), lat_t.min()
    max_lon, min_lon = lon_t.max(), lon_t.min()
    
    # transform trajectory to geograhical Point() objects
    coords = []
    for lat, lon in zip(lat_t, lon_t):
        coords.append(tuple([lat,lon]))
    points = [Point(xy) for xy in coords]
    
    # linearspaces for regular grid cells
    lat = np.linspace(min_lat - 0.00001, max_lat+ 0.00001, n_bins+1)
    lon = np.linspace(min_lon- 0.00001, max_lon+ 0.00001, n_bins+1)
    
    polygons = []
    dens = []
    for i in range(n_bins):
        for j in range(n_bins):
            # add polygons to plot (on map used XY projection : (lon, lat))
            cord_lst =  [(lon[j], lat[i]), (lon[j+1], lat[i]), (lon[j+1], lat[i+1]), (lon[j], lat[i+1]), (lon[j], lat[i])]
            poly = Polygon(cord_lst)
            polygons.append(poly)
            
            # but trajectory (points) are stored as (lat, lon)
            # calculate how many points each polygons contains 
            cord_lst_check = [(lat[i], lon[j]), (lat[i], lon[j+1]), (lat[i+1], lon[j+1]),(lat[i+1], lon[j]), (lat[i], lon[j])]
            poly_check = Polygon(cord_lst_check)
            dens.append(sum(poly_check.contains(points)))

    # store results in dataframe
    gdf = gpd.GeoDataFrame({'geometry':polygons, 'values':dens}, crs="EPSG:4326")
    gdf['poc'] = _compute_poc(gdf)
    
    gdf = gdf[gdf['values'] != 0].reset_index()
        
    return gdf

def export_poc_geojson(traj, file_name, plot_time = None):
    if plot_time:
        res = traj.result.sel(time = plot_time)
    else:
        res = traj.result.sel(time = traj.result.time[-1])
    
    lats = res.lat.values.flatten()
    lons = res.lon.values.flatten()
    
    gdf = _build_poc_grid(lats, lons, 10) # maybe add auto merging n_bins number in the future
    
    with open('DATA/colorscale.json', 'r') as f:
        data = json.load(f)
        
    file_name = file_name.replace('.nc', '_poc.geojson')    
    gdf_merged = _merge_polygons_by_level(gdf.copy(), data.get('POC'))
    gdf_merged.to_file(file_name, driver="GeoJSON")

    return

"""
    Plume triangle 
"""
def export_plume_triangle(traj, file_name):
    return


"""
    Trajectory picture
"""
def export_traj_picture(traj, file_name, plot_time = None):
    file_name = file_name.replace('.nc', '.png')    
    traj.plot(filename = file_name)
    return

"""
    main function
"""
def postprocess_trajectory(traj, file_name, formats):
    output_dir = resolve_path("OUTPUT")  
    file_name = os.path.join(output_dir, file_name)
    
    if formats.get('POC'):
        export_poc_geojson(traj, file_name)
    if formats.get('Triangle'):
        export_plume_triangle(traj, file_name)
    if formats.get('Picture'):
        export_traj_picture(traj, file_name)
        
    return