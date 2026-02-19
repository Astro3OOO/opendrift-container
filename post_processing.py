import os
import pandas as pd
import json
import numpy as np
from shapely.geometry import box
from shapely.ops import unary_union
import geopandas as gpd
from shapely.geometry import Point, Polygon
import logging
from case_study_tool import ResolvePath



def LabelAndConcatenate(gdf, colorscale = None):
    
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

def POC(df):   
    # calc area of each cell
    area = df.to_crs(epsg=3857)['geometry'].area
    # find relative value to cell size
    dc = df['values'] / area
    # find absolute value to all are size
    dg = sum(df['values']) / sum(area)
    # calc the quoient relative/absolute
    r = dc/dg 
    return 1 - np.exp(-r)


def MakePolygonsPOC(pos, bin = 10):
    
    #define boarder
    max_lat, min_lat = pos[0].max(), pos[0].min()
    max_lon, min_lon = pos[1].max(), pos[1].min()
    
    # transform trajectory to geograhical Point() objects
    coords = []
    for lat, lon in zip(pos[0], pos[1]):
        coords.append(tuple([lat,lon]))
    points = [Point(xy) for xy in coords]
    
    # linearspaces for regular grid cells
    lat = np.linspace(min_lat - 0.00001, max_lat+ 0.00001, bin+1)
    lon = np.linspace(min_lon- 0.00001, max_lon+ 0.00001, bin+1)
    
    polygons = []
    dens = []
    for i in range(bin):
        for j in range(bin):
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
    gdf['poc'] = POC(gdf)
    
    gdf = gdf[gdf['values'] != 0].reset_index()
        
    return gdf

def create_poc_geojson(traj, file_name, plot_time = None):
    if plot_time:
        res = traj.result.sel(time = plot_time)
    else:
        res = traj.result.sel(time = traj.result.time[-1])
    
    lats = res.lat.values.flatten()
    lons = res.lon.values.flatten()
    pos = [lats, lons]
    
    gdf = MakePolygonsPOC(pos, 10) # maybe add auto merging bin number in the future
    
    with open('DATA/colorscale.json', 'r') as f:
        data = json.load(f)
        
    file_name = file_name.replace('.nc', '.geojson')    
    output_dir = ResolvePath("OUTPUT")  
    file_name = os.path.join(output_dir, file_name)
    gdf_merged = LabelAndConcatenate(gdf.copy(), data.get('POC'))
    gdf_merged.to_file(file_name, driver="GeoJSON")

    return