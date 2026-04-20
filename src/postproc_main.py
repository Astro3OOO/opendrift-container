import importlib
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


EXPORT_HANDLERS = {
    'POC': ('src.post_processing.poc', 'export_poc_geojson'),
    'Polygons': ('src.post_processing.polygons', 'export_plume_polygons'),
    'Picture': ('src.post_processing.picture', 'export_traj_picture'),
    'Rectangles': ('src.post_processing.rotation', 'export_rotated'),
    'ConvexHull': ('src.post_processing.convexhull', 'export_convex_hull'),
    'Triangle':('src.post_processing.triangle', 'export_triangle'),
}

# global var to store imported modules and do not reimport them
_cached_handlers = {}

# handler imports module upon request 
def _get_handler(key):
    if key in _cached_handlers:
        return _cached_handlers[key]

    module_name, func_name = EXPORT_HANDLERS[key]
    try:
        logging.info(f'Lazy loading module {module_name}')
        module = importlib.import_module(module_name)
        handler = getattr(module, func_name)
    except ImportError as e:
        logging.error(f'Module {module} is not available: {e}')
        return      
    except Exception as e:
        logging.error(f'Hadler import failed: {e}')   
        return 
    
    logging.info(f'Module {module_name} imported successfully!')
    _cached_handlers[key] = handler
    return handler


"""
    main function
"""
# loop over selected post processing formats
def postprocess_trajectory(traj, file_name, formats):
    for key, enabled in formats.items():
        if not enabled or key not in EXPORT_HANDLERS:
            continue
        logging.info(f'Running post processing: {key}')
        handler = _get_handler(key)
        if handler:
            try:
                if key == 'ConvexHull':
                    gdf = handler(traj)
                    geojson_name = file_name.replace('.nc', '_convex_hull.geojson')
                    gdf.to_file(geojson_name, driver='GeoJSON')
                else:
                    handler(traj, file_name)
            except Exception as e:
                logging.error(f'Post processing {key} failed, error: {e}')

    return