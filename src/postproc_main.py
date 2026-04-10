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
    'Rectangles': ('src.post_processing.rectangles', 'export_rectangles'),
    'ConvexHull': ('src.post_processing.convexhull', 'export_convex_hull'),
    'Trapezoids': ('src.post_processing.trapezoid', 'export_trapezoids'),
}

# global var to store imported modules and do not reimport them
_cached_handlers = {}

# handler imports module upon request 
def _get_handler(key):
    if key in _cached_handlers:
        return _cached_handlers[key]

    module_name, func_name = EXPORT_HANDLERS[key]
    module = importlib.import_module(module_name)
    handler = getattr(module, func_name)
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

        handler = _get_handler(key)
        
        if key == 'ConvexHull':
            gdf = handler(traj)
            geojson_name = file_name.replace('.nc', '_convex_hull.geojson')
            gdf.to_file(geojson_name, driver='GeoJSON')
        else:
            handler(traj, file_name)

    return