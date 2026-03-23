from opendrift.models.leeway import Leeway as BaseLeeway

class Leeway(BaseLeeway):
    required_variables = {
        **BaseLeeway.required_variables,
        'sea_water_temperature': {
            'fallback': 10,
            'important': False
        },
    }
    
