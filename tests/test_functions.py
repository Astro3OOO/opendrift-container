from config_verification import verify_config_file
from case_study_tool import simulation
from dataset_preparation import prepare_dataset


def test_config_verification():
    valid, sim_vars, data_vars, settings = verify_config_file("INPUT/input_test.json")
    assert valid is True

def test_data_preparation():
    data_vars =  {
        "start_t": "2024-06-01 00:00:00",
        "end_t": "2024-06-03 00:00:00",
        "border": [56, 59, 21, 25],
        "copernicus": True,
        "user":None,
        "pword":None
    }
    try:
        ds = prepare_dataset(**data_vars)
    except:
        ds = None
    assert ds is not None

def test_simulation():
    sim_vars = {
        "model": "OceanDrift",
        "start_position": [57.5, 23.7],
        "start_t": "2024-06-01 00:00:00",
        "end_t": "2024-06-01 06:00:00",
        "num": 1,
        "rad": 0,
        "wdf": 0.03,
        "time_step": 3600,
        "file_name": "test_output.nc"
    }
    o, filename = simulation(datasets=[], **sim_vars)    
    assert o is not None

