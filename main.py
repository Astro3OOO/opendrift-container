from config_verification import verify_config_file
from case_study_tool import simulation
from dataset_verification import validate_dataset
import sys
import json 
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def resolve_config_path(cfg: str) -> str:
    if os.path.isabs(cfg):
        return cfg

    if cfg.startswith("INPUT/") or cfg.startswith("/opendrift-container/INPUT/"):
        return cfg

    return os.path.join("INPUT", cfg)

def main() -> int:
    if len(sys.argv) < 2:
        logging.error("Usage: python main.py <config.json>")
        return 1

    raw_path = sys.argv[1]
    
    input_file = resolve_config_path(raw_path)
    if not os.path.exists(input_file):
        logging.error(f"Config file '{input_file}' does not exist.")
        return 2

    logging.info("Validating input...")
    is_valid, sim_vars, data_vars = verify_config_file(input_file)

    if not is_valid:
        logging.error("Validation failed.")
        return 3

    vocab_path = "DATA/VariableMapping.json"
    if not os.path.exists(vocab_path):
        logging.error(f"Vocabulary file missing: {vocab_path}")
        return 4

    try:
        with open(vocab_path, "r") as f:
            vocabulary_data = json.load(f)
    except json.JSONDecodeError:
        logging.error("Vocabulary JSON format error.")
        return 5

    logging.info("Input valid. Preparing datasets...")
    
    '''
        SELECTION OF DATA FILES
    Will edit data_vars['folder'] from mounted /DATSETS to symlinked /SELECTED
    '''
    
    select = sim_vars.pop('selection')
    start_t = sim_vars.get('start_t')
    end_t = sim_vars.get('end_t')
    if select:
        try:
            from dataset_selection import select_dataset
            
            folder = data_vars.get('folder')
            data_vars.update(select_dataset(start_t, end_t, folder))
        except ImportError as e:
            logging.error(f'Module dataset_selection not available: {e}')
            return 10
        except Exception as e:
            logging.exception(f'Dataset selection failed: {e}')
            return 10
        logging.info(f'Data is selected. Reading...')
        
    try:
        from dataset_preparation import prepare_dataset
        
        ds = prepare_dataset(**data_vars)
    except ImportError as e:
        logging.error(f'Module dataset_preparation not available: {e}')
        return 6
    except Exception as e:
        logging.exception(f"Dataset preparation failed: {e}")
        return 6

    logging.info("Dataset ready. Running simulation...")

    vc = sim_vars.pop("vocabulary")
    if vc not in vocabulary_data:
        logging.error(f"Requested vocabulary '{vc}' not found.")
        return 7
    
    empty = sim_vars.pop('allow_empty_ds')
    if not validate_dataset(ds, start_t, end_t, empty):
        logging.error('Dataset time validation failed. ')
        return 8

    post_proc = sim_vars.pop('postprocessing')
    try:
        o, file_name = simulation(datasets=ds, std_names=vocabulary_data[vc], **sim_vars)
    except Exception as e:
        logging.exception(f"Simulation failed: {e}")
        return 9
    
    if post_proc:
        try:
            from post_processing import postprocess_trajectory
            
            postprocess_trajectory(o, file_name, post_proc)
        except ImportError as e:
            logging.error(f'Module dataset_preparation not available: {e}')
            return 11
        except Exception as e:
            logging.exception(f"Postprocessing failed: {e}")
            return 11

    print("Simulation completed successfully.")
    return 0


if __name__ == "__main__":
    exit(main())