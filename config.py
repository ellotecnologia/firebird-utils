import logging

def setup_config(debug=False):
    logging_level = logging.DEBUG if debug else logging.INFO
        
    logging.basicConfig(
        level=logging_level,
        #format='%(asctime)s - %(message)s',
        format='=> %(message)s',
        datefmt='%H:%M:%S')
