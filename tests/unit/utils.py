import os.path

from tests.utils import load_json


def load_data(file_path):
    abspath_for_this_script = os.path.abspath(__file__)
    file_dir = os.path.dirname(abspath_for_this_script)
    base_dir_path = os.path.join(file_dir, "data")

    return load_json(base_dir_path, file_path)
