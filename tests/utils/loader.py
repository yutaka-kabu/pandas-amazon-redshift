import os.path


def load_json(base_dir_path, file_path):
    import json

    full_path = os.path.join(base_dir_path, file_path)
    with open(full_path, "rb") as fp:
        payload = fp.read().decode("utf-8")
        return json.loads(payload)
