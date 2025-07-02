from pathlib import Path

# Nested loop, for each dir go through every file and get the link


def get_all_files_in_dir(dir_path):
    path_dictionary = {}
    if Path(dir_path).exists():
        for dir in Path(dir_path).iterdir():
            if dir.is_dir():
                for file in dir.iterdir():
                    if dir.name in path_dictionary:
                        path_dictionary[dir.name].append(file.name)
                    else:
                        path_dictionary[dir.name] = [file.name]
    return path_dictionary


get_all_files_in_dir("airflow/data")
