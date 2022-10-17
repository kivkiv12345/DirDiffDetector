#!/usr/bin/python3

""" Sort cached duplicate results by size. """

from json import dump, load
from ast import literal_eval


JSON_NAME = 'json_cache.json'


def main():

    with open(JSON_NAME, 'r') as json_file:
        json_data = load(json_file)
    # {(filename, filesize): [locations, ...]}
    all_files: dict[tuple[str, float], set[str]] = {literal_eval(key): set(value) for key, value in json_data['all_files'].items()}

    # Dictionaries in Python > 2.7 are ordered, so let's reorder the dict by size
    size_ordered = {key: all_files[key] for key in reversed(sorted((key for key in all_files.keys()), key=lambda x: x[1]))}
    print(size_ordered)


if __name__ == '__main__':
    main()
