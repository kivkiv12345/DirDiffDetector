#!/usr/bin/python3

""" Find differences between files in multiple directories. """

from time import time
from signal import SIGINT
from sys import getsizeof
from threading import Event
from json import dump, load
from ast import literal_eval
from typing import NamedTuple
from traceback import format_exc
from os import listdir, kill, stat
from json.decoder import JSONDecodeError
from concurrent.futures import ThreadPoolExecutor
from os.path import isfile, isdir, islink, abspath, split, getsize, join


JSON_NAME = 'json_cache.json'

# {(filename, filesize): [locations, ...]}
all_files: dict[tuple[str, float], set[str]] = {}
current_dir: str = ''


class Arguments(NamedTuple):
    directory: str
    exit_event: Event
    # mydict: dict[str, float]
    # their_dicts: tuple[dict[str, float], ...]
    # is_main: bool = True


def worker(args: Arguments):
    """ Starts recurring into specified path in search of duplicate files. """

    def dir_worker(directory: str):
        """ Nested file tree walker, so we can access outer arguments """

        global current_dir  # Ensure we assign to global object

        workdir = *(join(directory, path) for path in listdir(directory)),

        # Filter out finished directories
        if current_dir.startswith(directory):
            # substr = '/'.join(current_dir.split('/')[:directory.count('/')+1])
            start = current_dir.index(directory)+len(directory)+1
            substr = current_dir[:(None if (idx := current_dir.find('/', start)) == -1 else idx)]
            try:
                idx = workdir.index(substr)
            except ValueError:  # Operating on files at deepest level
                pass
            else:  # Operating on directory
                workdir = (
                    *(join(directory, path) for path in workdir[:idx] if stat(path).st_mtime > time()),
                    *(join(directory, path) for path in workdir[idx:]),
                )

        # if current_dir.startswith(directory) and current_dir != directory:
        #
        #     # Find the index of the current directory in os.listdir()
        #     start = current_dir.index(directory)
        #     end = current_dir.index('/', directory.rindex('/'))  # Error here when directory == current_dir
        #     substr = current_dir[start:end]
        #     idx = workdir.index(substr)
        #
        #     # Attempt to add previously scanned directories to the pool,
        #     # if their change time is newer than the last scan time.
        #     # This doesn't quite work, nested directories don't update the change time of their outer directories.
        #     workdir = (
        #         *(join(directory, path) for path in workdir[:idx] if stat(path).st_mtime > time()),
        #         *(join(directory, path) for path in workdir[idx:]),
        #     )

        for path in workdir:
            if isdir(path):
                try:
                    dir_worker(path)
                    current_dir = path
                    if args.exit_event.is_set():
                        raise SystemExit
                    else:
                        # Print progress whenever a directory finishes
                        gb_wasted = sum(key[1] * len(lst) - 1 for key, lst in all_files.items()) * 0.000000001
                        duplicates = sum(len(lst) - 1 for lst in all_files.values())
                        total_files = sum(len(lst) for lst in all_files.values())

                        print(f"\x1b[KSpace wasted: {gb_wasted:.0f} GB\tDuplicates: {duplicates}\tTotal files: {total_files} Current:\n\x1b[K{path}\033[2A")
                except PermissionError:
                    continue
            elif isfile(path):
                try:
                    filesize = getsize(path)
                except (FileNotFoundError, OSError):  # Symlink
                    continue

                filename = split(path)[-1]
                filetuple = (filename, filesize)

                if filetuple in all_files:
                    all_files[filetuple].add(path)
                    # print(f"Multiple '{filename}' at {all_files[filetuple]}")
                else:  # Add the current file
                    all_files[filetuple] = {path}

    try:  # Print any exception, so ThreadPoolExecutor() doesn't silently eat it.
        dir_worker(abspath(args.directory))  # Start initial directory scan
    except Exception as e:
        print(format_exc())


def main(*dirs, main_dir_idx: int = None):

    # {filename: filesize for file in dir}
    #checkdicts = *({} for _ in dirs),

    global all_files, current_dir

    # Deserialize JSON
    try:
        with open(JSON_NAME, 'r') as json_file:
            print('Resuming from cached results...')
            json_data = load(json_file)
        current_dir = json_data['current_dir']
        all_files = {literal_eval(key): set(value) for key, value in json_data['all_files'].items()}
    except (JSONDecodeError, FileNotFoundError):
        pass  # Never mind then...

    exit_event = Event()  # Event used to signal keyboard interrupt

    # arguments = *(Arguments(directory, my_dict:=checkdicts[i], tuple(dct for dct in checkdicts if dct is not my_dict), i == main_dir_idx) for i, directory in enumerate(dirs)),
    arguments = *(Arguments(directory, exit_event) for i, directory in enumerate(dirs)),

    with ThreadPoolExecutor(len(dirs)) as executor:
        try:

            for res in executor.map(worker, arguments):
                print(res)

        except (Exception, KeyboardInterrupt) as e:
            exit_event.set()
            executor.shutdown(wait=True)
            # Serialize to JSON so we can resume later.
            json_data = {
                'current_dir': current_dir,
                'all_files': {str(key): tuple(value) for key, value in all_files.items()},
            }
            with open(JSON_NAME, 'w+') as json_file:
                dump(json_data, json_file, indent=4)
            print('\n')


if __name__ == '__main__':
    main('/', main_dir_idx=0)
