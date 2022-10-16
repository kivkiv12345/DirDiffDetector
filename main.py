""" Find differences between files in multiple directories. """

from os import listdir
from os.path import isfile, isdir, islink, abspath, split
from concurrent.futures import ThreadPoolExecutor
from typing import NamedTuple


class Arguments(NamedTuple):
    directory: str
    mydict: dict[str, float]
    their_dicts: tuple[dict[str, float], ...]
    is_main: bool = True


def worker(args: Arguments):

    def dir_worker(directory: str):
        for path in (abspath(path) for path in listdir(directory)):
            print(isfile(path), path)
            if isdir(path):
                dir_worker(path)
            else:
                filename = split(path)[-1]


    dir_worker(args.directory)

    return


def main(*dirs, main_dir_idx: int = None):

    # {filename: filesize for file in dir}
    checkdicts = *({} for _ in dirs),

    with ThreadPoolExecutor(len(dirs)) as executor:

        arguments = *(Arguments(directory, my_dict:=checkdicts[i], tuple(dct for dct in checkdicts if dct is not my_dict), i == main_dir_idx) for i, directory in enumerate(dirs)),

        for res in executor.map(worker, arguments):
            print(res)


if __name__ == '__main__':
    main('dir1', 'dir2', main_dir_idx=0)
