import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TextIO, Union

from magic import from_file

from io_utils import to_path


class _BinaryFileWrapper(object):

    def __init__(self, file_obj):
        self._file_obj = file_obj

    def write(self, contents: str):
        self._file_obj.write(contents.encode())

    def __getattribute__(self, item):
        try:
            return super().__getattribute__(item)
        except AttributeError:
            return getattr(self._file_obj, item)


def right_strip(file_in: TextIO, file_out: TextIO):
    """
    Strips the right side of each line and writes it to the out file

    :param file_in: The file object to read from
    :type file_in: TextIO
    :param file_out: The file object to write to
    :type file_out: TextIO
    """
    for line in file_in:
        file_out.write(f'{line.rstrip()}\n')


def right_strip_file(file_path: Union[Path, str]) -> str:
    """
    Right strips the file path by first writing the contents to a temporary
    file and then copying over this resulting file to the path of the original
    file. If the file does not exist or the mime type is not of type text/*,
    the path is not stripped.

    :param file_path: The path to strip
    :type file_path: Union[Path, str]
    :return: The status of right stripping the file
    :rtype: str
    """
    file_path = to_path(file_path)
    if not file_path.is_file():
        return 'not a file'
    mime_type = from_file(str(file_path), mime=True)
    if not mime_type.startswith('text/'):
        return 'skipping file'

    temporary_file = NamedTemporaryFile(delete=False)
    with file_path.open('r') as file_in, temporary_file as file_out:
        right_strip(file_in, _BinaryFileWrapper(file_out))
    os.rename(temporary_file.name, str(file_path))
    return 'success'


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('file_path', nargs='+')
    parsed = parser.parse_args()

    def apply_right_strip(file_path: str):
        """
        Applies a right strip at the path. When the path points to a directory,
        this directory is recursively stripped as well

        :param file_path: The path to apply the right strip on
        :type file_path: str
        """
        if os.path.isdir(file_path):
            for sub_path in os.listdir(file_path):
                apply_right_strip(os.path.join(file_path, sub_path))
        else:
            right_strip_file(file_path)


    for path in parsed.file_path:
        apply_right_strip(path)
