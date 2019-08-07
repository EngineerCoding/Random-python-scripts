import os
import shutil
from typing import IO

from io_utils import copy_full_file_obj, verbose_copy_file_obj


def _check_byte_size(size: int) -> bool:
    """
    Checks whether size is a valid size. Sizes include None or >= 0

    :param size: The size to check
    :type size: int

    :return: Whether the size is valid
    :rtype: bool
    """
    if size is None:
        return True
    return size >= 0


def copy_bytes(source_file: str, target_file: str, max_bytes: int = None,
               offset: int = None, chunk_size: int = 1024,
               print_file: IO = None):
    """
    Copies the source file to the target files. An offset in the source file
    can be specified along with a maximum amount of bytes to copy.

    :param source_file: The path to the source file
    :type source_file: str
    :param target_file: The path to the target file
    :type target_file: str
    :param max_bytes: If defined, the maximum amount of bytes to copy
    :type max_bytes: int
    :param offset: If defined, the offset to read
    :type offset: int
    :param chunk_size: The chunk size to read/write with
    :type chunk_size: int
    :param print_file: A file handle to print verbosity to
    :type print_file: IO

    :raises FileNotFoundError: When the source file is not found
    :raises ValueError: When the defined max_bytes < 0
    :raises ValueError: When the defined offset < 0
    :raises ValueError: When the chunk_size is <= 0
    :raises ValueError: When the max_bytes cannot be pulled from the file, \
        also considering the offset
    :raises ValueError: When the max_bytes or file size cannot be stored on\
        target destination
    """
    if not os.path.isfile(source_file):
        raise FileNotFoundError(source_file)
    elif not _check_byte_size(max_bytes) or not _check_byte_size(offset):
        raise ValueError('Byte sizes must be not defined or >= 0!')
    elif not _check_byte_size(chunk_size) or chunk_size <= 0:
        raise ValueError('Chunk size must be defined and > 0!')
    # Check if the max_bytes can be pulled from the file
    # also considering the offset
    offset = 0 if offset is None else offset
    file_size = os.path.getsize(source_file)
    if max_bytes and file_size - max_bytes - offset < 0:
        raise ValueError(f'Cannot pull {max_bytes} from source (offset:'
                         f' {offset}, file size: {file_size}')
    elif not max_bytes:
        max_bytes = file_size

    # Create the dir of the target file and sanity check
    # the available space
    target_file_dir = os.path.dirname(target_file)
    os.makedirs(target_file_dir, exist_ok=True)
    free_space = shutil.disk_usage(target_file_dir).free
    if free_space < max_bytes:
        raise ValueError(f'Cannot store {max_bytes} bytes in {free_space} '
                         f'available bytes')

    with open(source_file, 'rb') as infile, open(target_file, 'wb') as outfile:
        if offset:
            infile.seek(offset)
        copy_func = verbose_copy_file_obj
        if print_file is not None:
            copy_func = copy_full_file_obj
        copy_func(infile, outfile, max_bytes=max_bytes, print_file=print_file)


if __name__ == '__main__':
    import argparse
    import sys

    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument('source', help='Source file')
    argument_parser.add_argument('target', help='Target file to copy to')
    argument_parser.add_argument('--bytes', default=None, type=int,
                                 help='The amount of bytes to copy')
    argument_parser.add_argument('--offset', default=None, type=int,
                                 help='The offset for when to start reading')
    argument_parser.add_argument('--chunk-size', default=1024, type=int)
    argument_parser.add_argument('--quiet', '-q', action='store_true',
                                 help='Turns off output to stdout')
    parsed = argument_parser.parse_args()

    kwargs = dict(max_bytes=parsed.bytes, print_file=sys.stdout,
                  offset=parsed.offset, chunk_size=parsed.chunk_size)
    if parsed.quiet:
        kwargs['print_file'] = None

    try:
        copy_bytes(parsed.source, parsed.target, max_bytes=parsed.bytes,
                   offset=parsed.offset, chunk_size=parsed.chunk_size)
    except (ValueError, FileNotFoundError) as exception:
        print(str(exception), file=sys.stderr)
        sys.exit(1)
