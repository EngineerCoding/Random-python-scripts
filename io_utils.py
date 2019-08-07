import os
import sys
import time
from datetime import timedelta
from pathlib import Path
from typing import BinaryIO, Callable, Generator, IO, Optional, Tuple, Union

import math

AVAILABLE_UNITS = ['bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']


def convert_byte_to_unit(byte_amount: int) -> Tuple[int, str]:
    """
    Converts a raw byte amount to a byte amount in other units, such as
    KB, MB, etc up to YB. This returns a tuple containing the converted
    amount along with the unit.

    :param byte_amount: The amount of bytes which needs to get converted
    :type byte_amount: int

    :return: A tuple containing the converted amount along with the unit
    :rtype: tuple
    """
    for index, unit in enumerate(AVAILABLE_UNITS):
        lower_threshold = 0 if index == 0 else 1024 ** (index - 1)
        upper_threshold = 1024 ** index
        if lower_threshold <= byte_amount < upper_threshold:
            if lower_threshold == 0:
                return byte_amount, unit
            else:
                converted_bytes = byte_amount / lower_threshold
                return converted_bytes, AVAILABLE_UNITS[index - 1]
    # Default to the maximum
    max_index = len(AVAILABLE_UNITS) - 1
    return byte_amount / (1024 ** max_index), AVAILABLE_UNITS[max_index]


def to_path(path: Union[Path, str]) -> Path:
    """
    Converts a str to a Path object and resolves the path so that an
    absolute path is used in the underlying path str.
    
    :param path: The path to resolve and make absolute
    :type path: Union[Path, str] 
    :return: The resolved and absolute path
    :rtype: Path
    """
    if isinstance(path, str):
        path = Path(path)
    return path.resolve().absolute()


def flush_file_handle(file_handle: IO):
    """
    Attempts to flush the handle. First tries to call .flush and then
    tries to apply `os.fsync` to the file descriptor to truly flush the
    buffer to the file.

    :param file_handle: The file handle to attempt to flush
    :type file_handle: IO
    """
    if hasattr(file_handle, 'flush'):
        file_handle.flush()
    if hasattr(file_handle, 'fileno'):
        os.fsync(file_handle.fileno())


def copy_file_obj(from_handle: BinaryIO, to_handle: BinaryIO, *args,
                  chunk_size: int = 65536, max_bytes: Optional[int] = None,
                  force_flush: bool = False, **kwargs
                  ) -> Generator[int, None, None]:
    """
    Copies a file object to another file object with the amount of bytes
    with a configured chunk size. This is a generator, which yields the
    amount of bytes read and written after flushing the to_handle (if
    this is supported by the to_handle). If you don't need those updates,
    please use `copy_full_file_obj`.

    :param from_handle: The handle to read from
    :type from_handle: BinaryIO
    :param to_handle: The handle to write to
    :type to_handle: BinaryIO
    :param chunk_size: The chunk size to read and write with
    :type chunk_size: int
    :param max_bytes: The maximum amount of bytes that should be read and \
        written
    :type max_bytes: Optional[int]
    :param force_flush: Whether to apply a flush after each write
    :type force_flush: bool
    """
    # Initial read
    if max_bytes and max_bytes < chunk_size:
        data = from_handle.read(max_bytes)
    else:
        data = from_handle.read(chunk_size)
    # If there is no data, update the callable with 0
    if not data:
        yield 0
    else:
        total_read = 0
        while data:
            data_len = len(data)
            total_read += data_len
            to_handle.write(data)
            if force_flush:
                flush_file_handle(to_handle)
            yield data_len
            # Read the next data
            if total_read == max_bytes:
                break
            new_data_chunk_size = chunk_size
            if max_bytes and total_read + chunk_size > max_bytes:
                new_data_chunk_size = max_bytes - total_read
            data = from_handle.read(new_data_chunk_size)


def copy_full_file_obj(*args, **kwargs):
    """
    Refer to `copy_file_obj` for details of the parameters. In contrary to
    that function, this function will apply a full copy without yielding
    the read and written bytes.
    """
    for _ in copy_file_obj(*args, **kwargs):
        pass


def copy_file_obj_download_size(*args, **kwargs) -> Generator[
        Tuple[int, int], None, None]:
    """
    Refer to `copy_file_obj` for details of the parameters. This generator
    yields additional data in comparison with the `copy_file_obj` function,
    as it will yield the als the total written size
    """
    downloaded_size = 0
    for written_bytes in copy_file_obj(*args, **kwargs):
        downloaded_size += written_bytes
        yield written_bytes, downloaded_size


def copy_file_obj_measure_speed(*args, **kwargs) -> Generator[
        Tuple[int, int, int, Optional[int]], None, None]:
    """
    Refer to `copy_file_obj` for details of the parameters. This generator
    yields additional data in comparison with the `copy_file_obj` function,
    as it will yield the total downloaded size, the speed in bytes/s and
    estimated remaining time in seconds (if max_bytes is set)
    """
    start_time = time.clock()
    max_bytes = kwargs.get('max_bytes')

    for written_bytes, downloaded_size in copy_file_obj_download_size(
            *args, **kwargs):
        delta_t = time.clock() - start_time
        speed = downloaded_size // (1 if delta_t == 0 else delta_t)
        estimated_seconds = None
        if max_bytes:
            remaining_bytes = max_bytes - downloaded_size
            estimated_seconds = math.ceil(remaining_bytes / speed)
        yield written_bytes, downloaded_size, speed, estimated_seconds


def _get_progress_bar_string(percentage, progress_bar_length):
    char_amount = int(progress_bar_length * percentage)
    progress_chars = '=' * char_amount
    if progress_bar_length - char_amount > 0:
        progress_chars += '>'
    progress_chars_left = ' ' * (progress_bar_length - char_amount - 1)
    return f'[{progress_chars}{progress_chars_left}]'


def verbose_copy_file_obj(*args, print_file: IO = sys.stdout,
                          progress_bar_length: int = 50,
                          **kwargs):
    """
    A verbose version of `copy_file_obj`, where a progress bar is shown along
    with the current speed and total amount of bytes. The described parameters
    are in addition to the ones of `copy_file_obj`. For the progress bar to
    actually show up, please define the max_bytes parameter

    :param print_file: The file object which is used to the printing method
    :type print_file: IO
    :param progress_bar_length: The length (in chars) of the progress bar
    :type progress_bar_length: int
    """
    max_bytes = kwargs.get('max_bytes', None)
    prev_len = 0

    downloaded_size = 0
    for data in copy_file_obj_measure_speed(*args, **kwargs):
        written_bytes, downloaded_size, speed, estimated_seconds = data
        converted_speed, speed_unit = convert_byte_to_unit(speed)
        converted_speed = round(converted_speed, 2)

        display_string = ''
        # Progress bar
        if max_bytes is not None and max_bytes > 0:
            percentage = downloaded_size / max_bytes
            display_percentage = str(
                round(percentage * 100, 2))[:5].ljust(5, '0')
            display_string += _get_progress_bar_string(
                percentage, progress_bar_length)
            display_string += f' {display_percentage}% '
        display_string += f' {converted_speed} {speed_unit}/s'
        # ETA
        if estimated_seconds:
            try:
                eta = str(timedelta(seconds=estimated_seconds))
            except OverflowError:
                pass
            else:
                display_string += f' ETA {eta}'
        display_string_len = len(display_string)
        if prev_len > display_string_len:
            difference = prev_len - display_string_len
            prev_len = display_string_len
            display_string += ' ' * difference
        else:
            prev_len = display_string_len
        print(f'\r{display_string}', end='', file=print_file)
    total_size, total_unit = convert_byte_to_unit(downloaded_size)
    total_size = round(total_size, 2)
    display_string = ''
    if max_bytes:
        display_string += _get_progress_bar_string(1, progress_bar_length)
        display_string += ' '
    display_string += f'{total_size} {total_unit}'
    difference = (prev_len - len(display_string)
                  if prev_len > len(display_string) else 0)
    print(f'\r{display_string}{" " * difference}', file=print_file)


class FileDummy(object):
    """
    A class which can be used with the copy functions and redirects the file
    'writes' and 'flushes' to a callable. The flush is called after each write
    and only exists in this class for the sake of completeness
    """

    def __init__(self, write_callable: Callable,
                 flush_callable: Callable = None):
        self._write_callable = write_callable
        self._flush_callable = flush_callable

    def write(self, data):
        self._write_callable(data)

    def flush(self):
        if self._flush_callable:
            self._flush_callable()
