import os
from typing import IO
from urllib.parse import urlparse

import requests

from io_utils import copy_full_file_obj, verbose_copy_file_obj


class ResponseFileWrapper(object):

    def __init__(self, response, chunk_size=65535):
        self._iterator = response.iter_content(chunk_size)

    def read(self, *args):
        try:
            return next(self._iterator)
        except StopIteration:
            return b''


def download_url(url: str, filename: str = None, force_ext: str = None,
                 print_file: IO = None) -> bool:
    """
    Downloads a single URL to a file. When the filename is not supplied,
    the filename is parsed from the URL and used. When an extension is
    going to be forced, this is simply added as suffix including the
    dot. When the final name of the file already exists in the current
    working directory, False is returned.

    :param url: The url to download
    :type url: str
    :param filename: The optional filename of the file
    :type filename: filename
    :param force_ext: The optional extension to add as suffix
    :type force_ext: str
    :param print_file: The file object which should be used to print \
        verbose statements to
    :type print_file: IO

    :return: Whether this was successful or not. When not successful, \
        the filename already exists in the cwd and is NOT overwritten
    :rtype: bool

    :raises ValueError: When the URL is malformed
    """
    if filename is None:
        parsed_url = urlparse(url)
        filename = os.path.basename(parsed_url.path)
    if filename.startswith('/'):
        filename = filename[1:]
    if force_ext is not None:
        if not filename.endswith(f'.{force_ext}'):
            filename += f'.{force_ext}'
    if os.path.isfile(filename):
        return False

    response = requests.get(url, stream=True)
    if not response.ok:
        return False

    max_bytes = response.headers.get('content-length')
    if max_bytes:
        max_bytes = int(max_bytes)
    copy_func = verbose_copy_file_obj
    if print_file is None:
        copy_func = copy_full_file_obj
    else:
        print(f'Downloading {url}', file=print_file)
    response = ResponseFileWrapper(response)
    with open(filename, 'wb') as out:
        copy_func(response, out, max_bytes=max_bytes, print_file=print_file)


def download_urls_in_file(url_file: str, force_ext: str = None,
                          print_file: IO = None):
    """
    Downloads URL's which are stored in a file. This assumes
    that each line solely contains an URL

    :param url_file: The path to the file containing URL's
    :type url_file: str
    :param force_ext: The extensions which should be forced for each filename
    :type force_ext: str
    :param print_file: The file handle which is printed to for verbosity
    :type print_file: IO

    :raises FileNotFoundError: When the path to the file could not be found
    """
    if not os.path.isfile(url_file):
        raise FileNotFoundError(url_file)
    else:
        with open(url_file) as url_file_handle:
            for line in url_file_handle:
                line = line.strip()
                if not line:
                    continue
                download_url(line, force_ext=force_ext, print_file=print_file)


if __name__ == '__main__':
    import argparse
    import sys

    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument('url_file',
                                 help='File containing URLs to download')
    argument_parser.add_argument('--extension', dest='ext', default=None,
                                 help='The extension to force')
    argument_parser.add_argument('--quiet', '-q', action='store_true',
                                 help='Turns off output to stdout')
    parsed = argument_parser.parse_args()

    kwargs = dict(force_ext=parsed.ext, print_file=sys.stderr)
    if parsed.quiet:
        kwargs['print_file'] = None

    download_urls_in_file(parsed.url_file, **kwargs)
