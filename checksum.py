from typing import Iterable, Type


class Checksum(object):
    """
    A base class to generate a checksum from streaming data
    """

    def update(self, buf: Iterable[int]):
        """
        Update the checksum with an iterable containing ints,
        such as a bytes object

        :param buf: The iterable to update the current checksum with
        :type buf: Iterable[int]
        """
        raise NotImplementedError

    def digest(self) -> int:
        """
        Calculates the digest based on the data this checksum has been
        updated with

        :return: The checksum digest
        :rtype: int
        """
        raise NotImplementedError

    def hexdigest(self) -> str:
        """
        A hexadecimal string representation of the checksum digest

        :return: The hexadecimal string
        :rtype: str
        """
        return hex(self.digest())


class _FletcherChecksum(Checksum):
    """
    Main class for calculating a checksum based on Fletcher's algorithm.
    Note that this is not the most efficient algorithm out there, but still
    does the job. It is not recommended to process large files with this
    specific implementation (>1GB)
    """

    def __init__(self, bit_size: int):
        """
        Creates the fletcher's instance version. The
        :param bit_size: The bit size which is used by this algorithm. Must \
            be even and > 0
        :type bit_size: int

        :raises ValueError: When the bit size <= 0 or not even
        """
        self._bit_size = bit_size
        if bit_size <= 0:
            raise ValueError('Bit size must be > 0')
        if bit_size & 1:
            raise ValueError('Bit size must be even')
        self._modulo = int(pow(2, bit_size / 2) - 1)
        self._sum_a = 0
        self._sum_b = 0

    def update(self, buf: Iterable[int]):
        for data_point in buf:
            self._sum_a = (self._sum_a + data_point) % self._modulo
            self._sum_b = (self._sum_a + self._sum_b) % self._modulo

    def digest(self) -> int:
        return (self._sum_b << (self._bit_size // 2)) | self._sum_a


class Fletcher8(_FletcherChecksum):
    """ Fletcher8 checksum: 8 bit checksum """

    def __init__(self):
        super().__init__(8)


class Fletcher16(_FletcherChecksum):
    """ Fletcher16 checksum: 16 bit checksum """

    def __init__(self):
        super().__init__(16)


class Fletcher32(_FletcherChecksum):
    """ Fletcher32 checksum: 32 bit checksum """

    def __init__(self):
        super().__init__(32)


class SimpleFletcher64(_FletcherChecksum):
    """ Fletcher64 checksum: 64 bit checksum """

    def __init__(self):
        super().__init__(64)


class Adler32(_FletcherChecksum):
    """
    The Adler32 checksum, based on the Fletcher32 checksum algorithm
    with a custom modulus and the first sum set to 1 instead of 0.
    This is less error prone than Fletcher16, but more error prone than
    Fletcher32.
    """

    def __init__(self):
        super().__init__(32)
        self._sum_a = 1
        self._modulo = 65521


class CRC32(Checksum):
    """
    A wrapper for the zlib crc32 implementation
    """

    def __init__(self, crc: int = 0):
        """
        Creates the CRC32 instance, a wrapper for the zlib.crc32 function

        :param crc: The initial CRC32 value
        :type crc: int
        """
        self._crc = crc

    def update(self, buf: Iterable[int]):
        from zlib import crc32
        self._crc = crc32(buf, self._crc)

    def digest(self) -> int:
        return self._crc


_algorithms = dict(
    fletcher16=Fletcher16, fletcher32=Fletcher32, fletcher64=SimpleFletcher64,
    adler32=Adler32, crc32=CRC32)


def get_available_algorithms() -> Iterable[str]:
    """
    Retrieves an iterable with the available algorithms

    :return: The iterable with available algorithms
    :rtype: Iterable[str]
    """
    return _algorithms.keys()


def get_algorithm(key: str) -> Type[Checksum]:
    """
    Retrieves the algorithm by name

    :param key: The algorithm to retrieve
    :type key: str
    :return: The associated Algorithm
    :rtype: Type[Checksum]
    :raises KeyError: When the algorithm does not exist
    """
    return _algorithms[key]


if __name__ == '__main__':
    import argparse
    import os
    import sys

    import io_utils

    argument_parser = argparse.ArgumentParser(
        description='Calculates a checksum for a file')
    argument_parser.add_argument('file', nargs='+',
                                 help='The file(s) to calculate the checksum '
                                      'for')
    argument_parser.add_argument('--algo', default='crc32',
                                 choices=get_available_algorithms(),
                                 help='Note that the crc32 algo is probably '
                                      'the quickest due to the fact that it '
                                      'is implemented in C (zlib).')
    argument_parser.add_argument(
        '--output-dec-only', dest='output_hex_only', action='store_false')
    argument_parser.add_argument(
        '--output-hex-only', dest='output_dec_only', action='store_false')
    argument_parser.add_argument('--verbose', action='store_true')
    parsed = argument_parser.parse_args()

    # initial variables
    copy_func = io_utils.copy_full_file_obj
    if parsed.verbose:
        copy_func = io_utils.verbose_copy_file_obj


    def handle_file(file_path: str):
        """
        Checks if the file exists and calculates the checksum if available

        :param file_path: The path to the file
        :type file_path: str
        """
        if not os.path.isfile(file_path):
            print('Not a file: ' + file_path)
            sys.exit(1)

        if parsed.output_dec_only or parsed.output_hex_only:
            algorithm = get_algorithm(parsed.algo)()
            max_bytes = os.stat(file_path).st_size
            with open(file_path, 'rb') as file_handle:
                copy_func(file_handle, io_utils.FileDummy(algorithm.update),
                          max_bytes=max_bytes)

        print(f'{file_path}\t', end='')
        if parsed.output_dec_only:
            print(algorithm.digest(), end='')
        if parsed.output_dec_only and parsed.output_hex_only:
            print('\t', end='')
        if parsed.output_hex_only:
            print(algorithm.hexdigest(), end='')
        print()


    for path in parsed.file:
        handle_file(path)
