import os

if __name__ == '__main__':
    # Make sure we have access to the utils and checksum
    import sys

    sys.path.append(
        os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import uuid
from pathlib import Path
from typing import Optional, Type, Union

import checksum
from deduplicate_files.storage import MetadataStorage
from io_utils import (
    copy_full_file_obj, FileDummy, verbose_copy_file_obj, to_path)


class DuplicateFileRemover(object):
    """
    An object which persist file metadata to a storage backend based on a
    checksum algorithm. This object will remove duplicate files and create
    symlinks to for those files pointing to a single file (reducing the
    storage space required)

    :param source_folder: The folder to look for duplicate files
    :type source_folder: Union[Path, str]
    :param symlink_folder: The folder which contains all the files to which \
        a symlink is created along with for example an SQLite database.
    :type symlink_folder: Union[Path, str]
    :param data_handler: The handler which persists file metadata of already \
        indexed files
    :type data_handler: MetadataStorage
    :param checksum_class: The class which is instantiated when calculating \
        a checksum for a file. Defaults to CRC32 for speed.
    :type checksum_class: Optional[Type[Checksum]]
    :param dry_run: Whether to actually remove files and create symlinks
    :type dry_run: bool
    :param verbose: Whether to be verbose about the current operation
    :type verbose: bool
    """

    def __init__(self, source_folder: Union[Path, str],
                 symlink_folder: Union[Path, str],
                 data_handler: MetadataStorage,
                 checksum_class: Optional[Type[checksum.Checksum]] = None,
                 dry_run: bool = False, verbose: bool = False):
        self._source_folder = to_path(source_folder)
        self._symlink_folder = to_path(symlink_folder)
        self._data_handler = data_handler
        self._checksum_class = checksum_class
        if checksum_class is None:
            self._checksum_class = checksum.CRC32

        self._dry_run = dry_run
        self._verbose = verbose
        self._copy_func = (verbose_copy_file_obj
                           if verbose else copy_full_file_obj)

    def get_checksum(self, file_path: Path) -> int:
        """
        Calculates the checksum for a given path. Uses the checksum
        algorithm defined in the constructor.

        :param file_path: The path to calculate the checksum for
        :type file_path: Path

        :return: The calculated checksum
        :rtype: int
        """
        checksum_instance = self._checksum_class()
        if self._verbose:
            print(f'Calculating checksum for {file_path}')
        with file_path.open('rb') as file_handle:
            self._copy_func(file_handle, FileDummy(checksum_instance.update),
                            max_bytes=file_path.stat().st_size)
        return checksum_instance.digest()

    def _create_symlink(self, original_path: Path, checksum_digest: int,
                        symlink_path: Optional[Path] = None) -> Path:
        """
        Creates a link of the original path to the symlink path. When the
        symlink path does not exist yet, this is created on both the file
        system and persisted to the data handler.

        :param original_path: The path to the original file
        :type original_path: Path
        :param checksum_digest: The digest of the file at the original path
        :type checksum_digest: int
        :param symlink_path: The path to the symlink path, if it exists
        :type symlink_path: Optional[Path]

        :return: The path to the symlink or created symlink
        :rtype: Path
        """
        if symlink_path is None:
            symlink_name = str(uuid.uuid4())
            symlink_path = self._symlink_folder.joinpath(symlink_name)
            file_size, _ = MetadataStorage.get_file_metadata(original_path)
            if not self._dry_run:
                with original_path.open('rb') as orig_fh:
                    with symlink_path.open('wb') as symlink_fh:
                        self._copy_func(
                            orig_fh, symlink_fh, max_bytes=file_size)
            if self._dry_run or self._verbose:
                print(f'Created symlink {symlink_name} with '
                      f'checksum {checksum_digest}')
            self._data_handler.upsert_symlink(symlink_path, checksum_digest)
        self._data_handler.link_file_to_symlink(original_path, symlink_path)
        if not self._dry_run:
            original_path.unlink()
            original_path.symlink_to(symlink_path)
        if self._dry_run or self._verbose:
            print(f'Removed {original_path}')
            print(f'Created symlink {original_path} -> {symlink_path}')
        return symlink_path

    def add_file(self, file_path: Union[Path, str]):
        """
        Indexes the file if it should be indexed and tries to find a matching
        symlink or tries to find a duplicate file in current pool to create
        a new symlink for.

        :param file_path: The path to the file to index
        :type file_path: Union[Path, str]
        """
        file_path = to_path(file_path)
        self._print(f'Indexing {file_path} ')
        # Ignore files which are in the target directory
        if str(file_path).startswith(str(self._symlink_folder)):
            return self._print('\tIgnoring => in symlink dir')

        if not self._should_index_file(file_path):
            return self._print('\tIgnoring => index not required')

        checksum_digest = self.get_checksum(file_path)
        self._print(f'\t=> checksum:{checksum_digest}')
        self._data_handler.upsert_file(file_path, checksum_digest)
        symlink_path = self._data_handler.get_symlink_file(checksum_digest)
        if not symlink_path:
            for other_file in self._data_handler.get_unlinked_files(
                    checksum_digest):
                if other_file.samefile(file_path):
                    continue
                # Found another file with the same checksum!
                symlink_path = self._create_symlink(
                    other_file, checksum_digest, symlink_path=symlink_path)
        # Not an elif since the symlink_path may be created in the previous
        # if block
        if symlink_path:
            self._create_symlink(
                file_path, checksum_digest, symlink_path=symlink_path)

    def index_source_files(self, source_folder: Optional[Path] = None):
        """
        Indexes the file in the source directory. When the source_folder
        parameter is not set, this will default to the source folder defined
        on this object.

        :param source_folder: The root to search files for
        :type source_folder: Path
        """
        if source_folder is None:
            source_folder = self._source_folder
        for sub_path in source_folder.iterdir():
            if sub_path.is_dir():
                self.index_source_files(sub_path)
            else:
                self.add_file(sub_path)

    def index_symlink_files(self):
        """
        Reindexes files in the symlink folder. Note that this does not detect
        removed files.
        """
        for sub_path in self._symlink_folder.iterdir():
            if self._should_index_file(sub_path, symlink_file=True):
                self._print(f'Indexing {sub_path} ', end='')
                checksum_digest = self.get_checksum(sub_path)
                self._print(f'\t=> checksum:{checksum_digest}')
                self._data_handler.upsert_symlink(
                    sub_path, checksum_digest)

    def _should_index_file(self, file_path: Path,
                           symlink_file: bool = False) -> bool:
        """
        Whether a file should be indexed or not

        :param file_path: The path to the file which is attempted to be indexed
        :type file_path: Path
        :param symlink_file: Whether this index is happening for a file in \
            the symlink directory or not
        :return: Whether the file should be indexed or not
        :rtype: bool
        """
        if not file_path.is_file() or file_path.is_symlink():
            return False

        if symlink_file:
            data = self._data_handler.get_stored_symlink_file_metadata(
                file_path)
        else:
            data = self._data_handler.get_stored_file_metadata(file_path)
        file_size, modified_date = None, None
        if data:
            file_size, modified_date = data
        if file_size is None or modified_date is None:
            return not symlink_file

        a_file_size, a_mod_date = MetadataStorage.get_file_metadata(file_path)
        return a_file_size != file_size or a_mod_date != modified_date

    def _print(self, *args, **kwargs):
        """
        Prints something to the stdout if verbose is set to true. Method has
        the same signature as the builtin print
        """
        if self._verbose:
            print(*args, **kwargs)


if __name__ == '__main__':
    import argparse
    import ctypes
    import sys

    from deduplicate_files.storage import (
        DryRunStorage, SQLiteMetadataStorage)

    argument_parser = argparse.ArgumentParser(
        description='Tries to find duplicate files in the specified folder '
                    'and creates symbolic links in the target directory. This '
                    'target directory contains the original files along with '
                    'metadata in a SQLite database. The target directory '
                    'is not scanned and created when it does not exist yet. To'
                    ' be able to create symlinks on windows, this script '
                    'be run with admin privileges.')
    argument_parser.add_argument('file_folder',
                                 help='The folder which to scan for duplicate '
                                      'files')
    argument_parser.add_argument('--symlink-folder', default='.symlinks',
                                 help='The folder to store the original files '
                                      'in, along with a database with the file'
                                      ' metadata')
    argument_parser.add_argument('--add-file', nargs='+', dest='files',
                                 help='Files to specifically add to the '
                                      'storage. Applies --no-index and -'
                                      '-no-symlink-index implicitly.')
    argument_parser.add_argument('--no-index', action='store_true',
                                 help='Don\'t apply indexing on sourec files')
    argument_parser.add_argument('--no-symlink-index', action='store_true',
                                 help='Don\'t apply indexing on the original '
                                      'target files')
    argument_parser.add_argument('--checksum-algo', default='crc32',
                                 choices=checksum.get_available_algorithms(),
                                 help='The algorithm to use for calculating '
                                      'checksums')
    argument_parser.add_argument('--dry-run', action='store_true',
                                 help='Stops files from being deleted and '
                                      'symlinks to be created')
    argument_parser.add_argument('--verbose', action='store_true',
                                 help='Enable more logging of output')
    parsed = argument_parser.parse_args()
    # check if there is work to do
    if parsed.no_index and parsed.no_target_index and not parsed.files:
        print('No work to do!')
        sys.exit(0)
    # check the directories
    if not os.path.isdir(parsed.file_folder):
        print('Not a directory: ' + parsed.file_folder)
        sys.exit(1)
    if not os.path.isdir(parsed.symlink_folder):
        try:
            os.makedirs(parsed.symlink_folder, exist_ok=True)
        except NotADirectoryError:
            print('Could not create directory: ' + parsed.symlink_folder)
            sys.exit(1)
    # check if we are able to create symlinks if we are on windows
    if os.name == 'nt':
        try:
            if not ctypes.windll.shell32.IsUserAnAdmin():
                print('Cannot create symbolic links without elevated '
                      'privileges!')
                sys.exit(1)
        except Exception:
            print('You seem to be running an ancient version of windows!')
            print('Cannot check whether you ran this script as admin.\n')
            sys.exit(1)


    def verbose_print(*args, **kwargs):
        if parsed.verbose:
            print(*args, **kwargs)


    _data_handler = SQLiteMetadataStorage(parsed.symlink_folder)
    if parsed.dry_run:
        _data_handler = DryRunStorage(_data_handler)

    # Apply work!
    _checksum_class = checksum.get_algorithm(parsed.checksum_algo)
    duplicate_file_remover = DuplicateFileRemover(
        parsed.file_folder, parsed.symlink_folder, _data_handler,
        checksum_class=_checksum_class, dry_run=parsed.dry_run,
        verbose=parsed.verbose)

    if parsed.files:
        for path in parsed.files:
            duplicate_file_remover.add_file(path)
    if not parsed.files and not parsed.no_symlink_index:
        verbose_print('========== Indexing symlink files ==========')
        duplicate_file_remover.index_symlink_files()
    if not parsed.files and not parsed.no_index:
        verbose_print('============= Indexing files ==============')
        duplicate_file_remover.index_source_files()
    _data_handler.close()
