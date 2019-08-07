from pathlib import Path
from typing import List, Optional, Tuple, Union

from io_utils import to_path


class MetadataStorage(object):
    """
    The object which is an abstraction layer for persisting file metadata
    """

    def __init__(self, symlink_dir: Union[Path, str]):
        self.symlink_dir = to_path(symlink_dir)

    @staticmethod
    def get_file_metadata(path: Path) -> Tuple[int, int]:
        """
        Retrieves the file metadata for a path, specifically the file size
        and date in seconds when the file was last modified.

        :param path: The path to check
        :type path: Path

        :return: A tuple of the file size and modification date in seconds
        :rtype: Tuple[int, int]
        """
        stats = path.stat()
        return stats.st_size, stats.st_mtime

    def upsert_file(self, path: Path, checksum: int):
        """
        Inserts the path with the checksum in the storage or updates it when
        it already exists.

        :param path: The path to insert or update
        :type path: Path
        :param checksum: The checksum which is associated with the path
        :type checksum: int
        """
        raise NotImplementedError

    def upsert_symlink(self, path: Path, checksum: int):
        """
        Inserts the path as symlink in the storage or updates it when it
        already exists

        :param path: The path to insert or update
        :type path: Path
        :param checksum: The checksum which is associated with the path
        :type checksum: int
        """
        raise NotImplementedError

    def link_file_to_symlink(self, path: Path, symlink_path: Path):
        """
        Links a file path to a symlink path which both have been previously
        stored in this storage.

        :param path: The path to link the symlink to
        :type path: Path
        :param symlink_path: The path to the where the original data is stored
        :type symlink_path: Path
        """
        raise NotImplementedError

    def get_unlinked_files(self, checksum: int) -> List[Path]:
        """
        Retrieves a list of files which have a specific checksum. When a path
        is already linked to a symlink, don't include that path.

        :param checksum: The checksum to retrieve unlinked paths for
        :type checksum: int

        :return: A list of unlinked paths with a checksum
        :rtype: List[Path]
        """
        raise NotImplementedError

    def get_symlink_file(self, checksum: int) -> Optional[Path]:
        """
        Retrieves the path to a symlink file with a specific checksum if
        available. When not available, nothing should be returned.

        :param checksum: The checksum to retrieve unlinked paths for
        :type checksum: int

        :return: A symlink associated with a specific checksum
        :rtype: Optional[Path]
        """
        raise NotImplementedError

    def get_stored_file_metadata(self, path: Path) -> Optional[
            Tuple[int, int]]:
        """
        Retrieves the stored metadata of path, if available in this storage.

        :param path: The path to retrieve the metadata for
        :type path: Path

        :return: A tuple of the file size and last modification date
        :rtype: Optional[Tuple[int, int]]
        """
        raise NotImplementedError

    def get_stored_symlink_file_metadata(self, path: Path) -> Optional[
            Tuple[int, int]]:
        """
        Retrieves the stored metadata of path, if available as symlink in this
        storage.

        :param path: The path to retrieve the metadata for
        :type path: Path

        :return: A tuple of the file size and last modification date
        :rtype: Optional[Tuple[int, int]]
        """
        raise NotImplementedError

    def close(self):
        """
        Closes any open resources
        """
        pass
