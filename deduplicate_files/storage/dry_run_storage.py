from collections import namedtuple
from operator import itemgetter
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from iter_utils import get_first
from .metadata_storage import MetadataStorage

_PathData = namedtuple('PathData', 'metadata checksum')


class DryRunStorage(MetadataStorage):
    """
    Storage which uses an in-memory storage and falls back on a live
    MetadataStorage object for retrieving data only. This way a
    simulation is achieved which is as close as possible to the real
    thing
    """

    def __init__(self, true_storage: MetadataStorage):
        super().__init__('')
        self._true_storage = true_storage
        self._inserted_files = dict()
        self._inserted_symlinks = dict()
        self._linked_files = set()

    @staticmethod
    def _upsert(source: dict, path: Path, checksum: int):
        metadata = MetadataStorage.get_file_metadata(path)
        source[path] = _PathData(metadata=metadata, checksum=checksum)

    @staticmethod
    def _get_paths(source: dict, checksum: int) -> Iterable[Path]:
        return map(itemgetter(0),
                   filter(lambda path, data: data.checksum == checksum,
                          source.items()))

    def upsert_file(self, path: Path, checksum: int):
        print(f'Upsert {path} with checksum {checksum}')
        DryRunStorage._upsert(self._inserted_files, path, checksum)

    def upsert_symlink(self, path: Path, checksum: int):
        print(f'Upsert symlink {path} with checksum {checksum}')
        DryRunStorage._upsert(self._inserted_symlinks, path, checksum)

    def link_file_to_symlink(self, path: Path, symlink_path: Path):
        print(f'Link {path} to {symlink_path}')
        self._linked_files.add(path)

    def get_unlinked_files(self, checksum: int) -> List[Path]:
        original_list = self._true_storage.get_unlinked_files(checksum)
        original_list.extend(
            DryRunStorage._get_paths(self._inserted_files, checksum))
        return list(filter(
            lambda path: path not in self._linked_files, original_list))

    def get_symlink_file(self, checksum: int) -> Optional[Path]:
        return get_first(
            DryRunStorage._get_paths(self._inserted_symlinks, checksum))

    def get_stored_file_metadata(self, path: Path) -> Optional[
            Tuple[int, int]]:
        if path in self._inserted_files:
            return self._inserted_files[path].metadata
        return self._true_storage.get_stored_file_metadata(path)

    def get_stored_symlink_file_metadata(self, path: Path) -> Optional[
            Tuple[int, int]]:
        if path in self._inserted_symlinks:
            return self._inserted_files[path].metadata
        return self._true_storage.get_stored_symlink_file_metadata(path)
