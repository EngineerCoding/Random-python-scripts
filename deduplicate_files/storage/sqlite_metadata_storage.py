import sqlite3
from operator import itemgetter
from pathlib import Path
from typing import List, Optional, Tuple

from iter_utils import get_first
from .metadata_storage import MetadataStorage

_SYMLINK_INDEX_TABLE = 'Symlink_index'
_FILE_INDEX_TABLE = 'File_index'
_SQL_SYMLINK_INDEX_TABLE = '''
CREATE TABLE Symlink_index (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    modified_date INTEGER NOT NULL,
    checksum INTEGER NOT NULL,
    
    CONSTRAINT si_path_unique UNIQUE (path),
    CONSTRAINT si_checksum_unique UNIQUE (checksum)
)'''
_SQL_FILE_INDEX_TABLE = '''
CREATE TABLE File_index (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL,
    file_size INTEGER,
    modified_date INTEGER,
    checksum INTEGER,
    symlink INTEGER REFERENCES Symlink_index(id),
    
    CONSTRAINT fi_path_unique UNIQUE (path)
)
'''
_SQL_FETCH_PATH = '''
SELECT id, path, checksum
FROM {table}
WHERE path = ?'''
_SQL_UPDATE_PATH = '''
UPDATE {table}
SET
    file_size = ?,
    modified_date = ?,
    checksum = ?
WHERE file_path = ?
'''
_SQL_INSERT_PATH = '''
INSERT INTO {table} (path, file_size, modified_date, checksum)
    VALUES (?, ?, ?, ?)
'''
_SQL_LINK_FILE_SYMLINK = '''
UPDATE File_index
SET
    file_size = NULL,
    modified_date = NULL,
    checksum = NULL,
    symlink = ?
WHERE path = ?     
'''
_SQL_FETCH_CHECKSUM = '''
SELECT path
FROM {table}
WHERE checksum = ? AND file_size != 0
'''
_SQL_FETCH_FILE_METADATA = '''
SELECT file_size, modified_date
FROM {table}
WHERE path = ?
'''


class SQLiteMetadataStorage(MetadataStorage):
    """
    Storage based on a SQLite3 backend
    """

    def __init__(self, symlink_dir):
        super().__init__(symlink_dir)
        self._database_file = self.symlink_dir.joinpath('data.sqlite3')
        already_exists = self._database_file.exists()
        self._database_connection = sqlite3.connect(str(self._database_file))
        if not already_exists:
            self.symlink_dir.mkdir(exist_ok=True)
            cursor = self._database_connection.cursor()
            cursor.execute(_SQL_SYMLINK_INDEX_TABLE)
            cursor.execute(_SQL_FILE_INDEX_TABLE)
            self._database_connection.commit()

    def _upsert(self, path: Path, checksum: int, table: str):
        str_path = str(path)
        cursor = self._database_connection.cursor()
        cursor.execute(_SQL_FETCH_PATH.format(table=table), (str_path,))
        row = cursor.fetchone()
        data_tuple = (*MetadataStorage.get_file_metadata(path), checksum)
        if row:
            if row[2] != checksum:
                cursor.execute(
                    _SQL_UPDATE_PATH.format(table=table), data_tuple)
        else:
            cursor.execute(
                _SQL_INSERT_PATH.format(table=table), (str_path, *data_tuple))
        self._database_connection.commit()

    def upsert_file(self, path: Path, checksum: int):
        self._upsert(path, checksum, _FILE_INDEX_TABLE)

    def upsert_symlink(self, path: Path, checksum: int):
        self._upsert(path, checksum, _SYMLINK_INDEX_TABLE)

    def link_file_to_symlink(self, path: Path, symlink_path: Path):
        cursor = self._database_connection.cursor()
        cursor.execute(_SQL_FETCH_PATH.format(table=_SYMLINK_INDEX_TABLE),
                       (str(symlink_path), ))
        symlink_id = cursor.fetchone()[0]
        cursor.execute(_SQL_LINK_FILE_SYMLINK, (symlink_id, str(path)))
        self._database_connection.commit()

    def _get_checksum_files(self, checksum: int, table: str) -> List[Path]:
        cursor = self._database_connection.cursor()
        cursor.execute(_SQL_FETCH_CHECKSUM.format(table=table), (checksum,))
        return list(map(
            Path, map(itemgetter(0), cursor.fetchall())))

    def get_unlinked_files(self, checksum: int) -> List[Path]:
        return self._get_checksum_files(checksum, _FILE_INDEX_TABLE)

    def get_symlink_file(self, checksum: int) -> Optional[Path]:
        return get_first(
            self._get_checksum_files(checksum, _SYMLINK_INDEX_TABLE))

    def _get_metadata(self, path: Path, table: str) -> Optional[
            Tuple[int, int]]:
        cursor = self._database_connection.cursor()
        cursor.execute(_SQL_FETCH_FILE_METADATA.format(table=table),
                       (str(path),))
        return cursor.fetchone()

    def get_stored_file_metadata(self, path: Path) -> Optional[
            Tuple[int, int]]:
        return self._get_metadata(path, _FILE_INDEX_TABLE)

    def get_stored_symlink_file_metadata(self, path: Path) -> Optional[
            Tuple[int, int]]:
        return self._get_metadata(path, _SYMLINK_INDEX_TABLE)

    def close(self):
        self._database_connection.close()
