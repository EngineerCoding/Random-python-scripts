# Deduplicate files

The idea behind this project is to reduce occupied hard drive space when for example a bunch of backups in the same folder exist. When I speak of backups, I speak of uncompressed backups as this whole process is file based. Personally I've got a bunch of backups sitting in a folder, but have the uncompressed for easy access to older files. However, these backups contain a lot of the same file and therefore occupy unnecessarily more hard drive space than required.
 
This project attempts to find duplicate files recursively based on file checksums, and once duplicate files are found this file is moved to a special folder and symlinks in the original locations are created to this file. While this reduces data redundancy, eg. a specific sector of de hard drive gets correct and not the complete drive, it does allow for more backups on the same hard drive. Furthermore, if your drive goes corrupt completely or is lost in a fire your multiple backups will serve no purpose anyway.

# Usage

This program is written with the use of Python 3.7.4, and is not tested on any version below. In theory this program should also work with Python >= 3.6. No dependencies are required for this program.

For usage, please run:
```
python deduplicate.py -h
```

# Contributing

This program is simply created for personal use, but contributions which don't change the core functionality are more than welcome!\
