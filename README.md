# Random python scripts

A collection of random python scripts which can either be used standalone are be copy and pasted to be used as a library. A quick overview of the available files:

## io_utils.py

A library which can used to convert an amount of bytes to the human readable form (MB, GB, etc) and is able to copy one file handle to the other while setting for example and optionally to print the verbose progress.


## iter_utils.py

Utilities for iterables, currently only contains a single method.

## install_to_path.py

Installs the standalone scripts below to a folder by wrapping it in a bash file (yes, windows not supported). When this folder is added from the command line, you can use these executables directly without first activating dependencies and such.

## right_strip.py

Strips the whitespace on the right hand side of a file, or recursively when applied on a folder. Can both be used a standalone program or library. Depends on `io_utils.py` and [python-magic](https://pypi.org/project/python-magic/).

## byte_copy.py

A script to copy a set amount of bytes from one file to a new file, with the ability to set an offset. This is useful if you have a file which is larger in size than your thumb drive, so that you can transfer the file in two goes. Can also be used as a library, but always depends on `io_utils.py`. 

## checksum.py

A script to calculate checksums of binary data iteratively. Can be used both standalone (to calculate checksums of files from the command line) and as library. Note that the fletcher checksum implementations are not the most efficient, as it will take a while to calculate the checksums for larger files. When in doubt, use the CRC32 implementation which uses a C backend. This library mostly can be used as abstraction for checksum calculations.

## download_urls.py

A script to download URLs which are stored in a text file. Can also be used as a library to download an URL to a file while (optionally) showing progress. Depends on `io_utils.py`.

## deduplicate_files

A program to deduplicate files in a folder by creating a symlink for duplicate files to the same file on the hard disk. Read more in the [folder](deduplicate_files/README.md). Can both be used as standalone program or library. Depends on both `io_utils.py` and `iter_utils.py`
