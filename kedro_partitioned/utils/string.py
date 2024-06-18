"""Utils for string manipulation."""

from pathlib import Path


def get_filepath_extension(filepath: str) -> str:
    """Returns the file extension given a filepath.

    Args:
        filepath (str)

    Returns:
        str

    Example:
        >>> get_filepath_extension('a.txt')
        '.txt'

        >>> get_filepath_extension('.gitignore')
        ''

        >>> get_filepath_extension('a/.gitignore')
        ''

        >>> get_filepath_extension('path/to.file/file.extension')
        '.extension'
    """
    return Path(filepath).suffix


def get_filepath_without_extension(filepath: str) -> str:
    """Returns the same filepath without extension.

    Args:
        filepath (str)

    Returns:
        str

    Example:
        >>> get_filepath_without_extension('/home/a.txt')
        '/home/a'

        >>> get_filepath_without_extension('a.txt')
        'a'

        >>> get_filepath_without_extension('a/.gitignore')
        'a/.gitignore'
    """
    ext = get_filepath_extension(filepath)
    if ext:
        return filepath.rsplit(ext, 1)[0]
    else:
        return filepath
