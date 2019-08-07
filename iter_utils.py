from typing import Any, Iterable, Optional


def get_first(iterable: Iterable[Any],
              default: Optional[Any] = None) -> Optional[Any]:
    """
    Retrieves the first item in the iterable which is not None. When no
    non-None item is found, the default will be returned.

    :param iterable: The iterable to check
    :type iterable: Iterable[Any]
    :param default: The default to return
    :type default: Optional[Any]

    :return: The found item or the default
    :rtype: Optional[Any]
    """
    for item in iterable:
        if item is not None:
            return item
    return default
