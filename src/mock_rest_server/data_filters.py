from typing import Any
from logging import getLogger

LOGGER = getLogger(__name__)


def _record_param_equals_value(param: str, search: str):
    """Generates a curried search filter for whether
    a parameter on a record contains a search string"""
    LOGGER.info("Building filter for [%s] = `%s`", param, search)
    def _filter_func(rec: dict[str, Any]):
        return param in rec and search.casefold() == str(rec[param]).casefold()

    return _filter_func


def _record_param_contains_value(param: str, search: str):
    """Generates a curried search filter for whether
    a parameter on a record contains a search string"""
    LOGGER.info("Building filter for [%s] contains `%s`", param, search)
    def _filter_func(rec: dict[str, Any]):
        return param in rec and search.casefold() in str(rec[param]).casefold()

    return _filter_func


def _record_param_startswith_value(param: str, search: str):
    """Generates a curried search filter for whether
    a parameter on a record starts with a search string"""
    LOGGER.info("Building filter for [%s] starts with `%s`", param, search)

    def _filter_func(rec: dict[str, Any]):
        return (
            param in rec
            and rec[param]
            and str(rec[param]).casefold().startswith(search.casefold())
        )

    return _filter_func


def _record_param_endswith_value(param: str, search: str):
    """Generates a curried search filter for whether
    a parameter on a record ends with a search string"""
    LOGGER.info("Building filter for [%s] ends with `%s`", param, search)

    def _filter_func(rec: dict[str, Any]):
        return (
            param in rec
            and rec[param]
            and str(rec[param]).casefold().endswith(search.casefold())
        )

    return _filter_func


def build_query_filter(param: str, value: str, wild_card: str):
    """Build the appropriate query filter depending on
    the presence and position of a wild card in the value"""
    if value.startswith(wild_card):
        if value.endswith(wild_card):
            search = value.strip(wild_card)
            return _record_param_contains_value(param, search)
        else:
            search = value.lstrip(wild_card)
            return _record_param_endswith_value(param, search)
    elif value.endswith(wild_card):
        search = value.rstrip(wild_card)
        return _record_param_startswith_value(param, search)
    else:
        return _record_param_equals_value(param, value)
