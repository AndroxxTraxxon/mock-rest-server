"""A simple HTTP server with REST and json for python 3.

addrecord takes utf8-encoded URL parameters
getrecord returns utf8-encoded json.
"""

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
import json
from urllib import parse
from typing import Any, Callable

from .database import (
    JsonDatabase,
    JsonDatabaseError,
    DuplicateValue,
    NotFound,
    MissingId,
)


ERROR_RESPONSE_LOOKUP = {
    DuplicateValue: HTTPStatus.BAD_REQUEST,
    NotFound: HTTPStatus.NOT_FOUND,
    MissingId: HTTPStatus.BAD_REQUEST,
}

JSON_CONTENT_TYPE = "application/json"


class JsonHttpRequestHandler(BaseHTTPRequestHandler):
    """JSON HTTP Request Handler"""

    PATH_SEP = "/"
    FIELD_QUERY_PARAM = "_f"
    WILD_CARD = "*"

    def do_POST(self) -> None:
        """Handler function for POST requests"""
        url_parts = parse.urlsplit(self.path)
        slices = url_parts.path.lstrip(self.PATH_SEP).split(self.PATH_SEP)
        if slices == [""]:
            self.http_method_not_allowed()

        resource_type, *rest = slices
        if len(rest) > 1:
            return self.unexpected_request_path()
        elif rest:
            record_id = rest[0]
        else:
            record_id = None

        ctype = self.headers.get("content-type", "")

        # refuse to receive non-json content
        if ctype != JSON_CONTENT_TYPE:
            self.send_response(HTTPStatus.BAD_REQUEST)
            self.end_headers()
            return

        length = int(self.headers.get("content-length", "0"))

        try:
            record = json.loads(self.rfile.read(length))
        except json.JSONDecodeError:
            return self.json_parse_error()
        try:
            created_record = JsonDatabase.instance().create(
                resource_type, record, record_id
            )
        except JsonDatabaseError as ex:
            return self.handle_database_error(ex)

        self.respond_json(created_record)

    def do_GET(self) -> None:
        """Handler function for GET requests"""
        url_parts = parse.urlsplit(self.path)
        slices = url_parts.path.lstrip(self.PATH_SEP).split(self.PATH_SEP)
        if slices == [""]:
            return self.list_available_resources()

        resource_type, *rest = slices
        if resource_type not in JsonDatabase.instance().available_resources():
            return self.not_found()

        if not rest or rest == [""]:
            query_fields = None
            query_filters: list[Callable[[dict[str, Any]], bool]] = []
            if url_parts.query:
                query_params = parse.parse_qs(url_parts.query)
                if self.FIELD_QUERY_PARAM in query_params:
                    query_fields = query_params[self.FIELD_QUERY_PARAM]
                query_filters = self.generate_search_filters(query_params)

            try:
                records = JsonDatabase.instance().list_resource(
                    resource_type, query_fields, query_filters
                )
            except JsonDatabaseError as err:
                return self.handle_database_error(err)

            return self.respond_json(records)
        print(rest)
        if len(rest) > 1:
            return self.unexpected_request_path()

        record_id = rest[0]
        try:
            record = JsonDatabase.instance().read(resource_type, record_id)
        except JsonDatabaseError as err:
            return self.handle_database_error(err)
        if not record:
            return self.not_found()

        self.respond_json(record)
        return

    def list_available_resources(self) -> None:
        """List the currently available resources in the JSON database"""
        payload = {"resources": sorted(JsonDatabase.instance().available_resources())}
        self.respond_json(payload)

    def generate_search_filters(self, query_params: dict[str, list[str]]):
        """Generates the filtering functions for the list"""
        query_filters = []
        for param, values in query_params.items():
            if param == self.FIELD_QUERY_PARAM:
                continue

            for value in values:
                if value.startswith(self.WILD_CARD):
                    if value.endswith(self.WILD_CARD):
                        search = value.strip(self.WILD_CARD)
                        query_filters.append(record_param_contains_value(param, search))
                    else:
                        search = value.lstrip(self.WILD_CARD)
                        query_filters.append(record_param_endswith_value(param, search))
                elif value.endswith(self.WILD_CARD):
                    search = value.rstrip(self.WILD_CARD)
                    query_filters.append(record_param_startswith_value(param, search))
                else:
                    query_filters.append(record_param_equals_value(param, value))
        return query_filters

    def do_PUT(self) -> None:
        url_parts = parse.urlsplit(self.path)
        slices = url_parts.path.lstrip(self.PATH_SEP).split(self.PATH_SEP)
        if slices == [""]:
            return self.http_method_not_allowed()

        resource_type, *rest = slices
        if len(rest) > 1:
            self.unexpected_request_path()
            return
        elif rest:
            record_id = rest[0]
        else:
            self.missing_id()
            return
        ctype = self.headers.get("content-type", "")

        # refuse to receive non-json content
        if ctype != JSON_CONTENT_TYPE:
            self.send_response(HTTPStatus.BAD_REQUEST)
            self.end_headers()
            return

        length = int(self.headers.get("content-length", "0"))

        try:
            record = json.loads(self.rfile.read(length))
        except json.JSONDecodeError:
            self.json_parse_error()
            return
        try:
            updated_record = JsonDatabase.instance().set(
                resource_type, record, record_id
            )
        except JsonDatabaseError as ex:
            return self.handle_database_error(ex)

        self.respond_json(updated_record)
        return

    def do_PATCH(self) -> None:
        """PATCH command response handler"""
        url_parts = parse.urlsplit(self.path)
        slices = url_parts.path.lstrip(self.PATH_SEP).split(self.PATH_SEP)
        if slices == [""]:
            return self.http_method_not_allowed()

        resource_type, *rest = slices
        if len(rest) > 1:
            return self.unexpected_request_path()
        elif rest:
            record_id = rest[0]
        else:
            return self.missing_id()

        ctype = self.headers.get("content-type", "")

        # refuse to receive non-json content
        if ctype != JSON_CONTENT_TYPE:
            self.send_response(HTTPStatus.BAD_REQUEST)
            self.end_headers()
            return

        length = int(self.headers.get("content-length", "0"))
        try:
            record = json.loads(self.rfile.read(length))
        except json.JSONDecodeError:
            return self.json_parse_error()
        try:
            updated_record = JsonDatabase.instance().update(
                resource_type, record, record_id
            )
        except JsonDatabaseError as ex:
            return self.handle_database_error(ex)

        self.respond_json(updated_record)

    def do_DELETE(self) -> None:
        url_parts = parse.urlsplit(self.path)
        slices = url_parts.path.lstrip(self.PATH_SEP).split(self.PATH_SEP)
        if slices == [""]:
            return self.http_method_not_allowed()

        resource_type, *rest = slices
        if len(rest) > 1:
            return self.unexpected_request_path()

        elif rest:
            record_id = rest[0]
        else:
            return self.missing_id()

        try:
            JsonDatabase.instance().delete(resource_type, record_id)
        except JsonDatabaseError as ex:
            return self.handle_database_error(ex)

        self.send_response(HTTPStatus.NO_CONTENT)
        self.end_headers()

    def unexpected_request_path(self):
        """Respond with an unexpected request path error message"""
        self.respond_json(
            {"error": f"Unexpected request path `{self.path}`"}, HTTPStatus.BAD_REQUEST
        )

    def http_method_not_allowed(self):
        """Respond with an HTTP Method not allowed message."""
        self.respond_json(
            {"error": f"Cannot {self.command} records at `{self.path}`."},
            HTTPStatus.METHOD_NOT_ALLOWED,
        )

    def missing_id(self):
        """Respond with a Missing ID error message"""
        self.respond_json({"error": "Missing record ID"}, HTTPStatus.BAD_REQUEST)

    def json_parse_error(self):
        """Respond with a JSON parse error message"""
        self.respond_json(
            {"error": "Unable parse JSON payload"}, HTTPStatus.BAD_REQUEST
        )

    def not_found(self):
        """Respond with a resource not found error message"""
        self.respond_json(
            {"error": "The requested resource does not exist."}, HTTPStatus.NOT_FOUND
        )

    def respond_json(self, json_payload, response_status=HTTPStatus.OK):
        """Respond with a JSON payload."""
        self.send_response(response_status)
        self.send_header("Content-Type", JSON_CONTENT_TYPE)
        self.end_headers()
        body = json.dumps(json_payload)
        self.wfile.write(body.encode())

    def handle_database_error(self, error: JsonDatabaseError):
        """Handle a JsonDatabaseError"""
        response_status = ERROR_RESPONSE_LOOKUP.get(
            type(error), HTTPStatus.INTERNAL_SERVER_ERROR
        )
        self.send_response(response_status)
        self.send_header("Content-Type", JSON_CONTENT_TYPE)
        self.end_headers()
        body = json.dumps({"error": error.args[0]})
        self.wfile.write(body.encode())
        return


def record_param_equals_value(param: str, search: str):
    """Generates a curried search filter for whether
    a parameter on a record contains a search string"""
    print(f"Filtering for records where `{param}` contains `{search}`")

    def _filter_func(rec: dict[str, Any]):
        return param in rec and search.casefold() == str(rec[param]).casefold()

    return _filter_func


def record_param_contains_value(param: str, search: str):
    """Generates a curried search filter for whether
    a parameter on a record contains a search string"""
    print(f"Filtering for records where `{param}` contains `{search}`")

    def _filter_func(rec: dict[str, Any]):
        print(
            f"Checking whether {search.casefold()} is in {str(rec[param]).casefold()}"
        )
        return param in rec and search.casefold() in str(rec[param]).casefold()

    return _filter_func


def record_param_startswith_value(param: str, search: str):
    """Generates a curried search filter for whether
    a parameter on a record starts with a search string"""

    print(f"Filtering for records where `{param}` starts with `{search}`")

    def _filter_func(rec: dict[str, Any]):
        return (
            param in rec
            and rec[param]
            and str(rec[param]).casefold().startswith(search.casefold())
        )

    return _filter_func


def record_param_endswith_value(param: str, search: str):
    """Generates a curried search filter for whether
    a parameter on a record ends with a search string"""
    print(f"Filtering for records where `{param}` ends with `{search}`")

    def _filter_func(rec: dict[str, Any]):
        return (
            param in rec
            and rec[param]
            and str(rec[param]).casefold().endswith(search.casefold())
        )

    return _filter_func
