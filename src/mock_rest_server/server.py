"""A simple HTTP server with REST and json for python 3.

addrecord takes utf8-encoded URL parameters
getrecord returns utf8-encoded json.
"""

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
import json
from urllib import parse
from typing import Any, Callable
import traceback
from .database import (
    JsonDatabase,
    JsonDatabaseError,
    DuplicateValue,
    NotFound,
    MissingId,
)

from .data_filters import build_query_filter

ERROR_RESPONSE_LOOKUP = {
    DuplicateValue: HTTPStatus.BAD_REQUEST,
    NotFound: HTTPStatus.NOT_FOUND,
    MissingId: HTTPStatus.BAD_REQUEST,
}

CONTENT_TYPE_HEADER = "Content-Type"
JSON_CONTENT_TYPE = "application/json"


class RequestBodyReadError(ValueError):
    """An Exception type for when the request body cannot be read."""


class UninitializedDatabase(ValueError):
    """An exception type for when the JSON database has not been configured"""


class JsonHttpResponse:
    """A Data class to represent the content and status
    of JSON-formatted HTTP responses"""

    status: HTTPStatus
    has_body: bool
    body: Any

    def __init__(self, status: HTTPStatus, has_body: bool, body: Any):
        self.status = status
        self.has_body = has_body
        self.body = body

    @classmethod
    def empty(cls, status=HTTPStatus.NO_CONTENT):
        """Build a response with no payload, and only a status"""
        return cls(status, False, None)

    @classmethod
    def with_payload(cls, payload: Any, status=HTTPStatus.OK):
        """Build a response with a payload"""
        return cls(status, True, payload)

    @classmethod
    def with_error(cls, message: str, status=HTTPStatus.BAD_REQUEST):
        """Build a response with an error message"""
        return cls.with_payload({"error": message}, status)

    @classmethod
    def from_exception(cls, ex: Exception, status=HTTPStatus.INTERNAL_SERVER_ERROR):
        """Build a response from an exception"""
        return cls.with_error(
            ex.args[0] if ex.args else f"{type(ex)}: {str(ex)}", status
        )

    @classmethod
    def from_database_error(cls, error: JsonDatabaseError):
        """Handle a JsonDatabaseError"""
        status = ERROR_RESPONSE_LOOKUP.get(
            type(error), HTTPStatus.INTERNAL_SERVER_ERROR
        )
        return cls.from_exception(error, status)


class _StandardResponses:
    """A collection of standard responses and standard response patterns"""

    json_parse_error = JsonHttpResponse.with_error("Unable parse JSON payload")
    internal_error = JsonHttpResponse.with_error(
        "There was an error processing your request.", HTTPStatus.INTERNAL_SERVER_ERROR
    )
    not_found = JsonHttpResponse.with_error(
        "The requested resource could not be found.", HTTPStatus.NOT_FOUND
    )
    missing_id = JsonHttpResponse.with_error("Missing record ID")

    @staticmethod
    def unexpected_path(path: str):
        """Response generator for Unexpected Path error"""
        return JsonHttpResponse.with_error(f"Unexpected request path `{path}`")

    @staticmethod
    def method_not_allowed(method: str, path: str):
        """Response generator for unsupported method error"""
        return JsonHttpResponse.with_error(
            f"Unsupported method {method} at path {path}", HTTPStatus.METHOD_NOT_ALLOWED
        )

    @staticmethod
    def unexpected_content_type(content_type: str):
        """Response generator for unexpected content type error"""
        return JsonHttpResponse.with_error(
            f"Unsupported Content-Type Header value `{content_type}`. "
            f"Expected `{JSON_CONTENT_TYPE}`"
        )


class JsonHttpRequestHandler(BaseHTTPRequestHandler):
    """JSON HTTP Request Handler"""

    PATH_SEP = "/"
    FIELD_QUERY_PARAM = "_f"
    WILD_CARD = "*"
    database: JsonDatabase | None = None

    # internal handler variables used by BaseHTTPRequestHandler
    raw_requestline: str
    requestline: str
    request_version: str
    command: str
    close_connection: bool

    @classmethod
    def configure(cls, **kwargs):
        """Configure the request handler class"""
        if database := kwargs.pop("database", None):
            cls.database = database

    def handle_one_request(self):
        """Handle a single HTTP request."""
        try:
            self.raw_requestline = self.rfile.readline(65537)
            if len(self.raw_requestline) > 65536:
                self.requestline = ""
                self.request_version = ""
                self.command = ""
                self.send_error(HTTPStatus.REQUEST_URI_TOO_LONG)
                return
            if not self.raw_requestline:
                self.close_connection = True
                return
            if not self.parse_request():
                # An error code has been sent, just exit
                return
            mname = "respond_" + self.command.lower()
            if not hasattr(self, mname):
                raise NotImplementedError()
            try:
                if not self.database:
                    raise UninitializedDatabase()
                response = getattr(self, mname)(self.database)
            except JsonDatabaseError as e:
                response = JsonHttpResponse.from_database_error(e)
            except NotImplementedError:
                response = JsonHttpResponse.with_error(
                    f"Unsupported method ({self.command})", HTTPStatus.NOT_IMPLEMENTED
                )
            except UninitializedDatabase:
                response = JsonHttpResponse.with_error(
                    "Database was not initialized", HTTPStatus.INTERNAL_SERVER_ERROR
                )
            except Exception as e:  # pylint: disable=broad-exception-caught
                print(traceback.format_exc())
                response = JsonHttpResponse.from_exception(e)
            self.send_response(response.status)
            if response.has_body:
                self.send_header(CONTENT_TYPE_HEADER, JSON_CONTENT_TYPE)

            self.end_headers()
            if response.has_body:
                self.wfile.write(json.dumps(response.body).encode())
            self.wfile.flush()  # actually send the response if not already done.
        except TimeoutError as e:
            # a read or a write timed out.  Discard this connection
            self.log_error("Request timed out: %r", e)
            self.close_connection = True
        return

    def read_request_body(self):
        """Read the content of the Request body as JSON content."""
        ctype = self.headers.get(CONTENT_TYPE_HEADER.lower(), "")
        if ctype != JSON_CONTENT_TYPE:
            message = (
                f"Unsupported Content-Type Header value `{ctype}`. "
                f"Expected `{JSON_CONTENT_TYPE}`"
            )
            raise ValueError(message)

        length = int(self.headers.get("content-length", "0"))

        return json.loads(self.rfile.read(length))

    def respond_post(self, database: JsonDatabase) -> JsonHttpResponse:
        """Handler function for POST requests"""
        url_parts = parse.urlsplit(self.path)
        slices = url_parts.path.lstrip(self.PATH_SEP).split(self.PATH_SEP)
        if slices == [""]:
            return _StandardResponses.method_not_allowed(self.command, self.path)

        resource_type, *rest = slices
        if len(rest) > 1:
            return _StandardResponses.unexpected_path(self.path)
        elif rest:
            record_id = rest[0]
        else:
            record_id = None

        try:
            record = self.read_request_body()

            created_record = database.create(
                resource_type, record, record_id
            )

            return JsonHttpResponse.with_payload(created_record)
        except json.JSONDecodeError:
            return _StandardResponses.json_parse_error
        except ValueError as er:
            return JsonHttpResponse.from_exception(er, HTTPStatus.BAD_REQUEST)

    def respond_get(self, database: JsonDatabase) -> JsonHttpResponse:
        """Handler function for GET requests"""
        url_parts = parse.urlsplit(self.path)
        slices = url_parts.path.lstrip(self.PATH_SEP).split(self.PATH_SEP)
        if slices == [""]:
            return JsonHttpResponse.with_payload(
                {"resources": list(database.available_resources())}
            )

        resource_type, *rest = slices
        if resource_type not in database.available_resources():
            return _StandardResponses.not_found

        if not rest or rest == [""]:
            query_fields = None
            query_filters: list[Callable[[dict[str, Any]], bool]] = []
            if url_parts.query:
                query_params = parse.parse_qs(url_parts.query)
                if self.FIELD_QUERY_PARAM in query_params:
                    query_fields = query_params[self.FIELD_QUERY_PARAM]
                query_filters = self.generate_search_filters(query_params)

            records = database.list_resource(
                resource_type, query_fields, query_filters
            )

            return JsonHttpResponse.with_payload(records)

        if len(rest) > 1:
            return _StandardResponses.unexpected_path(self.path)

        record_id = rest[0]
        record = database.read(resource_type, record_id)
        if not record:
            return _StandardResponses.not_found

        return JsonHttpResponse.with_payload(record)

    def generate_search_filters(self, query_params: dict[str, list[str]]):
        """Generates the filtering functions for the list"""
        query_filters = []
        for param, values in query_params.items():
            if param == self.FIELD_QUERY_PARAM:
                continue

            for value in values:
                query_filters.append(build_query_filter(param, value, self.WILD_CARD))
        return query_filters

    def respond_put(self, database: JsonDatabase) -> JsonHttpResponse:
        """PUT command response handler"""
        url_parts = parse.urlsplit(self.path)
        slices = url_parts.path.lstrip(self.PATH_SEP).split(self.PATH_SEP)
        if slices == [""]:
            return _StandardResponses.method_not_allowed(self.command, url_parts.path)

        resource_type, *rest = slices
        if len(rest) > 1:
            return _StandardResponses.unexpected_path(url_parts.path)
        elif rest:
            record_id = rest[0]
        else:
            return _StandardResponses.missing_id

        try:
            record = self.read_request_body()
        except json.JSONDecodeError:
            return _StandardResponses.json_parse_error
        except ValueError as er:
            return JsonHttpResponse.from_exception(er, HTTPStatus.BAD_REQUEST)

        updated_record = database.set(resource_type, record, record_id)

        return JsonHttpResponse.with_payload(updated_record)

    def respond_patch(self, database: JsonDatabase) -> JsonHttpResponse:
        """PATCH command response handler"""
        url_parts = parse.urlsplit(self.path)
        slices = url_parts.path.lstrip(self.PATH_SEP).split(self.PATH_SEP)
        if slices == [""]:
            return _StandardResponses.method_not_allowed(self.command, url_parts.path)

        resource_type, *rest = slices
        if len(rest) > 1:
            return _StandardResponses.unexpected_path(url_parts.path)
        elif rest:
            record_id = rest[0]
        else:
            return _StandardResponses.missing_id

        try:
            record = self.read_request_body()
        except json.JSONDecodeError:
            return _StandardResponses.json_parse_error
        except ValueError as er:
            return JsonHttpResponse.from_exception(er, HTTPStatus.BAD_REQUEST)

        updated_record = database.update(
            resource_type, record, record_id
        )

        return JsonHttpResponse.with_payload(updated_record)

    def respond_delete(self, database: JsonDatabase) -> JsonHttpResponse:
        """DELETE command response handler"""
        url_parts = parse.urlsplit(self.path)
        slices = url_parts.path.lstrip(self.PATH_SEP).split(self.PATH_SEP)
        if slices == [""]:
            return _StandardResponses.method_not_allowed(self.command, url_parts.path)

        resource_type, *rest = slices
        if len(rest) > 1:
            return _StandardResponses.unexpected_path(url_parts.path)

        elif rest:
            record_id = rest[0]
        else:
            return _StandardResponses.missing_id

        database.delete(resource_type, record_id)

        return JsonHttpResponse.empty()
