"""Mock JSON Database implementation"""

import json
from pathlib import Path
from collections import defaultdict
from typing import Any, Optional, Callable, Iterable
from threading import Lock, Event
from time import time
from logging import getLogger
import uuid

LOGGER = getLogger(__name__)


class JsonDatabaseError(Exception):
    """Base Error class for Json Database handling"""


class DuplicateValue(JsonDatabaseError):
    """Duplicate Value Error"""


class NotFound(JsonDatabaseError):
    """Resource or record not found"""


class NotInitialized(JsonDatabaseError):
    """Attempted to access the instance without initializing"""


class MissingId(JsonDatabaseError):
    """Missing an explicit Id or an Id in the record body"""


class JsonDatabase(object):
    """A Singleton class to manage access to and persistence for the DB data"""

    _instance = None
    records: dict[str, dict[str, dict[str, Any]]]
    db_file: Path | None
    id_field: str
    data_lock: Lock

    def __init__(
        self,
        db_file: Path | None = None,
        id_field: str = "id",
        persist_period_limit: int = 30,
    ):
        self.db_file = db_file
        self.records = defaultdict(dict)
        self.id_field = id_field
        self.persist_period_limit = persist_period_limit
        self.persist_stop = Event()
        self.data_lock = Lock()
        self.data_changed = Event()
        self.dirty = False
        self.last_save = -1  # this will be populated by time.time() during runtime.
        self.logger = LOGGER.getChild(type(self).__name__)
        if not self.db_file:
            self.logger.warning("JSON DB Filepath not provided.")
        elif not self.db_file.exists():
            self.logger.warning(f"JSON DB Filepath {db_file} does not exist.")
        elif not self.db_file.is_file():
            self.logger.warning(f"JSON DB Filepath {db_file} is not a file.")
        else:
            try:
                self.logger.info(f"Loading existing JSON DB from file: {db_file}")
                with self.data_lock, self.db_file.open() as db:
                    self.records.update(
                        {
                            resource: {
                                record[self.id_field]: record
                                for record in resource_records
                            }
                            for resource, resource_records in json.load(db).items()
                        }
                    )
            except Exception as ex:  # pylint: disable=broad-exception-caught
                self.logger.warning(f"Error Loading JSON DB from file {db_file}: {ex}")

    def maintain_data_persistence(self):
        """A Threaded event loop to persist data changes, but not too often."""
        while not self.persist_stop.is_set():
            self.data_changed.wait()
            now = time()
            # debounce to prevent disk thrashing
            if now - self.last_save < self.persist_period_limit:
                try:
                    self.persist_stop.wait(
                        self.persist_period_limit + self.last_save - now
                    )
                except TimeoutError:
                    pass  # we didn't stop the program. this is normal.
            if self.dirty:
                self._persist()

    def _persist(self):
        """Saves the current state of the records to disk"""
        self.last_save = time()
        if not self.db_file:
            if not hasattr(self, "__db_file_missing_warning_sent"):
                setattr(self, "__db_file_missing_warning_sent", True)
                print("No database file specified.")
            return
        self.logger.info("Writing JSON DB changes to storage...")
        with self.data_lock, self.db_file.open("w+") as db:
            if self.data_changed.is_set():
                self.data_changed.clear()
            self.dirty = False
            json.dump(
                {
                    resource: list(resource_records.values())
                    for resource, resource_records in self.records.items()
                },
                db,
                indent=2,
            )

    def shutdown(self):
        """Stop the persist event loop and save current state to disk."""
        # don't need to wait, shutting down
        self.persist_period_limit = 0
        self.persist_stop.set()
        self.data_changed.set()

    def available_resources(self):
        """Returns the set of currently available resources"""
        return sorted(set(self.records.keys()))

    def list_resource(
        self,
        resource: str,
        fields: list[str] | set[str] | None = None,
        filters: list[Callable[[dict[str, Any]], bool]] | None = None,
    ):
        """Lists all records from a resource"""

        if resource not in self.records:
            raise NotFound(f"Unknown Resource {resource}")
        resource_records: Iterable[dict[str, Any]] = self.records[resource].values()

        if filters:
            for record_filter in filters:
                resource_records = filter(record_filter, resource_records)
        if fields:
            resource_records = [
                {key: value for key, value in record.items() if key in fields}
                for record in resource_records
            ]
        return [record.copy() for record in resource_records]

    def read(self, resource: str, record_id: str):
        """Reads a record by id from a resource."""
        if resource not in self.records:
            raise NotFound(f"Unknown Resource {resource}")
        if record_id not in self.records[resource]:
            raise NotFound(
                f"Record [{record_id}] does not exist for resource {resource}"
            )
        return self.records[resource][record_id].copy()

    def create(self, resource, record, record_id: Optional[str] = None):
        """Inserts a record into a resource."""
        if record_id:
            record[self.id_field] = record_id
        elif self.id_field in record:
            record_id = record[self.id_field]

        if not record_id:
            record_id = str(uuid.uuid4())
            record[self.id_field] = record_id

        if resource in self.records and record_id in self.records[resource]:
            raise DuplicateValue(f"Duplicate Record ID on resource {resource}")

        with self.data_lock:
            self.records[resource][record_id] = record
            self.dirty = True
            self.data_changed.set()

        return record.copy()

    def set(
        self, resource: str, record: dict[str, Any], record_id: Optional[str] = None
    ):
        """Replaces a record in a resource with the provided record."""
        if record_id:
            record[self.id_field] = record_id
        elif self.id_field in record:
            record_id = record[self.id_field]

        if not record_id:
            raise MissingId("Missing ID for record")
        with self.data_lock:
            self.records[resource][record_id] = record
            self.dirty = True
            self.data_changed.set()

        return record.copy()

    def update(
        self, resource: str, record: dict[str, Any], record_id: Optional[str] = None
    ):
        """Performs a partial update on a record of a resource."""
        if record_id:
            record[self.id_field] = record_id
        elif self.id_field in record:
            record_id = record[self.id_field]

        if record_id not in self.records[resource]:
            raise NotFound(
                f"Record [{record_id}] does not exist for resource {resource}"
            )
        with self.data_lock:
            self.records[resource][record_id].update(record)
            self.dirty = True
            self.data_changed.set()

        return self.records[resource][record_id].copy()

    def delete(self, resource: str, record_id: str):
        """Deletes the a record from a resource by ID"""
        if resource not in self.records:
            raise NotFound(f"Unknown Resource {resource}")
        if record_id not in self.records[resource]:
            raise NotFound(
                f"Record [{record_id}] does not exist for resource {resource}"
            )
        with self.data_lock:
            del self.records[resource][record_id]
            self.dirty = True
            self.data_changed.set()

        return None
