"""
This module contains the :class:`CSVImporter` class, a resumable, batched,
cleansing importer for CSV data.
"""

import hashlib
import io
import json
import os
import warnings
from datetime import date, datetime, timedelta
from decimal import Decimal

from agate.data_types import DataType, Number, Text
from agate.exceptions import CastError, ImportStateMismatchError
from agate.table import Table
from agate.type_tester import TypeTester
from agate.utils import letter_name

#: Version of the journal format written by this module.
FORMAT_VERSION = 1

#: Name of the journal file inside the state directory.
JOURNAL_FILENAME = 'journal.jsonl'

#: Size of the chunks read when hashing the source for its fingerprint.
HASH_CHUNK_BYTES = 65536


def _json_safe(value):
    """
    Convert a value into a JSON-serializable form with a stable
    representation.
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]

    return repr(value)


def _type_spec(data_type):
    """
    Build a stable, JSON-serializable description of a :class:`.DataType`
    instance, including its public configuration attributes.
    """
    spec = {'class': type(data_type).__name__}

    for key, value in sorted(data_type.__dict__.items()):
        if key.startswith('_'):
            continue

        spec[key] = _json_safe(value)

    return spec


def _encode_value(value):
    """
    Encode a cast (native) value so it can be stored in the journal as JSON
    without losing its type.
    """
    if value is None or isinstance(value, (str, bool, int, float)):
        return value

    if isinstance(value, Decimal):
        return {'__decimal__': str(value)}

    if isinstance(value, datetime):
        return {'__datetime__': value.isoformat()}

    if isinstance(value, date):
        return {'__date__': value.isoformat()}

    if isinstance(value, timedelta):
        return {'__timedelta__': [value.days, value.seconds, value.microseconds]}

    raise TypeError('Can not serialize value of type "%s".' % type(value).__name__)


def _decode_value(value):
    """
    Reverse :func:`_encode_value`.
    """
    if isinstance(value, dict) and len(value) == 1:
        if '__decimal__' in value:
            return Decimal(value['__decimal__'])
        if '__datetime__' in value:
            return datetime.fromisoformat(value['__datetime__'])
        if '__date__' in value:
            return date.fromisoformat(value['__date__'])
        if '__timedelta__' in value:
            days, seconds, microseconds = value['__timedelta__']
            return timedelta(days=days, seconds=seconds, microseconds=microseconds)

    return value


def _to_line(record):
    """
    Serialize a journal record to a single line of JSON.
    """
    return json.dumps(record, ensure_ascii=False, sort_keys=True) + '\n'


class CSVImporter:
    """
    A resumable, batched and cleansing CSV importer.

    Unlike :meth:`.Table.from_csv`, which aborts the entire import when a
    single value can not be cast, the importer casts each record with the
    declared column types and sorts records into two outputs:

    * Good records are collected into the result set.
    * Bad records are isolated in a quarantine, together with their original
      (unmodified) field values and the reason the cast failed.

    Records are processed in batches. After each batch the new results, the
    new quarantine entries and the read progress (the number of source
    records processed so far) are committed to a journal on disk in a single
    atomic write, so the three can never disagree. If the process is closed
    or exits abnormally, a new importer created with the same source,
    configuration and ``state_dir`` resumes reading after the last committed
    record: committed records are never collected twice and no record is
    skipped. Records are numbered by CSV record (as opposed to physical
    line), so fields containing quoted newlines do not disturb the numbering.

    Once the bad records have been fixed, :meth:`replay` re-casts only the
    quarantined records. Those that now cast cleanly are moved into the
    result set; those that still fail remain in the quarantine with the new
    reason. Replaying does not change the read progress.

    :meth:`result_table` and :meth:`quarantine_table` both return native
    :class:`.Table` instances. The result table uses the declared column
    types and preserves native values such as :class:`decimal.Decimal`.

    The saved state is bound to a fingerprint of the import source and
    configuration. The source is identified by its path, size and a hash of
    its full contents, which is computed when the importer is created. If
    the source or the configuration changes, the saved position is not
    reused: an :class:`.ImportStateMismatchError` is raised instead and the
    old state is left untouched. Pass ``reset=True`` (or call :meth:`reset`
    on an importer that successfully opened) to explicitly discard the old
    state and start over.

    The journal is append-only: committed batches are never modified, so
    previously saved snapshots of the import remain unchanged.

    A single importer instance is not thread-safe, and a state directory
    must not be written to by more than one process at a time.

    :param path:
        Filepath or file-like object to read CSV data from. File-like
        objects must be seekable; the importer always reads the source from
        the beginning, skipping already committed records.
    :param column_names:
        A sequence of column names. If not specified and ``header`` is
        :code:`True`, the first row of the CSV is used. If not specified and
        ``header`` is :code:`False`, names are assigned using
        :func:`.letter_name`.
    :param column_types:
        The declared native types the records are cast with. A sequence of
        :class:`.DataType` instances, or a dictionary mapping column names
        to :class:`.DataType` instances. Required: type inference with
        :class:`.TypeTester` is not supported, because the importer streams
        records instead of reading them all first.
    :param state_dir:
        Directory in which the import state (the journal) is kept. Defaults
        to a directory named after the source file with an
        ``.agate_import`` suffix. Required for file-like sources.
    :param batch_size:
        The number of records processed between commits. A crash loses at
        most one uncommitted batch. Does not affect the contents of the
        result; may be changed between runs.
    :param skip_lines:
        The number of lines to skip from the top of the file.
    :param header:
        If :code:`True`, the first row of the CSV is assumed to contain
        column names.
    :param encoding:
        Character encoding of the CSV file.
    :param sniff_limit:
        Limit CSV dialect sniffing to the specified number of bytes. Set to
        None to sniff the entire file. Defaults to 0 (no sniffing).
    :param row_limit:
        Limit how many records will be read.
    :param source_id:
        An identifier for a file-like source, used to match saved progress
        to the source. Required when ``path`` is file-like.
    :param reset:
        If :code:`True`, any previously saved state in ``state_dir`` is
        discarded before the import begins.
    :param kwargs:
        Additional keyword arguments are passed through to the CSV reader.
    """

    #: Name of the quarantine column containing the CSV record number.
    RECORD_NUMBER_COLUMN = 'record_number'

    #: Name of the quarantine column containing the failure reason.
    ERROR_COLUMN = 'error'

    def __init__(self, path, column_names=None, column_types=None, state_dir=None, batch_size=1000,
                 skip_lines=0, header=True, encoding='utf-8', sniff_limit=0, row_limit=None,
                 source_id=None, reset=False, **kwargs):
        self._path = path
        self._header = header
        self._encoding = encoding
        self._sniff_limit = sniff_limit
        self._row_limit = row_limit
        self._csv_kwargs = kwargs

        if not isinstance(skip_lines, int):
            raise ValueError('skip_lines argument must be an int')

        self._skip_lines = skip_lines

        if not isinstance(batch_size, int) or batch_size < 1:
            raise ValueError('batch_size must be a positive integer')

        self._batch_size = batch_size

        # Validate column types
        if column_types is None:
            raise ValueError(
                'column_types must be declared for a cleansing import. '
                'Type inference with TypeTester is not supported.'
            )
        elif isinstance(column_types, TypeTester):
            raise ValueError(
                'column_types must be instances of DataType. '
                'Type inference with TypeTester is not supported.'
            )
        elif isinstance(column_types, dict):
            if column_names is None:
                raise ValueError('column_names are required when column_types is a dictionary.')

            for v in column_types.values():
                if not isinstance(v, DataType):
                    raise ValueError('Column types must be instances of DataType.')

            try:
                column_types = [column_types[name] for name in column_names]
            except KeyError as e:
                raise ValueError('column_types is missing a type for column %s.' % e)
        else:
            column_types = list(column_types)

            for column_type in column_types:
                if not isinstance(column_type, DataType):
                    raise ValueError('Column types must be instances of DataType.')

        self._column_types = tuple(column_types)

        if column_names is not None:
            column_names = tuple(column_names)

            if len(column_names) != len(self._column_types):
                raise ValueError('column_names and column_types must be the same length.')

        self._column_names = column_names

        # Validate the source and state directory
        if hasattr(path, 'read'):
            if source_id is None:
                raise ValueError(
                    'source_id is required when importing from a file-like object, '
                    'so that saved progress can be matched to the source.'
                )

            if state_dir is None:
                raise ValueError('state_dir is required when importing from a file-like object.')

            try:
                path.seek(0)
            except (AttributeError, OSError):
                raise ValueError('File-like sources must be seekable.')
        else:
            if state_dir is None:
                state_dir = os.path.abspath(path) + '.agate_import'

        self._state_dir = state_dir
        self._journal_path = os.path.join(state_dir, JOURNAL_FILENAME)

        # In-memory state
        self._result_rows = []
        self._quarantine = {}
        self._processed = 0
        self._done = False
        self._initialized = False

        # Fingerprint of the source and configuration
        self._fingerprint = {
            'format': FORMAT_VERSION,
            'source': self._source_fingerprint(source_id),
            'config': {
                'column_names': list(column_names) if column_names is not None else None,
                'column_types': [_type_spec(t) for t in self._column_types],
                'encoding': encoding,
                'skip_lines': skip_lines,
                'header': header,
                'sniff_limit': sniff_limit,
                'row_limit': row_limit,
                'csv_kwargs': {k: _json_safe(v) for k, v in sorted(kwargs.items())},
            },
        }

        if reset:
            self._discard_state()

        # Load any previously committed state, validating the fingerprint
        self._load()

        # Resolve column names
        if self._column_names is None:
            if header:
                self._column_names = self._read_header_names()
            else:
                self._column_names = tuple(letter_name(i) for i in range(len(self._column_types)))
                warnings.warn('Column names not specified. "%s" will be used as names.' % str(self._column_names),
                              RuntimeWarning, stacklevel=2)

    @property
    def state_dir(self):
        """
        The directory in which the import state is kept.
        """
        return self._state_dir

    @property
    def progress(self):
        """
        The number of source records committed so far.
        """
        return self._processed

    @property
    def done(self):
        """
        Whether the source has been read to the end (or to ``row_limit``)
        and the final batch committed.
        """
        return self._done

    def run(self, batch_limit=None):
        """
        Read records from the source, resuming after the last committed
        record, and commit batches until the source is exhausted (or
        ``row_limit`` is reached).

        Returns the current result set as a :class:`.Table`, including
        results committed by previous runs.

        :param batch_limit:
            The maximum number of batches to commit in this run. By default
            all remaining records are processed.
        """
        if self._done:
            return self.result_table()

        close = False

        if hasattr(self._path, 'read'):
            f = self._path
            f.seek(0)
        else:
            f = open(self._path, encoding=self._encoding)
            close = True

        try:
            self._consume_skip_lines(f)

            handle, reader = self._make_reader(f)

            if self._header:
                # Discard the header row
                next(reader, None)

            # Skip records committed by previous runs
            for _ in range(self._processed):
                try:
                    next(reader)
                except StopIteration:
                    raise ImportStateMismatchError(
                        'The source ended before the committed progress of %i records. '
                        'The saved state in "%s" does not match the source.' % (self._processed, self._state_dir)
                    )

            cast_funcs = [t.cast for t in self._column_types]
            processed = self._processed
            batches = 0
            lookahead = None
            exhausted = False

            while True:
                batch_results = []
                batch_quarantine = []

                while len(batch_results) + len(batch_quarantine) < self._batch_size:
                    if lookahead is not None:
                        raw = lookahead
                        lookahead = None
                    else:
                        if self._row_limit is not None and processed >= self._row_limit:
                            exhausted = True
                            break

                        try:
                            raw = next(reader)
                        except StopIteration:
                            exhausted = True
                            break

                    processed += 1

                    values, reason = self._cast_record(raw, cast_funcs)

                    if reason is None:
                        batch_results.append([_encode_value(v) for v in values])
                    else:
                        batch_quarantine.append({
                            'record': processed,
                            'values': [v if v is None or isinstance(v, str) else str(v) for v in raw],
                            'reason': reason,
                        })

                if not exhausted:
                    # The batch is full; check whether the source is exhausted
                    if self._row_limit is not None and processed >= self._row_limit:
                        exhausted = True
                    else:
                        try:
                            lookahead = next(reader)
                        except StopIteration:
                            exhausted = True

                self._commit({
                    'type': 'batch',
                    'processed': processed,
                    'results': batch_results,
                    'quarantine': batch_quarantine,
                    'done': exhausted,
                })

                batches += 1

                if exhausted:
                    break

                if batch_limit is not None and batches >= batch_limit:
                    break
        finally:
            if close:
                f.close()

        return self.result_table()

    def replay(self, rows):
        """
        Re-cast quarantined records, typically after their values have been
        fixed. Only the records included in ``rows`` are replayed.

        Records that now cast cleanly are moved into the result set. Records
        that still fail remain in the quarantine with the new reason and the
        new field values. The read progress is not changed. The replay is
        committed as a single atomic record.

        Returns the updated quarantine as a :class:`.Table`.

        :param rows:
            A :class:`.Table` or a sequence of sequences with the quarantine
            structure: the record number, followed by one value per declared
            column. A table returned by :meth:`quarantine_table`, possibly
            with edited values, may be passed directly; its trailing error
            column is ignored.
        """
        if isinstance(rows, Table):
            rows = rows.rows

        cast_funcs = [t.cast for t in self._column_types]
        len_column_types = len(self._column_types)

        resolved = []
        replay_results = []
        replay_quarantine = []
        seen = set()

        for row in rows:
            record_number = int(row[0])

            if record_number in seen:
                raise ValueError('Record %i was included in the replay more than once.' % record_number)

            seen.add(record_number)

            if record_number not in self._quarantine:
                raise ValueError('Record %i is not in the quarantine.' % record_number)

            fields = list(row)[1:len_column_types + 1]

            if len(fields) < len_column_types:
                fields += [None] * (len_column_types - len(fields))

            values, reason = self._cast_record(fields, cast_funcs)

            if reason is None:
                resolved.append(record_number)
                replay_results.append([_encode_value(v) for v in values])
            else:
                replay_quarantine.append({
                    'record': record_number,
                    'values': [v if v is None or isinstance(v, str) else str(v) for v in fields],
                    'reason': reason,
                })

        if not seen:
            return self.quarantine_table()

        self._commit({
            'type': 'replay',
            'resolved': resolved,
            'results': replay_results,
            'quarantine': replay_quarantine,
        })

        return self.quarantine_table()

    def result_table(self):
        """
        The current result set as a :class:`.Table` with the declared column
        names and types. Native values, such as :class:`decimal.Decimal`,
        are preserved.
        """
        return Table(list(self._result_rows), self._column_names, self._column_types)

    def quarantine_table(self):
        """
        The current quarantine as a :class:`.Table` with a
        :code:`record_number` column, one :class:`.Text` column per declared
        column holding the original field values, and an :code:`error`
        column holding the reason the record was quarantined.
        """
        len_column_types = len(self._column_types)
        column_names = [self.RECORD_NUMBER_COLUMN] + list(self._column_names) + [self.ERROR_COLUMN]
        column_types = [Number()] + [Text() for _ in self._column_names] + [Text()]

        rows = []

        for record_number in sorted(self._quarantine):
            values, reason = self._quarantine[record_number]
            values = list(values[:len_column_types])

            if len(values) < len_column_types:
                values += [None] * (len_column_types - len(values))

            rows.append([record_number] + values + [reason])

        return Table(rows, column_names, column_types)

    def reset(self):
        """
        Discard the saved state and start the import over. The next run
        reads the source from the beginning.
        """
        self._discard_state()

    def _discard_state(self):
        """
        Delete the journal and clear the in-memory state.
        """
        if os.path.exists(self._journal_path):
            os.remove(self._journal_path)

        try:
            os.rmdir(self._state_dir)
        except OSError:
            pass

        self._result_rows = []
        self._quarantine = {}
        self._processed = 0
        self._done = False
        self._initialized = False

    def _source_fingerprint(self, source_id):
        """
        Compute the source part of the fingerprint: the absolute path, size
        and a hash of the full contents, so that any change to the source
        invalidates the saved position.
        """
        if hasattr(self._path, 'read'):
            return {'id': source_id}

        abspath = os.path.abspath(self._path)

        hasher = hashlib.sha256()

        with open(abspath, 'rb') as f:
            for chunk in iter(lambda: f.read(HASH_CHUNK_BYTES), b''):
                hasher.update(chunk)

        return {
            'path': abspath,
            'size': os.path.getsize(abspath),
            'sha256': hasher.hexdigest(),
        }

    def _consume_skip_lines(self, f):
        """
        Skip the configured number of lines from the top of the file.
        """
        skip_lines = self._skip_lines

        while skip_lines > 0:
            f.readline()
            skip_lines -= 1

    def _make_reader(self, f):
        """
        Create a CSV reader for the given file, applying dialect sniffing.
        """
        from agate import csv

        handle = f
        kwargs = dict(self._csv_kwargs)

        if self._sniff_limit is None:
            # Overwrite `handle` to not read the file a second time in `csv.reader`.
            handle = io.StringIO(f.read())
            sample = handle.getvalue()
        elif self._sniff_limit > 0:
            offset = f.tell()
            sample = f.read(self._sniff_limit)
            f.seek(offset)

        if self._sniff_limit is None or self._sniff_limit > 0:
            kwargs['dialect'] = csv.Sniffer().sniff(sample)

        return handle, csv.reader(handle, header=self._header, **kwargs)

    def _read_header_names(self):
        """
        Read the column names from the first row of the source.
        """
        close = False

        if hasattr(self._path, 'read'):
            f = self._path
            f.seek(0)
        else:
            f = open(self._path, encoding=self._encoding)
            close = True

        try:
            self._consume_skip_lines(f)

            handle, reader = self._make_reader(f)

            try:
                names = next(reader)
            except StopIteration:
                raise ValueError(
                    'column_names could not be determined because the source is empty. '
                    'Pass column_names explicitly.'
                )

            return tuple(names)
        finally:
            if close:
                f.close()

    def _cast_record(self, raw, cast_funcs):
        """
        Cast a single record with the declared column types. Returns the
        cast values, or the reason the record was quarantined.
        """
        len_column_types = len(self._column_types)

        if len(raw) > len_column_types:
            return None, 'Record has %i values, but only %i columns were declared.' % (len(raw), len_column_types)

        fields = list(raw)

        if len(fields) < len_column_types:
            fields += [None] * (len_column_types - len(fields))

        values = []

        for i, field in enumerate(fields):
            try:
                values.append(cast_funcs[i](field))
            except CastError as e:
                return None, "Column '%s': %s" % (self._column_names[i], e)

        return values, None

    def _commit(self, record):
        """
        Atomically commit a record to the journal: the record (and the
        header, if this is the first commit) is written and synced to disk
        in a single write, then applied to the in-memory state.
        """
        lines = []

        if not self._initialized:
            lines.append(_to_line({
                'type': 'header',
                'format': FORMAT_VERSION,
                'fingerprint': self._fingerprint,
                'column_names': list(self._column_names),
            }))

        lines.append(_to_line(record))

        os.makedirs(self._state_dir, exist_ok=True)

        with open(self._journal_path, 'ab') as f:
            f.write(''.join(lines).encode('utf-8'))
            f.flush()
            os.fsync(f.fileno())

        self._initialized = True

        self._apply(record)

    def _load(self):
        """
        Load previously committed state from the journal, validating the
        fingerprint. A torn tail left by an abnormal exit is truncated, so
        only fully committed batches are kept.
        """
        if not os.path.exists(self._journal_path):
            return

        committed_size = 0
        first = True

        with open(self._journal_path, 'rb') as f:
            while True:
                offset = f.tell()
                line = f.readline()

                if not line:
                    # Clean end of file
                    committed_size = offset
                    break

                if not line.endswith(b'\n'):
                    # Torn write: the last commit was not completed
                    committed_size = offset
                    break

                try:
                    record = json.loads(line)
                except ValueError:
                    # Torn write: the last commit was not completed
                    committed_size = offset
                    break

                if first and (not isinstance(record, dict) or record.get('type') != 'header'):
                    raise ValueError('The journal in "%s" is corrupt.' % self._state_dir)

                first = False

                self._apply(record)

        if committed_size < os.path.getsize(self._journal_path):
            with open(self._journal_path, 'r+b') as f:
                f.truncate(committed_size)

        self._initialized = committed_size > 0

    def _apply(self, record):
        """
        Apply a committed journal record to the in-memory state.
        """
        if not isinstance(record, dict):
            raise ValueError('The journal in "%s" is corrupt.' % self._state_dir)

        record_type = record.get('type')

        if record_type == 'header':
            if record.get('format') != FORMAT_VERSION:
                raise ValueError(
                    'The journal in "%s" uses an unsupported format version. ' % self._state_dir
                )

            fingerprint = record.get('fingerprint')

            if fingerprint != self._fingerprint:
                differences = []

                if isinstance(fingerprint, dict):
                    for key in ('source', 'config'):
                        if fingerprint.get(key) != self._fingerprint.get(key):
                            differences.append(key)

                what = ' and '.join(differences) if differences else 'fingerprint'

                raise ImportStateMismatchError(
                    'The saved import state in "%s" does not match this import: the %s changed. '
                    'The saved state was left untouched. If this change is intentional, '
                    'pass reset=True to discard it or use a different state_dir.' % (self._state_dir, what)
                )

            names = record.get('column_names')

            if not isinstance(names, list):
                raise ValueError('The journal in "%s" is corrupt.' % self._state_dir)

            if self._column_names is not None and tuple(names) != self._column_names:
                raise ImportStateMismatchError(
                    'The saved import state in "%s" was created with different column names. '
                    'The saved state was left untouched. If this change is intentional, '
                    'pass reset=True to discard it or use a different state_dir.' % self._state_dir
                )

            self._column_names = tuple(names)
        elif record_type == 'batch':
            try:
                results = record['results']
                quarantine = record['quarantine']
                processed = record['processed']
            except (KeyError, TypeError):
                raise ValueError('The journal in "%s" is corrupt.' % self._state_dir)

            for row in results:
                self._result_rows.append(tuple(_decode_value(v) for v in row))

            for entry in quarantine:
                self._quarantine[entry['record']] = (entry['values'], entry['reason'])

            self._processed = processed

            if record.get('done'):
                self._done = True
        elif record_type == 'replay':
            try:
                resolved = record['resolved']
                results = record['results']
                quarantine = record['quarantine']
            except (KeyError, TypeError):
                raise ValueError('The journal in "%s" is corrupt.' % self._state_dir)

            for record_number in resolved:
                self._quarantine.pop(record_number, None)

            for row in results:
                self._result_rows.append(tuple(_decode_value(v) for v in row))

            for entry in quarantine:
                self._quarantine[entry['record']] = (entry['values'], entry['reason'])
        else:
            raise ValueError('The journal in "%s" is corrupt.' % self._state_dir)
