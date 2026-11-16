import io
import json
import os
import shutil
import tempfile
import warnings
from datetime import date
from decimal import Decimal

from agate import CSVImporter, Table
from agate.csv_importer import JOURNAL_FILENAME
from agate.data_types import Boolean, Date, Number, Text
from agate.exceptions import ImportStateMismatchError
from agate.testcase import AgateTestCase
from agate.type_tester import TypeTester


class TestCSVImporter(AgateTestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir)

        self.path = os.path.join(self.dir, 'data.csv')
        self.state_dir = os.path.join(self.dir, 'state')

        self.column_names = ['number', 'text', 'boolean', 'date']
        self.column_types = [Number(), Text(), Boolean(), Date(date_format='%Y-%m-%d')]

        self.write_csv(
            'number,text,boolean,date\n'
            '1,a,true,2024-01-01\n'
            '2,b,false,2024-01-02\n'
            'oops,c,true,2024-01-03\n'
            '4,d,maybe,2024-01-04\n'
            '5,e,true,not-a-date\n'
            '6,f,false,2024-01-06\n'
        )

    def write_csv(self, contents, path=None):
        with open(path or self.path, 'w', encoding='utf-8') as f:
            f.write(contents)

    def make_importer(self, **kwargs):
        kwargs.setdefault('state_dir', self.state_dir)

        return CSVImporter(self.path, self.column_names, self.column_types, **kwargs)

    def journal_lines(self):
        with open(os.path.join(self.state_dir, JOURNAL_FILENAME), 'rb') as f:
            return [json.loads(line) for line in f.read().decode('utf-8').splitlines()]

    def test_basic_import(self):
        importer = self.make_importer()
        result = importer.run()

        self.assertTrue(importer.done)
        self.assertEqual(importer.progress, 6)

        self.assertColumnNames(result, self.column_names)
        self.assertColumnTypes(result, [Number, Text, Boolean, Date])
        self.assertEqual(len(result.rows), 3)
        self.assertSequenceEqual(result.rows[0], (Decimal(1), 'a', True, date(2024, 1, 1)))
        self.assertSequenceEqual(result.rows[1], (Decimal(2), 'b', False, date(2024, 1, 2)))
        self.assertSequenceEqual(result.rows[2], (Decimal(6), 'f', False, date(2024, 1, 6)))

    def test_result_table_native_values(self):
        importer = self.make_importer()
        result = importer.run()

        self.assertIsInstance(result.rows[0][0], Decimal)
        self.assertIsInstance(result.rows[0][1], str)
        self.assertIsInstance(result.rows[0][2], bool)
        self.assertIsInstance(result.rows[0][3], date)

    def test_quarantine_table(self):
        importer = self.make_importer()
        importer.run()

        quarantine = importer.quarantine_table()

        self.assertColumnNames(quarantine, ['record_number', 'number', 'text', 'boolean', 'date', 'error'])
        self.assertColumnTypes(quarantine, [Number, Text, Text, Text, Text, Text])
        self.assertEqual(len(quarantine.rows), 3)

        self.assertSequenceEqual(
            quarantine.rows[0],
            (Decimal(3), 'oops', 'c', 'true', '2024-01-03', quarantine.rows[0][5])
        )
        self.assertIn('number', quarantine.rows[0][5])
        self.assertIn('oops', quarantine.rows[0][5])

        self.assertEqual([int(r[0]) for r in quarantine.rows], [3, 4, 5])
        self.assertIn('boolean', quarantine.rows[1][5])
        self.assertIn('date', quarantine.rows[2][5])

    def test_batch_commit_contains_results_quarantine_and_progress(self):
        importer = self.make_importer(batch_size=2)
        importer.run()

        lines = self.journal_lines()

        self.assertEqual(lines[0]['type'], 'header')

        batches = [line for line in lines if line['type'] == 'batch']
        self.assertEqual(len(batches), 3)

        # Results, quarantine and progress are saved in the same record
        for batch in batches:
            self.assertIn('results', batch)
            self.assertIn('quarantine', batch)
            self.assertIn('processed', batch)

        self.assertEqual([b['processed'] for b in batches], [2, 4, 6])
        self.assertEqual(len(batches[0]['results']), 2)
        self.assertEqual(len(batches[1]['quarantine']), 2)
        self.assertEqual(len(batches[2]['quarantine']), 1)
        self.assertFalse(batches[0]['done'])
        self.assertTrue(batches[2]['done'])

    def test_journal_is_append_only(self):
        importer = self.make_importer(batch_size=2)
        importer.run(batch_limit=1)

        with open(os.path.join(self.state_dir, JOURNAL_FILENAME), 'rb') as f:
            first_commit = f.read()

        importer.run()

        with open(os.path.join(self.state_dir, JOURNAL_FILENAME), 'rb') as f:
            self.assertTrue(f.read().startswith(first_commit))

    def test_resume_after_close(self):
        importer = self.make_importer(batch_size=2)
        importer.run(batch_limit=1)

        self.assertEqual(importer.progress, 2)
        self.assertFalse(importer.done)

        resumed = self.make_importer(batch_size=2)

        self.assertEqual(resumed.progress, 2)
        self.assertFalse(resumed.done)

        result = resumed.run()

        self.assertEqual(resumed.progress, 6)
        self.assertTrue(resumed.done)

        # Committed records are neither collected twice nor skipped
        self.assertEqual(len(result.rows), 3)
        self.assertSequenceEqual(result.rows[0], (Decimal(1), 'a', True, date(2024, 1, 1)))
        self.assertSequenceEqual(result.rows[1], (Decimal(2), 'b', False, date(2024, 1, 2)))
        self.assertSequenceEqual(result.rows[2], (Decimal(6), 'f', False, date(2024, 1, 6)))

        quarantine = resumed.quarantine_table()
        self.assertEqual([int(r[0]) for r in quarantine.rows], [3, 4, 5])

    def test_resume_after_abnormal_exit(self):
        importer = self.make_importer(batch_size=2)
        importer.run(batch_limit=1)

        journal_path = os.path.join(self.state_dir, JOURNAL_FILENAME)

        # Simulate a crash in the middle of writing the next batch
        with open(journal_path, 'ab') as f:
            f.write(b'{"type": "batch", "proces')

        resumed = self.make_importer(batch_size=2)

        # The torn commit is discarded, the committed progress is kept
        self.assertEqual(resumed.progress, 2)

        with open(journal_path, 'rb') as f:
            self.assertFalse(f.read().endswith(b'proces'))

        result = resumed.run()

        self.assertEqual(resumed.progress, 6)
        self.assertEqual(len(result.rows), 3)
        self.assertEqual(len(resumed.quarantine_table().rows), 3)

    def test_resume_from_mid_file_crash(self):
        importer = self.make_importer(batch_size=2)
        importer.run()

        journal_path = os.path.join(self.state_dir, JOURNAL_FILENAME)

        with open(journal_path, 'rb') as f:
            lines = f.readlines()

        # Keep the header and the first batch, plus a torn half of the second
        os.remove(journal_path)

        other_state_dir = os.path.join(self.dir, 'other_state')
        os.makedirs(other_state_dir)

        with open(os.path.join(other_state_dir, JOURNAL_FILENAME), 'wb') as f:
            f.writelines(lines[:2])
            f.write(lines[2][:len(lines[2]) // 2])

        resumed = CSVImporter(self.path, self.column_names, self.column_types, state_dir=other_state_dir, batch_size=2)

        self.assertEqual(resumed.progress, 2)
        self.assertEqual(len(resumed.result_table().rows), 2)
        self.assertEqual(len(resumed.quarantine_table().rows), 0)

        result = resumed.run()

        self.assertEqual(resumed.progress, 6)
        self.assertEqual(len(result.rows), 3)
        self.assertEqual([int(r[0]) for r in resumed.quarantine_table().rows], [3, 4, 5])

    def test_quoted_newlines_record_numbering(self):
        self.write_csv(
            'number,text,boolean,date\n'
            '1,"line one\nline two",true,2024-01-01\n'
            '2,b,false,2024-01-02\n'
            'oops,c,true,2024-01-03\n'
        )

        importer = self.make_importer(batch_size=1)
        importer.run(batch_limit=1)

        self.assertEqual(importer.progress, 1)

        resumed = self.make_importer(batch_size=1)
        result = resumed.run()

        self.assertEqual(resumed.progress, 3)

        # The bad record is CSV record 3, even though it starts on physical line 5
        quarantine = resumed.quarantine_table()
        self.assertEqual([int(r[0]) for r in quarantine.rows], [3])

        # The quoted newline survives in the good record
        self.assertEqual(result.rows[0][1], 'line one\nline two')

    def test_replay(self):
        importer = self.make_importer()
        importer.run()

        quarantine = importer.quarantine_table()

        fixed = []

        for row in quarantine.rows:
            record_number = int(row[0])
            fields = list(row)[1:5]

            if record_number == 3:
                fields[0] = '30'
            elif record_number == 4:
                fields[2] = 'true'
            elif record_number == 5:
                fields[3] = 'still-bad'

            fixed.append([record_number] + fields)

        updated = importer.replay(fixed)

        # Read progress is unchanged
        self.assertEqual(importer.progress, 6)
        self.assertTrue(importer.done)

        # Fixed records moved into the result set
        result = importer.result_table()
        self.assertEqual(len(result.rows), 5)
        self.assertSequenceEqual(result.rows[3], (Decimal(30), 'c', True, date(2024, 1, 3)))
        self.assertSequenceEqual(result.rows[4], (Decimal(4), 'd', True, date(2024, 1, 4)))

        # Still-failing records remain with the new reason and values
        self.assertEqual(len(updated.rows), 1)
        self.assertEqual(int(updated.rows[0][0]), 5)
        self.assertEqual(updated.rows[0][4], 'still-bad')
        self.assertIn('still-bad', updated.rows[0][5])
        self.assertNotIn('not-a-date', updated.rows[0][5])

    def test_replay_persists_across_reopen(self):
        importer = self.make_importer()
        importer.run()
        importer.replay([[3, '30', 'c', 'true', '2024-01-03']])

        resumed = self.make_importer()

        self.assertEqual(len(resumed.result_table().rows), 4)
        self.assertSequenceEqual(
            resumed.result_table().rows[3],
            (Decimal(30), 'c', True, date(2024, 1, 3))
        )
        self.assertEqual([int(r[0]) for r in resumed.quarantine_table().rows], [4, 5])

    def test_replay_accepts_table(self):
        importer = self.make_importer()
        importer.run()

        quarantine = importer.quarantine_table()
        fixed_rows = []

        for row in quarantine.rows:
            fields = list(row)
            fields[1] = '30'
            fixed_rows.append(fields)

        fixed = Table(fixed_rows, quarantine.column_names, quarantine.column_types)
        updated = importer.replay(fixed)

        self.assertEqual(len(importer.result_table().rows), 4)
        self.assertEqual([int(r[0]) for r in updated.rows], [4, 5])

    def test_replay_unknown_record(self):
        importer = self.make_importer()
        importer.run()

        with self.assertRaises(ValueError):
            importer.replay([[99, '1', 'a', 'true', '2024-01-01']])

    def test_replay_duplicate_record(self):
        importer = self.make_importer()
        importer.run()

        with self.assertRaises(ValueError):
            importer.replay([
                [3, '30', 'c', 'true', '2024-01-03'],
                [3, '31', 'c', 'true', '2024-01-03'],
            ])

    def test_replay_empty_commits_nothing(self):
        importer = self.make_importer()
        importer.run()

        before = len(self.journal_lines())
        importer.replay([])

        self.assertEqual(len(self.journal_lines()), before)

    def test_source_change_raises_and_keeps_old_state(self):
        importer = self.make_importer()
        importer.run()

        journal_path = os.path.join(self.state_dir, JOURNAL_FILENAME)

        with open(journal_path, 'rb') as f:
            before = f.read()

        self.write_csv(
            'number,text,boolean,date\n'
            '1,a,true,2024-01-01\n'
            '2,b,false,2024-01-02\n'
            '3,c,true,2024-01-03\n'
        )

        with self.assertRaises(ImportStateMismatchError):
            self.make_importer()

        # The old snapshot is unchanged
        with open(journal_path, 'rb') as f:
            self.assertEqual(f.read(), before)

        # The saved state still resumes with the original source
        self.write_csv(
            'number,text,boolean,date\n'
            '1,a,true,2024-01-01\n'
            '2,b,false,2024-01-02\n'
            'oops,c,true,2024-01-03\n'
            '4,d,maybe,2024-01-04\n'
            '5,e,true,not-a-date\n'
            '6,f,false,2024-01-06\n'
        )

        resumed = self.make_importer()
        self.assertEqual(resumed.progress, 6)

    def test_config_change_raises(self):
        importer = self.make_importer()
        importer.run()

        with self.assertRaises(ImportStateMismatchError):
            CSVImporter(self.path, self.column_names, [Number(), Text(), Text(), Date(date_format='%Y-%m-%d')],
                        state_dir=self.state_dir)

        with self.assertRaises(ImportStateMismatchError):
            CSVImporter(self.path, ['n', 'text', 'boolean', 'date'], self.column_types, state_dir=self.state_dir)

        with self.assertRaises(ImportStateMismatchError):
            self.make_importer(skip_lines=1)

    def test_batch_size_change_does_not_invalidate(self):
        importer = self.make_importer(batch_size=2)
        importer.run(batch_limit=1)

        resumed = self.make_importer(batch_size=100)

        self.assertEqual(resumed.progress, 2)

    def test_reset(self):
        importer = self.make_importer()
        importer.run()

        self.write_csv(
            'number,text,boolean,date\n'
            '7,g,true,2024-01-07\n'
        )

        fresh = self.make_importer(reset=True)

        self.assertEqual(fresh.progress, 0)
        self.assertFalse(fresh.done)

        result = fresh.run()

        self.assertEqual(len(result.rows), 1)
        self.assertSequenceEqual(result.rows[0], (Decimal(7), 'g', True, date(2024, 1, 7)))

    def test_reset_method(self):
        importer = self.make_importer()
        importer.run()

        importer.reset()

        self.assertEqual(importer.progress, 0)
        self.assertFalse(importer.done)
        self.assertFalse(os.path.exists(os.path.join(self.state_dir, JOURNAL_FILENAME)))

        result = importer.run()

        self.assertEqual(importer.progress, 6)
        self.assertEqual(len(result.rows), 3)

    def test_run_is_idempotent_when_done(self):
        importer = self.make_importer()
        importer.run()

        before = len(self.journal_lines())
        importer.run()

        self.assertEqual(len(self.journal_lines()), before)

    def test_empty_source(self):
        self.write_csv('number,text,boolean,date\n')

        importer = self.make_importer()
        result = importer.run()

        self.assertTrue(importer.done)
        self.assertEqual(importer.progress, 0)
        self.assertColumnNames(result, self.column_names)
        self.assertColumnTypes(result, [Number, Text, Boolean, Date])
        self.assertEqual(len(result.rows), 0)
        self.assertEqual(len(importer.quarantine_table().rows), 0)

    def test_no_column_types(self):
        with self.assertRaises(ValueError):
            CSVImporter(self.path, self.column_names, None, state_dir=self.state_dir)

        with self.assertRaises(ValueError):
            CSVImporter(self.path, self.column_names, TypeTester(), state_dir=self.state_dir)

    def test_dict_column_types(self):
        column_types = {
            'number': Number(),
            'text': Text(),
            'boolean': Boolean(),
            'date': Date(date_format='%Y-%m-%d'),
        }

        importer = CSVImporter(self.path, self.column_names, column_types, state_dir=self.state_dir)
        result = importer.run()

        self.assertColumnTypes(result, [Number, Text, Boolean, Date])
        self.assertEqual(len(result.rows), 3)

    def test_header_false(self):
        self.write_csv(
            '1,a,true,2024-01-01\n'
            'oops,b,false,2024-01-02\n'
        )

        warnings.simplefilter('ignore')

        try:
            importer = CSVImporter(self.path, column_types=self.column_types, state_dir=self.state_dir, header=False)
        finally:
            warnings.resetwarnings()

        result = importer.run()

        self.assertColumnNames(result, ['a', 'b', 'c', 'd'])
        self.assertEqual(len(result.rows), 1)
        self.assertEqual([int(r[0]) for r in importer.quarantine_table().rows], [2])

    def test_skip_lines(self):
        self.write_csv(
            'junk line\n'
            'number,text,boolean,date\n'
            '1,a,true,2024-01-01\n'
        )

        importer = self.make_importer(skip_lines=1)
        result = importer.run()

        self.assertEqual(importer.progress, 1)
        self.assertEqual(len(result.rows), 1)

    def test_row_limit(self):
        importer = self.make_importer(row_limit=3)
        result = importer.run()

        self.assertTrue(importer.done)
        self.assertEqual(importer.progress, 3)
        self.assertEqual(len(result.rows), 2)
        self.assertEqual([int(r[0]) for r in importer.quarantine_table().rows], [3])

    def test_short_row_padded(self):
        self.write_csv(
            'number,text,boolean,date\n'
            '1,a,true\n'
        )

        importer = self.make_importer()
        result = importer.run()

        self.assertEqual(len(result.rows), 1)
        self.assertSequenceEqual(result.rows[0], (Decimal(1), 'a', True, None))

    def test_long_row_quarantined_with_original_fields(self):
        self.write_csv(
            'number,text,boolean,date\n'
            '1,a,true,2024-01-01,extra\n'
        )

        importer = self.make_importer()
        importer.run()

        quarantine = importer.quarantine_table()

        self.assertEqual(len(quarantine.rows), 1)
        self.assertIn('5 values', quarantine.rows[0][5])

        # The journal keeps all the original fields
        batches = [line for line in self.journal_lines() if line['type'] == 'batch']
        self.assertEqual(batches[0]['quarantine'][0]['values'], ['1', 'a', 'true', '2024-01-01', 'extra'])

    def test_file_like_source(self):
        contents = (
            'number,text,boolean,date\n'
            '1,a,true,2024-01-01\n'
            'oops,b,false,2024-01-02\n'
            '3,c,true,2024-01-03\n'
        )

        importer = CSVImporter(io.StringIO(contents), self.column_names, self.column_types,
                               state_dir=self.state_dir, source_id='test', batch_size=2)
        importer.run(batch_limit=1)

        self.assertEqual(importer.progress, 2)

        resumed = CSVImporter(io.StringIO(contents), self.column_names, self.column_types,
                              state_dir=self.state_dir, source_id='test', batch_size=2)
        result = resumed.run()

        self.assertEqual(resumed.progress, 3)
        self.assertEqual(len(result.rows), 2)
        self.assertEqual(len(resumed.quarantine_table().rows), 1)

    def test_file_like_requires_source_id_and_state_dir(self):
        with self.assertRaises(ValueError):
            CSVImporter(io.StringIO(''), self.column_names, self.column_types, state_dir=self.state_dir)

        with self.assertRaises(ValueError):
            CSVImporter(io.StringIO(''), self.column_names, self.column_types, source_id='test')

    def test_file_like_source_id_mismatch(self):
        importer = CSVImporter(io.StringIO('number,text,boolean,date\n1,a,true,2024-01-01\n'),
                               self.column_names, self.column_types, state_dir=self.state_dir, source_id='one')
        importer.run()

        with self.assertRaises(ImportStateMismatchError):
            CSVImporter(io.StringIO('number,text,boolean,date\n1,a,true,2024-01-01\n'),
                        self.column_names, self.column_types, state_dir=self.state_dir, source_id='two')

    def test_default_state_dir(self):
        importer = CSVImporter(self.path, self.column_names, self.column_types)
        importer.run()

        self.assertEqual(importer.state_dir, os.path.abspath(self.path) + '.agate_import')
        self.assertTrue(os.path.exists(os.path.join(importer.state_dir, JOURNAL_FILENAME)))

    def test_invalid_arguments(self):
        with self.assertRaises(ValueError):
            self.make_importer(batch_size=0)

        with self.assertRaises(ValueError):
            self.make_importer(skip_lines='1')

        with self.assertRaises(ValueError):
            CSVImporter(self.path, self.column_names, [Number(), Text()], state_dir=self.state_dir)

        with self.assertRaises(ValueError):
            CSVImporter(self.path, self.column_names, [Number(), Text(), Boolean(), 'date'],
                        state_dir=self.state_dir)
