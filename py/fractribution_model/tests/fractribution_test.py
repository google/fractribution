# coding=utf-8
# Copyright 2020 Google LLC..
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Run: python3 -m unittest -v fractribution_test.py

import csv
from typing import Any, Dict, List, Tuple
import unittest
import fractribution
import fractribution_report
import tuple_transforms


class TestFractribution(unittest.TestCase):

  def _extract_path_summary(self):
    """Extract path summary input to Fractribution."""
    paths = []
    with open('tests/example_path_summary.csv') as csv_file:
      csv_reader = csv.DictReader(csv_file)
      for row in csv_reader:
        paths.append((
            row['path'],
            int(row['total_conversions']),
            int(row['total_non_conversions'])))
    return paths

  def _extract_customers(self) -> List[Tuple[str, str, int]]:
    """Extract the list of customers from the tests/customer.csv file.

    Returns:
      List of customer tuples, consisting of path to conversion, id and
      conversion time.
    """
    customers = []
    with open('tests/customers.csv') as csv_file:
      csv_reader = csv.DictReader(csv_file)
      for row in csv_reader:
        customers.append((
            row['path'], row['customer_id'], row['endpoint_datetime']))
    return customers

  def _extract_expected_output(
      self, expected_output_csv: str) -> Dict[str, Any]:
    """Extract the expected row output in the given file.

    Args:
      expected_output_csv: CSV file containing expected output rows.
    Returns:
      List of expected output rows as a Dictionary from column name to value.
    """
    customer_id_to_row = {}
    with open(expected_output_csv) as csv_file:
      csv_reader = csv.DictReader(csv_file)
      for row in csv_reader:
        for channel in self._channels:
          channel = fractribution_report._format_channel(channel)
          row[channel] = float(row[channel])
        row['total_attribution'] = 1.0
        customer_id_to_row[row['customer_id']] = row
    return customer_id_to_row

  def _check_row_equality(
      self, row1: Dict[str, Any], row2: Dict[str, Any]) -> bool:
    """Returns True if row1 and row 2 are equal and False otherwise.

    Args:
      row1: Dictionary from column name to value.
      row2: Dictionary from column name to value.
    Returns:
      True if row1 and row2 are equal, or almostEqual if the value is a float.
    """
    self.assertTrue(len(row1), len(row2))
    for (key, value1) in row1.items():
      self.assertIn(key, row2)
      value2 = row2[key]
      if isinstance(value1, float) or isinstance(value2, float):
        self.assertAlmostEqual(value1, value2)
      else:
        self.assertEqual(value1, value2)

  def setUp(self):
    super(TestFractribution, self).setUp()
    self._paths = self._extract_path_summary()
    self._customers = self._extract_customers()
    self._channels = 'A,B,C,D,E,F,G,H,I,J,K,Unmatched Channel'.split(',')
    self._report_window_start = '2016-08-01'
    self._report_window_end = '2016-08-31'

  def fractribution_test(self, transform_name: str, expected_output_csv: str):
    """End-to-end test of Fractribution with the given path transform.

    Args:
      transform_name: Name of the path transform method.
      expected_output_csv: Name of the CSV file containing the expected output
          for Fractribution using this transform.
    """
    path_transform = tuple_transforms.transform_name_to_function[transform_name]
    # Step 1: Transform the paths.
    (transformed_tuple_to_path, channel_to_encoding) = (
        fractribution.transform_paths(self._paths, path_transform))

    # Step 2: Run fractional attribution.
    fractribution.compute_fractional_values(transformed_tuple_to_path)
    fractribution.normalize_event_names(
        transformed_tuple_to_path, transform_name)

    # Step 3: Join customer-level paths with path fractional attribution info.
    # Extract customer-level path information.
    rows = fractribution_report.join_customers_with_attribution_paths(
        self._customers, path_transform, transformed_tuple_to_path,
        channel_to_encoding, self._report_window_start,
        self._report_window_end, self._channels)
    column_headers = [schema_field.name for schema_field in
                      fractribution_report.get_report_schema(self._channels)]
    rows = [dict(zip(column_headers, row)) for row in rows]

    # Load the expected output.
    expected_customer_id_to_row = self._extract_expected_output(
        expected_output_csv)

    # Check for equality.
    self.assertEqual(len(rows), len(expected_customer_id_to_row))
    for row in rows:
      customer_id = row['customer_id']
      self.assertIn(customer_id, expected_customer_id_to_row)
      self._check_row_equality(row, expected_customer_id_to_row[customer_id])

  def test_unique_fractribution(self):
    self.fractribution_test('unique', 'tests/expected_unique_fit.csv')

  def test_exposure_fractribution(self):
    self.fractribution_test('exposure', 'tests/expected_exposure_fit.csv')

  def test_first_fractribution(self):
    self.fractribution_test('first', 'tests/expected_first_fit.csv')

  def test_frequency_fractribution(self):
    self.fractribution_test('frequency', 'tests/expected_frequency_fit.csv')


if __name__ == '__main__':
  unittest.main()
