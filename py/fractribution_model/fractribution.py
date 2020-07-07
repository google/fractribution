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


"""Library for computing fractional attribution."""

import itertools
import string
from typing import Callable, Dict, Generator, List, Mapping, Tuple
from google.cloud import bigquery
 import tuple_path


def _get_counterfactual_marginal_contributions(
    path: tuple_path.Path,
    transformed_tuple_to_path: Mapping[Tuple[str, ...], tuple_path.Path]
    ) -> List[float]:
  """Returns the marginal contribution of each event in the path.

  Args:
    path: tuple_path.Path for computing counterfactual marginal contributions.
    transformed_tuple_to_path: Dict from all path tuples to tuple_path.Paths.

  Returns:
    List of marginal contribution values, one for each event in the input path.
  Raises:
    ValueError if the path has no event.
  """
  if not path.get_num_events():
    raise ValueError('Attempted to comput counterfactual marginal '
                     'contributions on an empty path.')
  marginal_contributions = [0] * path.get_num_events()
  # If the path contains a single event, then it has 100% of the contribution.
  if path.get_num_events() == 1:
    marginal_contributions[0] = path.get_conversion_probability()
  # Otherwise, for each event, compute the counterfactual marginal contribution.
  else:
    for i in range(path.get_num_events()):
      counterfactual_tuple = path.path_tuple[:i] + path.path_tuple[i+1:]
      counterfactual_conversion_prob = 0
      if counterfactual_tuple in transformed_tuple_to_path:
        counterfactual_conversion_prob = transformed_tuple_to_path[
            counterfactual_tuple].get_conversion_probability()
      marginal_contribution = (
          path.get_conversion_probability() - counterfactual_conversion_prob)
      # Floor at 0 to avoid negative contributions.
      marginal_contribution = max(0, marginal_contribution)
      marginal_contributions[i] = marginal_contribution
  return marginal_contributions


def normalize_event_names(
    transformed_tuple_to_path: Mapping[Tuple[str, ...], tuple_path.Path],
    path_transform_name: str) -> None:
  """Normalizes event names and aggregates attribution values if necessary.

  Side-effect: Updates event names in transformed_tuple_to_path.

  Args:
    transformed_tuple_to_path: Dict from all path tuples to tuple_path.Paths.
    path_transform_name: Name of the path transform used to create the Paths.
  """
  if path_transform_name in ('frequency', 'recency'):
    for path in transformed_tuple_to_path.values():
      event_to_attribution = {}
      for event in path.event_to_attribution:
        normalized_event = event
        if '(' in event:
          normalized_event = event[:event.find('(')]
        event_to_attribution[normalized_event] = (
            event_to_attribution.get(normalized_event, 0) +
            path.event_to_attribution[event])
      path.event_to_attribution = event_to_attribution


def compute_fractional_values(
    transformed_tuple_to_path: Mapping[Tuple[str, ...], tuple_path.Path],
    normalize: bool = True) -> None:
  """Compute fractional attribution values for all given paths.

  Side-effect: Each Path.event_to_attribution from transformed_tuple_to_path
      is updated with the fractional attribution values.

  Args:
    transformed_tuple_to_path: Dict from all path tuples to tuple_path.Paths.
    normalize: Set to True to normalize marginal contribution values and False
        otherwise. Default: True
  """
  for path in transformed_tuple_to_path.values():
    if not path.total_conversions:
      continue
    path.event_to_attribution = {}
    marginal_contributions = _get_counterfactual_marginal_contributions(
        path, transformed_tuple_to_path)
    sum_marginal_contributions = sum(marginal_contributions)
    if normalize and sum_marginal_contributions:
      marginal_contributions = [
          marginal_contribution / sum_marginal_contributions
          for marginal_contribution in marginal_contributions]
    # If all events have 0 marginal_contribution, use last touch attribution.
    if sum_marginal_contributions == 0:
      marginal_contributions[-1] = 1
    for i, event in enumerate(path.path_tuple):
      marginal_contribution = marginal_contributions[i]
      path.event_to_attribution[event] = (
          path.event_to_attribution.get(event, 0) + marginal_contribution)


def last_touch_attribution(
    transformed_tuple_to_path: Mapping[Tuple[str, ...], tuple_path.Path]
    ) -> None:
  """Assigns 100% attribution to the last event in each path.

  Side-effect: Each Path.event_to_attribution from transformed_tuple_to_path
      is updated with the last touch attribution values.

  Args:
    transformed_tuple_to_path: Dict from all path tuples to tuple_path.Paths.
  """
  for path in transformed_tuple_to_path.values():
    path.event_to_attribution = {}
    for event in path.path_tuple:
      path.event_to_attribution[event] = 0
    path.event_to_attribution[path.path_tuple[-1]] = 1


def _create_channel_mapping_iterator() -> Generator[str, None, None]:
  """Creates a Generator over an infinite sequence of unique strings.

  Yields:
    Generator over an infinite sequence of unique strings.
  """
  for size in itertools.count(1):
    for s in itertools.product(string.ascii_lowercase, repeat=size):
      yield ''.join(s)


def transform_paths(
    query_job: bigquery.job.QueryJob,
    path_transform: Callable[[Tuple[str, ...]], Tuple[str, ...]]
    ) -> Tuple[Dict[Tuple[str, ...], tuple_path.Path], Mapping[str, str]]:
  """Construct tuple_path.Paths from paths in the query_job.

  Args:
    query_job: QueryJob with rows of (
        path_string, customer_id, endpoint_datetime).
    path_transform: Function for transforming a path string into a path tuple.

  Returns:
    transformed_tuple_to_path: Dict from transformed tuple to tuple_path.Path
    channel_to_encoding: Dict from channel name to encoded channel name.
  """
  channel_to_encoding = {}
  channel_mapping_iterator = _create_channel_mapping_iterator()
  transformed_tuple_to_path = {}
  for (path_str, num_conversions, num_non_conversions) in query_job:
    channels = path_str.split(' > ')
    encoded_channels = []
    for channel in channels:
      if channel not in channel_to_encoding:
        channel_to_encoding[channel] = next(channel_mapping_iterator)
      encoded_channels.append(channel_to_encoding[channel])
    path = tuple_path.Path(
        path_transform(tuple(encoded_channels)),
        num_conversions,
        num_non_conversions)
    if path.path_tuple not in transformed_tuple_to_path:
      transformed_tuple_to_path[path.path_tuple] = path
    else:
      existing_path = transformed_tuple_to_path[path.path_tuple]
      existing_path.total_conversions += path.total_conversions
      existing_path.total_non_conversions += path.total_non_conversions
  return (transformed_tuple_to_path, channel_to_encoding)
