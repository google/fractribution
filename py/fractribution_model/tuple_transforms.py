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


"""Functions for applying transformations to path tuples.

unique: Identity transform
  E.g. (D, A, B, B, C, D, C, C) --> (D, A, B, B, C, D, C, C).
exposure: Collapse sequential repeats
  E.g. (D, A, B, B, C, D, C, C) --> (D, A, B, C, D, C).
first: Removes repeated events.
  E.g. (D, A, B, B, C, D, C, C) --> (D, A, B, C).
frequency: Removes repeat events but tracks them with a count.
  E.g. (D, A, B, B, C, D, C, C) --> (D(2), A(1), B(2), C(3)).
"""

import re
from typing import Tuple


def unique_transform(t: Tuple[str]) -> Tuple[str]:
  """Returns a tuple with elements of t after applying the Unique transform.

  The Unique transform is just the identity transform.
  E.g. (D, A, B, B, C, D, C, C) --> (D, A, B, B, C, D, C, C).

  Args:
    t: tuple
  Returns:
    A tuple containing elements of t after applying the Unique transform.
  """
  return tuple(t)


def exposure_transform(t: Tuple[str]) -> Tuple[str]:
  """Returns a tuple with elements of t after applying the Exposure transform.

  The Exposure transform collapses sequential duplicates into one.
  E.g. (D, A, B, B, C, D, C, C) --> (D, A, B, C, D, C).

  Args:
    t: tuple
  Returns:
    A tuple containing elements of t after applying the Exposure transform.
  """
  return tuple((el for i, el in enumerate(t) if i == 0 or el != t[i-1]))


def first_transform(t: Tuple[str]) -> Tuple[str]:
  """Returns a tuple with elements of t after applying the First transform.

  The First transform removes any repeat events.
  E.g. (D, A, B, B, C, D, C, C) --> (D, A, B, C).

  Args:
    t: tuple
  Returns:
    A tuple containing elements of t after applying the First transform.
  """
  element_set = set([])
  elements = []
  for element in t:
    if element not in element_set:
      element_set.add(element)
      elements.append(element)
  return tuple(elements)


def frequency_transform(t: Tuple[str]) -> Tuple[str]:
  """Returns a tuple with elements of t after applying the Fequency transform.

  The Frequency transform removes repeat events but tracks them with a count.
  E.g. (D, A, B, B, C, D, C, C) --> (D(2), A(1), B(2), C(3)).

  Args:
    t: tuple
  Returns:
    A tuple containing elements of t after applying the Frequency transform.
  Raises:
    ValueError if an event name in the tuple contains a '(' or ')'.
  """
  event_to_count = {}
  for event in t:
    if re.search(r'\(|\)', event):
      raise ValueError("Frequency transform forbids event names with '(' or ')'"
                       ". See event: %s" % event)
    event_to_count[event] = event_to_count.get(event, 0) + 1
  collapsed = []
  for event in t:
    count = event_to_count[event]
    if count > 0:
      collapsed.append(event + '(%i)' % count)
      # Reset count to 0, since the output has exactly one copy of each event.
      event_to_count[event] = 0
  return tuple(collapsed)


transform_name_to_function = {
    'unique': unique_transform,
    'exposure': exposure_transform,
    'first': first_transform,
    'frequency': frequency_transform,
}
