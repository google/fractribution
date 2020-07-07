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

"""A Path is sequence of events that ends in conversion or non-conversion."""

from typing import Mapping, Tuple


class Path(object):
  """A Path is sequence of events that ends in conversion or non-conversion.

  Attributes:
    path_tuple: Tuple of string events.
    total_conversions: Number of customers with this path that converted.
    total_non_conversions: Number of customers with this path that did not
        convert.
    event_to_attribution: Dict from event to attribution value for the event.
  """

  def __init__(self,
               path_tuple: Tuple[str, ...],
               total_conversions: int,
               total_non_conversions: int):
    self.path_tuple = path_tuple
    self.total_conversions = total_conversions
    self.total_non_conversions = total_non_conversions
    self.event_to_attribution = {}

  def get_conversion_probability(self) -> float:
    path_instances = self.total_conversions + self.total_non_conversions
    if not path_instances:
      return 0.0
    return self.total_conversions / path_instances

  def get_path_string(self,
                      encoding_to_event: Mapping[str, str] = None) -> str:
    """Returns a string representation of the path.

    Args:
      encoding_to_event: Optional mapping from encoded event/channel to the
          user-defined event/channel.
    Returns:
      String representation of the path with events/channels separated by " > ".
    """
    if not encoding_to_event:
      return ' > '.join(self.path_tuple)
    events = []
    for encoded_event in self.path_tuple:
      parens_index = encoded_event.find('(')
      if parens_index > 0:
        # Extract the encoded event name before the parens, map it to the
        # decoded event name, and then append everything from the parens
        # onwards.
        events.append((
            encoding_to_event[encoded_event[:parens_index]] +
            encoded_event[parens_index:]))
      else:
        events.append(encoding_to_event[encoded_event])
    return ' > '.join(events)

  def get_num_events(self) -> int:
    return len(self.path_tuple)
