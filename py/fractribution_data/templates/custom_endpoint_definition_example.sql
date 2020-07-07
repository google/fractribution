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

# You must use the following aliases (if referencing any of these):
#   `hits.eventInfo.eventCategory`: For _eventCategory_ requirements
#   `hits.eventInfo.eventAction`: For _eventAction_ requirements
#   `hits.eventInfo.eventLabel`: For _eventLabel_ requirements
#   `hits.eventInfo.eventValue`: For _eventValue_ requirements
#   `hits.page.pagePath`: For _pathPath_ (destination) requirements
#   `hits.page.hostname`: For _hostname_ requirements
#   `hc.index`: For `hits.customDimensions` _index_ requirements
#   `hc.value`: For `hits.customDimensions` _value_ requirements
#
# Example 1:
hits.eventInfo.eventCategory = "Contact Us"
#
# Example 2: Using Hits and Hc=Hits.customDimensions in customer conversion definition.
# Hits.eventInfo.eventCategory = 'customer_registration'
# AND REGEXP_CONTAINS(Hits.eventInfo.eventAction, r'complete|success')
# AND Hits.page.hostname = 'signup.your-site.com'
# AND Hc.index = 2
# AND Hc.value = 'specific_tag'
