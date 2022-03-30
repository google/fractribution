# coding=utf-8
# Copyright 2022 Google LLC..
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

"""Entry point for running fractribution in Docker container."""

import json
import logging
import os
import time
import googleapiclient.discovery
import main
import requests

import google.cloud.logging

client = google.cloud.logging.Client()
client.get_default_handler()
client.setup_logging()
logging.info(os.environ["fractribution_param"])
param = json.loads(os.environ["fractribution_param"])

logging.info("Start Fractribution")
logging.info(param)
try:
  main.run(param)
  logging.info("Fractribution Done!")

except Exception as e:
  logging.error("An exception occurred")
  logging.exception(e)

logging.info("Shutting down.....")
headers = {"Metadata-Flavor": "Google"}
meta_response = requests.get(
    url="http://metadata.google.internal/computeMetadata/v1/instance/name",
    headers=headers)
instance_name = meta_response.text
meta_response = requests.get(
    url="http://metadata.google.internal/computeMetadata/v1/instance/zone",
    headers=headers)
zone = meta_response.text.split("/")[-1]

meta_response = requests.get(
    url="http://metadata.google.internal/computeMetadata/v1/project/project-id",
    headers=headers)
project = meta_response.text
compute = googleapiclient.discovery.build(
    "compute", "v1", cache_discovery=False)
request = compute.instances().stop(
    project=project, zone=zone, instance=instance_name)
response = request.execute()
logging.info(response)

logging.getLogger().handlers[0].flush()
time.sleep(120)
