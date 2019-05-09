#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import absolute_import

import argparse
import datetime
import logging
import string
import time
import unittest

# import apache_beam as beam
# from apache_beam.io import Read
# from apache_beam.metrics.metric import MetricsFilter
# from apache_beam.options.pipeline_options import PipelineOptions
# from apache_beam.runners.runner import PipelineState
# from apache_beam.testing.util import assert_that, equal_to
# from apache_beam.transforms.combiners import Count

try:
  from google.cloud.spanner import Client
  import spannerio
except ImportError:
  Client = None


EXPERIMENT = 'beam_fn_api'
PROJECT = 'grass-clump-479'
INSTANCE = 'test-io'
DISK_SIZE_GB = 50
REGION = 'us-central1'
# RUNNER = 'dataflow'
RUNNER = 'direct'
NUM_WORKERS = 8
LOCATION_STAGE = 'gs://mf2199/stage'
LOCATION_TEMP = 'gs://mf2199/temp'
AUTOSCALING_ALGORITHM = 'NONE'
# AUTOSCALING_ALGORITHM = 'THROUGHPUT_BASED'

# LOG_LEVEL = logging.DEBUG
LOG_LEVEL = logging.INFO

CELL_SIZE = 100
COLUMN_COUNT = 6
COLUMNS = ('keyId', 'Name', 'Street', 'City', 'State', 'ZIP')
LETTERS_AND_DIGITS = string.ascii_letters + string.digits

ROW_COUNT = 100000
ROW_LIMIT = 100
MUTATIONS_LIMIT = 20000
MAX_ROW_MUTATIONS = MUTATIONS_LIMIT // len(COLUMNS)

DATE_NOW = datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d')
TIME_NOW = datetime.datetime.fromtimestamp(time.time()).strftime('%H%M%S')

DATABASE = 'test-db-{}k-{}-{}'.format(ROW_COUNT // 1000, DATE_NOW, TIME_NOW)
TABLE = 'test_table_{}_{}'.format(DATE_NOW, TIME_NOW)
JOB_NAME = DATABASE

TABLE_DDL = ["""CREATE TABLE """ + TABLE + """ (
  keyId INT64 NOT NULL,
  Name STRING(1024),
  Street STRING(1024),
  City STRING(1024),
  State STRING(1024),
  ZIP STRING(1024),
) PRIMARY KEY (keyId)"""]

PIPELINE_OPTIONS = [
    '--experiments={}'.format(EXPERIMENT),
    '--project={}'.format(PROJECT),
    '--instance={}'.format(INSTANCE),
    '--job_name={}'.format(JOB_NAME),
    '--disk_size_gb={}'.format(DISK_SIZE_GB),
    '--region={}'.format(REGION),
    '--runner={}'.format(RUNNER),
    '--autoscaling_algorithm={}'.format(AUTOSCALING_ALGORITHM),
    '--num_workers={}'.format(NUM_WORKERS),
    '--staging_location={}'.format(LOCATION_STAGE),
    '--temp_location={}'.format(LOCATION_TEMP),
]


@unittest.skipIf(Client is None, 'GCP Bigtable dependencies are not installed')
class SpannerIOTest(unittest.TestCase):
  """ Spanner IO Connector Test
  This tests the connector both ways, first writing rows to a new table, then reading them and comparing the counters
  """
  def setUp(self):
    pass

  def test_spanner_io(self):
    pass


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--project', type=str)
  parser.add_argument('--instance', type=str)
  parser.add_argument('--table', type=str, default='test-table')
  parser.add_argument('--region', type=str, default='us-central1')
  parser.add_argument('--staging_location', type=str)
  parser.add_argument('--temp_location', type=str)
  parser.add_argument('--setup_file', type=str)
  parser.add_argument('--extra_package', type=str)
  parser.add_argument('--num_workers', type=int, default=300)
  parser.add_argument('--autoscaling_algorithm', type=str, default='NONE')
  parser.add_argument('--experiments', type=str, default='beam_fn_api')
  parser.add_argument('--runner', type=str, default='dataflow')
  parser.add_argument('--disk_size_gb', type=int, default=50)
  parser.add_argument('--row_count', type=int, default=10000)
  parser.add_argument('--column_count', type=int, default=10)
  parser.add_argument('--cell_size', type=int, default=100)
  parser.add_argument('--log_level', type=int, default=logging.INFO)
  args = parser.parse_args()

  PROJECT_ID = args.project
  INSTANCE_ID = args.instance
  ROW_COUNT = args.row_count
  COLUMN_COUNT = args.column_count
  CELL_SIZE = args.cell_size

  COLUMN_FAMILY_ID = 'cf1'
  TIME_STAMP = datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d-%H%M%S')
  LETTERS_AND_DIGITS = string.ascii_letters + string.digits

  ROW_COUNT_K = ROW_COUNT / 1000
  NUM_WORKERS = min(ROW_COUNT_K, args.num_workers)
  TABLE_ID = '{}-{}k-{}'.format(args.table, ROW_COUNT_K, TIME_STAMP)
  JOB_NAME = 'spannerio-it-test-{}k-{}'.format(ROW_COUNT_K, TIME_STAMP)

  PIPELINE_PARAMETERS = [
    '--experiments={}'.format(args.experiments),
    '--project={}'.format(PROJECT_ID),
    '--job_name={}'.format(JOB_NAME),
    '--disk_size_gb={}'.format(args.disk_size_gb),
    '--region={}'.format(args.region),
    '--runner={}'.format(args.runner),
    '--autoscaling_algorithm={}'.format(args.autoscaling_algorithm),
    '--num_workers={}'.format(NUM_WORKERS),
    '--setup_file={}'.format(args.setup_file),
    '--extra_package={}'.format(args.extra_package),
    '--staging_location={}'.format(args.staging_location),
    '--temp_location={}'.format(args.temp_location),
  ]

  logging.getLogger().setLevel(args.log_level)

  suite = unittest.TestSuite()
  suite.addTest(SpannerIOTest('test_spanner_io'))
  unittest.TextTestRunner(verbosity=2).run(suite)

  print('This is a non-functional template')
