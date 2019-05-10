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
import random
import string
import time
import unittest

import apache_beam as beam
from apache_beam.metrics import Metrics
# from apache_beam.io import Read
# from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
# from apache_beam.runners.runner import PipelineState
# from apache_beam.testing.util import assert_that, equal_to
# from apache_beam.transforms.combiners import Count
from apache_beam.transforms import core

try:
  from google.cloud.spanner import Client
  import spannerio
except ImportError:
  Client = None


EXPERIMENT = 'beam_fn_api'
DISK_SIZE_GB = 50
RUNNER = 'dataflow'
# RUNNER = 'direct'
NUM_WORKERS = 8
AUTOSCALING_ALGORITHM = 'NONE'
# AUTOSCALING_ALGORITHM = 'THROUGHPUT_BASED'

LOG_LEVEL = logging.DEBUG
# LOG_LEVEL = logging.INFO

CELL_SIZE = 100
COLUMN_COUNT = 10
COLUMNS = tuple('column{:05d}'.format(i) for i in range(1, COLUMN_COUNT))
LETTERS_AND_DIGITS = string.ascii_letters + string.digits

ROW_COUNT = 10000
ROW_LIMIT = 100
MUTATIONS_LIMIT = 20000
MAX_ROW_MUTATIONS = MUTATIONS_LIMIT // COLUMN_COUNT

DATE_NOW = datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d')
TIME_NOW = datetime.datetime.fromtimestamp(time.time()).strftime('%H%M%S')

DATABASE = 'test-db-{}k-{}-{}'.format(ROW_COUNT // 1000, DATE_NOW, TIME_NOW)
TABLE = 'test_table_{}_{}'.format(DATE_NOW, TIME_NOW)
JOB_NAME = DATABASE

# TABLE_DDL = ["""CREATE TABLE """ + TABLE + """ (
#   keyId INT64 NOT NULL,
#   Name STRING(1024),
#   Street STRING(1024),
#   City STRING(1024),
#   State STRING(1024),
#   ZIP STRING(1024),
# ) PRIMARY KEY (keyId)"""]
columns = ''
for i in range(1, COLUMN_COUNT):
  columns += 'column{:05d} STRING({}),\n'.format(i, CELL_SIZE)
TABLE_DDL = ["""CREATE TABLE """ + TABLE + """ (
  keyId INT64 NOT NULL,\n""" + columns + """
) PRIMARY KEY (keyId)"""]


class GenerateTestRows(beam.DoFn):
  def __init__(self):
    super(self.__class__, self).__init__()
    self.generate_row = None

  def __setstate__(self, options):
    self.generate_row = Metrics.counter(self.__class__, 'Rows generated')

  def process(self, element):
    value = ''.join(random.choice(LETTERS_AND_DIGITS) for _ in range(CELL_SIZE))
    # for row_id in range(int(element[0]), int(element[1][0])):
    for row_id in range(ROW_COUNT):
      row_key = (row_id,)
      cells = list(row_key)
      for i in range(1, COLUMN_COUNT):
        cells.append(value)
      self.generate_row.inc()
      yield tuple(cells)

@unittest.skipIf(Client is None, 'GCP Spanner dependencies are not installed')
class SpannerIOTest(unittest.TestCase):
  """ Spanner IO Connector Test
  This tests the connector both ways, first writing rows to a new table, then reading them and comparing the counters
  """
  def setUp(self):
    print('Project ID: ', PROJECT_ID)
    print('Instance ID:', INSTANCE_ID)
    print('Database ID:', DATABASE)
    print('Table ID:   ', TABLE_ID)
    print('Runner:     ', RUNNER)
    # TODO: [TBD]

  def test_spanner_io(self):

    instance = Client(project=PROJECT_ID).instance(instance_id=INSTANCE_ID)
    database = instance.database(DATABASE)
    if not database.exists():
      database = instance.database(DATABASE, ddl_statements=TABLE_DDL)
      # database = instance.database(DATABASE)
      operation = database.create()
      operation.result()
      print 'Database "{}" and Table "{}" have been created'.format(DATABASE, TABLE)
    else:
      print 'Table "{}" already exists in database "{}"'.format(TABLE, DATABASE)

    pipeline_options = PipelineOptions(PIPELINE_OPTIONS)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)
    _ = (p
          | 'Impulse' >> core.Impulse()
          | 'Make Test Rows' >> beam.ParDo(GenerateTestRows())
          | 'Write' >> spannerio.WriteToSpanner(project=PROJECT_ID,
                                                instance=INSTANCE_ID,
                                                database=DATABASE,
                                                table=TABLE_ID,
                                                columns=COLUMNS)
         )
    p.run()
    # result = p.run()
    # result.wait_until_finish()

    print TABLE_DDL

    # TODO: [TBD]

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--autoscaling_algorithm', type=str, default='NONE')
  parser.add_argument('--cell_size', type=int, default=100)
  parser.add_argument('--column_count', type=int, default=10)
  parser.add_argument('--disk_size_gb', type=int, default=50)
  parser.add_argument('--experiments', type=str, default='beam_fn_api')
  parser.add_argument('--extra_package', type=str)
  parser.add_argument('--instance', type=str)
  parser.add_argument('--log_level', type=int, default=logging.INFO)
  parser.add_argument('--num_workers', type=int, default=8)
  parser.add_argument('--project', type=str)
  parser.add_argument('--region', type=str, default='us-central1')
  parser.add_argument('--row_count', type=int, default=10000)
  parser.add_argument('--runner', type=str, default='dataflow')
  parser.add_argument('--setup_file', type=str)
  parser.add_argument('--staging_location', type=str)
  parser.add_argument('--table', type=str, default='test-table')
  parser.add_argument('--temp_location', type=str)
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

  PIPELINE_OPTIONS = [
    '--experiments={}'.format(args.experiments),
    '--project={}'.format(PROJECT_ID),
    '--instance={}'.format(INSTANCE_ID),
    '--job_name={}'.format(JOB_NAME),
    '--disk_size_gb={}'.format(args.disk_size_gb),
    '--region={}'.format(args.region),
    '--runner={}'.format(args.runner),
    '--autoscaling_algorithm={}'.format(args.autoscaling_algorithm),
    '--num_workers={}'.format(NUM_WORKERS),
    # '--setup_file={}'.format(args.setup_file),
    # '--extra_package={}'.format(args.extra_package),
    '--staging_location={}'.format(args.staging_location),
    '--temp_location={}'.format(args.temp_location),
  ]

  logging.getLogger().setLevel(args.log_level)

  suite = unittest.TestSuite()
  suite.addTest(SpannerIOTest('test_spanner_io'))
  unittest.TextTestRunner(verbosity=2).run(suite)

  print('This is a non-functional template')
