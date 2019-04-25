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

""" Integration test for GCP Bigtable testing."""
from __future__ import absolute_import

import datetime
import time
import logging
import random
import string
import unittest

import apache_beam as beam
from apache_beam.io import Read
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.runner import PipelineState
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.combiners import Count

try:
  from google.cloud.bigtable import enums, row, column_family, Client
  import beam_bigtable as bigtableio
except ImportError:
  Client = None
  bigtableio = None


EXPERIMENTS = 'beam_fn_api'
PROJECT_ID = 'grass-clump-479'
INSTANCE_ID = 'python-write-2'
STORAGE_TYPE = enums.StorageType.HDD
REGION = 'us-central1'
RUNNER = 'dataflow'
# RUNNER = 'direct'       # Use this runner to run Apache Beam locally
LOCATION_ID = "us-central1-a"
SETUP_FILE = 'C:\\git\\beam_bigtable\\beam_bigtable_package\\setup.py'
EXTRA_PACKAGE = 'C:\\git\\beam_bigtable\\beam_bigtable_package\\dist\\bigtableio-0.3.120.tar.gz'
STAGING_LOCATION = 'gs://mf2199/stage'
TEMP_LOCATION = 'gs://mf2199/temp'
AUTOSCALING_ALGORITHM = 'NONE'
DISK_SIZE_GB = 50

ROW_COUNT = 10000
COLUMN_COUNT = 10
CELL_SIZE = 100
COLUMN_FAMILY_ID = 'cf1'

ROW_COUNT_K = ROW_COUNT / 1000
NUM_WORKERS = min(ROW_COUNT_K, 300)
TIME_STAMP = datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d-%H%M%S')
TABLE_ID = 'sample-table-{}k-{}'.format(ROW_COUNT_K, TIME_STAMP)
JOB_NAME = 'bigtableio-it-test-{}k-{}'.format(ROW_COUNT_K, TIME_STAMP)

LETTERS_AND_DIGITS = string.ascii_letters + string.digits

# LOG_LEVEL = logging.DEBUG
LOG_LEVEL = logging.INFO

PIPELINE_PARAMETERS = [
    '--experiments={}'.format(EXPERIMENTS),
    '--project={}'.format(PROJECT_ID),
    '--job_name={}'.format(JOB_NAME),
    '--disk_size_gb={}'.format(DISK_SIZE_GB),
    '--region={}'.format(REGION),
    '--runner={}'.format(RUNNER),
    '--autoscaling_algorithm={}'.format(AUTOSCALING_ALGORITHM),
    '--num_workers={}'.format(NUM_WORKERS),
    '--setup_file={}'.format(SETUP_FILE),
    '--extra_package={}'.format(EXTRA_PACKAGE),
    '--staging_location={}'.format(STAGING_LOCATION),
    '--temp_location={}'.format(TEMP_LOCATION),
]


class GenerateTestRows(beam.PTransform):
  """ A PTransform to generate dummy rows to write to a Bigtable Table.

  A PTransform that generates a list of `DirectRow` and writes it to a Bigtable Table.
  """
  def __init__(self):
    super(self.__class__, self).__init__()
    self.beam_options = {'project_id': PROJECT_ID,
                         'instance_id': INSTANCE_ID,
                         'table_id': TABLE_ID}

  def _generate(self):
    for i in range(ROW_COUNT):
      key = "key_%s" % ('{0:012}'.format(i))
      test_row = row.DirectRow(row_key=key)
      value = ''.join(random.choice(LETTERS_AND_DIGITS) for _ in range(CELL_SIZE))
      for j in range(COLUMN_COUNT):
        test_row.set_cell(column_family_id=COLUMN_FAMILY_ID,
                          column=('field%s' % j).encode('utf-8'),
                          value=value,
                          timestamp=datetime.datetime.now())
      yield test_row

  def expand(self, pvalue):
    return (pvalue
            | beam.Create(self._generate())
            | bigtableio.WriteToBigTable(project_id=self.beam_options['project_id'],
                                         instance_id=self.beam_options['instance_id'],
                                         table_id=self.beam_options['table_id']))


@unittest.skipIf(Client is None, 'GCP Bigtable dependencies are not installed')
class BigtableIOTest(unittest.TestCase):
  """ Bigtable IO Connector Test

  This tests the connector both ways, first writing rows to a new table, then reading them and comparing the counters
  """
  def setUp(self):
    self.result = None
    self.table = Client(project=PROJECT_ID, admin=True)\
                    .instance(instance_id=INSTANCE_ID)\
                    .table(TABLE_ID)

    if not self.table.exists():
      column_families = {COLUMN_FAMILY_ID: column_family.MaxVersionsGCRule(2)}
      self.table.create(column_families=column_families)
      logging.info('Table {} has been created!'.format(TABLE_ID))

  def test_bigtable_io(self):
    print 'Project ID: ', PROJECT_ID
    print 'Instance ID:', INSTANCE_ID
    print 'Table ID:   ', TABLE_ID

    pipeline_options = PipelineOptions(PIPELINE_PARAMETERS)
    p = beam.Pipeline(options=pipeline_options)
    _ = (p | 'Write Test Rows' >> GenerateTestRows())

    self.result = p.run()
    self.result.wait_until_finish()

    assert self.result.state == PipelineState.DONE

    if not hasattr(self.result, 'has_job') or self.result.has_job:
      query_result = self.result.metrics().query(MetricsFilter().with_name('Written Row'))
      if query_result['counters']:
        read_counter = query_result['counters'][0]
        logging.info('Number of Rows written: %d', read_counter.committed)
        assert read_counter.committed == ROW_COUNT

    # logging.info('Write sequence is complete.')

    p = beam.Pipeline(options=pipeline_options)
    count = (p
             | 'Read from Bigtable' >> Read(bigtableio.BigtableSource(project_id=PROJECT_ID,
                                                                      instance_id=INSTANCE_ID,
                                                                      table_id=TABLE_ID))
             | 'Count Rows' >> Count.Globally()
             )
    self.result = p.run()
    self.result.wait_until_finish()
    assert_that(count, equal_to([ROW_COUNT]))


if __name__ == '__main__':
  logging.getLogger().setLevel(LOG_LEVEL)
  unittest.main()
