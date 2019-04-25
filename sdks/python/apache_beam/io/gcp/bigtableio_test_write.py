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

import datetime
import time
import random
import string

import apache_beam as beam
from apache_beam import DoFn
from apache_beam import GroupByKey
from apache_beam import ParDo
from apache_beam import Pipeline
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from google.cloud._helpers import _microseconds_from_datetime
from google.cloud._helpers import UTC
from google.cloud.bigtable import enums
from google.cloud.bigtable import row
from google.cloud.bigtable import Client
from google.cloud.bigtable import column_family

from beam_bigtable import WriteToBigTable

PROJECT_ID = 'grass-clump-479'
INSTANCE_ID = 'python-write-2'
CLUSTER_ID = 'python-write-2-c1'
# LOCATION_ID = "us-east1-b"
LOCATION_ID = "us-central1-a"
EXISTING_INSTANCES = []
LABEL_KEY = u'python-bigtable-beam'
LABEL_STAMP = _microseconds_from_datetime(datetime.datetime.utcnow().replace(tzinfo=UTC))
LABELS = {LABEL_KEY: str(LABEL_STAMP)}

TIME_STAMP = datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d-%H%M%S')
# TABLE_ID = 'test-200kkk-write-' + TIME_STAMP
# TABLE_ID = 'sample-table-10k-{}'.format(TIME_STAMP)
TABLE_ID = 'sample-table-10k-2'
JOB_NAME = TABLE_ID

DISK_SIZE_GB = 50
NUM_WORKERS = 300
REGION = 'us-central1'
# RUNNER = 'dataflow'
RUNNER = 'direct'
REQUIREMENTS_FILE = 'C:\\git\\beam_bigtable\\beam_bigtable_package\\requirements.txt'
SETUP_FILE = 'C:\\git\\beam_bigtable\\beam_bigtable_package\\setup.py'
EXTRA_PACKAGE = 'C:\\git\\beam_bigtable\\beam_bigtable_package\\dist\\beam_bigtable-0.3.117.tar.gz'
STAGING_LOCATION = 'gs://mf2199/stage'
TEMP_LOCATION = 'gs://mf2199/temp'
EXPERIMENTS = 'beam_fn_api'
AUTOSCALING_ALGORITHM = 'NONE'

PIPELINE_PARAMETERS = [
		'--experiments={}'.format(EXPERIMENTS),
		'--project={}'.format(PROJECT_ID),
		# '--instance={}'.format(INSTANCE_ID),
		'--job_name={}'.format(JOB_NAME),
		'--requirements_file={}'.format(REQUIREMENTS_FILE),
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

COLUMN_COUNT = 20
# ROW_COUNT = 285714286
ROW_COUNT = 10000
BUNDLE_SIZE = 7000
CELL_SIZE = 1000
ROW_STEP = BUNDLE_SIZE if ROW_COUNT > BUNDLE_SIZE else ROW_COUNT

COLUMN_FAMILY_ID = 'cf1'
MAX_VERSIONS_RULE = column_family.MaxVersionsGCRule(2)
COLUMN_FAMILIES = {COLUMN_FAMILY_ID: MAX_VERSIONS_RULE}


class DummyRowMaker(DoFn):
	def __init__(self):
		super(self.__class__, self).__init__()
		self.row_gen_count = Metrics.counter(self.__class__, 'Rows generated')

	def __setstate__(self, options):
		self.row_gen_count = Metrics.counter(self.__class__, 'Rows generated')

	def process(self, row_range, *args, **kwargs):
		for row_index in range(int(row_range[0]), int(row_range[1][0])):
			rand = random.choice(string.ascii_letters + string.digits)
			dummy_row = row.DirectRow(row_key='key_{0:012}'.format(row_index))
			_ = [dummy_row.set_cell(column_family_id=COLUMN_FAMILY_ID,
									column=('Column%02d' % i).encode('utf-8'),
									value=''.join(rand for _ in range(CELL_SIZE)),
									timestamp=datetime.datetime.now())
				 for i in range(COLUMN_COUNT)]
			self.row_gen_count.inc()
			yield dummy_row

	def to_runner_api_parameter(self, unused_context):
		pass


def run():
	print 'Project ID: ', PROJECT_ID
	print 'Instance ID:', INSTANCE_ID
	print 'Table ID:   ', TABLE_ID
	print 'Job ID:     ', JOB_NAME

	Client(project=PROJECT_ID, admin=True) \
		.instance(instance_id=INSTANCE_ID, instance_type=enums.Instance.Type.PRODUCTION, labels=LABELS)\
		.table(TABLE_ID)\
		.create(column_families=COLUMN_FAMILIES)

	dummy_ranges = [(str(i), str(min(ROW_COUNT, i + ROW_STEP))) for i in xrange(0, ROW_COUNT, ROW_STEP)]

	pipeline_options = PipelineOptions(PIPELINE_PARAMETERS)
	pipeline_options.view_as(SetupOptions).save_main_session = True
	p = Pipeline(options=pipeline_options)
	count = (p
					 | 'Generate Dummy Ranges' >> beam.Create(dummy_ranges)
					 | 'GroupByKey' >> GroupByKey()
					 | 'Generate Dummy Rows' >> ParDo(DummyRowMaker())
					 | 'Write' >> WriteToBigTable(project_id=PROJECT_ID,
																				instance_id=INSTANCE_ID,
																				table_id=TABLE_ID)
					 )
	p.run()


if __name__ == '__main__':
	run()
