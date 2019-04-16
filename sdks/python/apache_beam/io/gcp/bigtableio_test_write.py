# WriteToBigTable
from __future__ import absolute_import
import argparse
import datetime
import time
import random
import string
import uuid
from sys import platform

import apache_beam as beam
from apache_beam import DoFn
from apache_beam import GroupByKey
from apache_beam import ParDo
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from google.cloud._helpers import _microseconds_from_datetime
from google.cloud._helpers import UTC
from google.cloud.bigtable import row
from google.cloud.bigtable import Client
from google.cloud.bigtable import column_family

from beam_bigtable import BigtableWrite

# from grpc import StatusCode
# from google.api_core.retry import if_exception_type
# from google.api_core.retry import Retry
# from google.cloud.bigtable.instance import Instance
# from google.cloud.bigtable.batcher import MutationsBatcher
# from google.cloud.bigtable.table import Table

PROJECT_ID = 'grass-clump-479'
INSTANCE_ID = 'python-write-2'
CLUSTER_ID = 'python-write-2-c1'
LOCATION_ID = "us-east1-b"
EXISTING_INSTANCES = []
LABEL_KEY = u'python-bigtable-beam'
# LABEL_STAMP = datetime.datetime.utcnow().replace(tzinfo=UTC)
LABEL_STAMP = _microseconds_from_datetime(datetime.datetime.utcnow().replace(tzinfo=UTC))
LABELS = {LABEL_KEY: str(LABEL_STAMP)}
# DEFAULT_TABLE_PREFIX = "python-test"
# table_id = DEFAULT_TABLE_PREFIX + "-" + str(uuid.uuid4())[:8]

TIME_STAMP = datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d-%H%M%S')
TABLE_ID = 'test-1000k-' + TIME_STAMP
JOB_NAME = 'test-1000k-write-' + TIME_STAMP

REQUIREMENTS_FILE = 'C:\\git\\beam_bigtable\\beam_bigtable_package\\requirements.txt'
SETUP_FILE = 'C:\\git\\beam_bigtable\\beam_bigtable_package\\setup.py'
EXTRA_PACKAGE = 'C:\\git\\beam_bigtable\\beam_bigtable_package\\dist\\beam_bigtable-0.3.117.tar.gz'


class GenerateRow(DoFn):
	def __init__(self):
		super(self.__class__, self).__init__()
		self.generate_row = Metrics.counter(self.__class__, 'generate_row')

	def __setstate__(self, options):
		self.generate_row = Metrics.counter(self.__class__, 'generate_row')

	def process(self, ranges, *args, **kwargs):
		for row_id in range(int(ranges[0]), int(ranges[1][0])):
			key = "beam_key%s" % ('{0:07}'.format(row_id))
			rand = random.choice(string.ascii_letters + string.digits)

			direct_row = row.DirectRow(row_key=key)
			_ = [direct_row.set_cell(
				'cf1',
				('field%s' % i).encode('utf-8'),
				''.join(rand for _ in range(100)),
				datetime.datetime.now()) for i in range(10)]
			self.generate_row.inc()
			yield direct_row

	def to_runner_api_parameter(self, unused_context):
		pass

class CreateAll:

	def __init__(self, project_id, instance_id, table_id):
		from google.cloud.bigtable import enums
		self.project_id = project_id
		self.instance_id = instance_id
		self.table_id = table_id
		self.instance_type = enums.Instance.Type.PRODUCTION
		self.storage_type = enums.StorageType.HDD
		self.client = Client(project=self.project_id, admin=True)

	def create_table(self):
		instance = self.client.instance(self.instance_id, instance_type=self.instance_type, labels=LABELS)
		if not instance.exists():
			cluster = instance.cluster(CLUSTER_ID, LOCATION_ID, default_storage_type=self.storage_type)
			instance.create(clusters=[cluster])
		table = instance.table(self.table_id)

		if not table.exists():
			max_versions_rule = column_family.MaxVersionsGCRule(2)
			column_family_id = 'cf1'
			column_families = {column_family_id: max_versions_rule}
			table.create(column_families=column_families)

class PrintKeys(DoFn):
	def __init__(self):
		super(self.__class__, self).__init__()
		from apache_beam.metrics import Metrics
		self.print_row = Metrics.counter(self.__class__.__name__, 'Print Row')

	def process(self, row_, *args, **kwargs):
		self.print_row.inc()
		return [row_]

	def to_runner_api_parameter(self, unused_context):
		pass

def run(argv=[]):
	# guid = str(uuid.uuid4())[:8]
	# table_id = 'testmillion' + guid
	# jobname = 'testmillion-write-' + guid

	# time_stamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d-%H%M%S')
	# table_id = 'test-1000k-' + time_stamp
	# jobname = 'test-1000k-write-' + time_stamp

	# if platform == "linux" or platform == "linux2" or platform == "darwin":
	#   argument = {
	#     'setup': '--setup_file=/git/beam_bigtable/beam_bigtable_package/setup.py',
	#     'extra_package': '--extra_package=/git/beam_bigtable/beam_bigtable_package/dist/beam_bigtable-0.3.117.tar.gz'
	#   }
	# elif platform == "win32":
	#   argument = {
	#     'setup': '--setup_file=C:\\git\\beam_bigtable\\beam_bigtable_package\\setup.py',
	#     'extra_package': '--extra_package=C:\\git\\beam_bigtable\\beam_bigtable_package\\dist\\beam_bigtable-0.3.117.tar.gz'
	#   }

	argv.extend([
		'--experiments=beam_fn_api',
		'--project={}'.format(PROJECT_ID),
		'--instance={}'.format(INSTANCE_ID),
		'--job_name={}'.format(JOB_NAME),
		'--requirements_file={}'.format(REQUIREMENTS_FILE),
		'--disk_size_gb=50',
		'--region=us-central1',
		'--runner=dataflow',
		'--autoscaling_algorithm=NONE',
		'--num_workers=300',
		'--setup_file=C:\\git\\beam_bigtable\\beam_bigtable_package\\setup.py',
		'--extra_package=C:\\git\\beam_bigtable\\beam_bigtable_package\\dist\\beam_bigtable-0.3.117.tar.gz',
		'--staging_location=gs://mf2199/stage',
		'--temp_location=gs://mf2199/temp',
		# argument['setup'],
		# argument['extra_package'],
	])
	parser = argparse.ArgumentParser(argv)
	(known_args, pipeline_args) = parser.parse_known_args(argv)

	create_table = CreateAll(PROJECT_ID, INSTANCE_ID, TABLE_ID)
	print('ProjectID:', PROJECT_ID)
	print('InstanceID:', INSTANCE_ID)
	print('TableID:', TABLE_ID)
	print('JobID:', JOB_NAME)
	create_table.create_table()

	# row_count = 285714286
	# row_limit = 1000
	# row_step = row_count if row_count <= row_limit else row_count/row_limit
	row_count = 10
	row_step = row_count if row_count <= 10000 else row_count / 10000
	pipeline_options = PipelineOptions(argv)
	pipeline_options.view_as(SetupOptions).save_main_session = True

	p = beam.Pipeline(options=pipeline_options)
	config_data = {'project_id': PROJECT_ID,
				   'instance_id': INSTANCE_ID,
				   'table_id': TABLE_ID}

	count = (p
			 | 'Ranges' >> beam.Create([(str(i), str(i + row_step)) for i in xrange(0, row_count, row_step)])
			 | 'Group' >> GroupByKey()
			 | 'Generate' >> ParDo(GenerateRow())
			 | 'Write' >> BigtableWrite(project_id=PROJECT_ID,
										  instance_id=INSTANCE_ID,
										  table_id=TABLE_ID))
	p.run()


if __name__ == '__main__':
	run()
