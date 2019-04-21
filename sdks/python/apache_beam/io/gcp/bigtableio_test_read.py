# from __future__ import absolute_import
# import argparse
# import datetime
# import time
#
# import apache_beam as beam
# from apache_beam import DoFn
# from apache_beam import pvalue
# from apache_beam.metrics import Metrics
# from apache_beam.transforms import core
# from apache_beam.transforms.util import Reshuffle
# from apache_beam.options.pipeline_options import PipelineOptions
# from apache_beam.options.pipeline_options import SetupOptions
# from apache_beam.testing.util import assert_that
#
#
# from beam_bigtable import BigtableSource
#
# STAGING_LOCATION = 'gs://mf2199/stage'
# TEMP_LOCATION = 'gs://mf2199/temp'
#
#
# class ReadFromBigTable(beam.PTransform):
#   def __init__(self, project_id, instance_id, table_id):
#     super(self.__class__, self).__init__()
#     self.beam_options = {'project_id': project_id,
#                          'instance_id': instance_id,
#                          'table_id': table_id}
#
#   def expand(self, pvalue):
#     return (pvalue
#             | 'ReadFromBigtable' >> beam.io.Read(BigtableSource(project_id=self.beam_options['project_id'],
#                                                                 instance_id=self.beam_options['instance_id'],
#                                                                 table_id=self.beam_options['table_id'])))
#
#
# class BigtableRead(beam.PTransform):
#   def __init__(self, project_id, instance_id, table_id):
#     super(self.__class__, self).__init__()
#     self.beam_options = {'project_id': project_id,
#                          'instance_id': instance_id,
#                          'table_id': table_id}
#
#   def expand(self, pbegin):
#     from apache_beam.options.pipeline_options import DebugOptions
#
#     assert isinstance(pbegin, pvalue.PBegin)
#     self.pipeline = pbegin.pipeline
#
#     debug_options = self.pipeline._options.view_as(DebugOptions)
#     if debug_options.experiments and 'beam_fn_api' in debug_options.experiments:
#       source = BigtableSource(project_id=self.beam_options['project_id'],
#                               instance_id=self.beam_options['instance_id'],
#                               table_id=self.beam_options['table_id'])
#
#       def split_source(unused_impulse):
#         return source.split()
#
#       return (
#           pbegin
#           | 'Impulse' >> core.Impulse()
#           | 'Split' >> core.FlatMap(split_source)
#           # | 'Split' >> beam.ParDo(split_source())
#           | 'Reshuffle' >> Reshuffle()
#           | 'Read' >> core.FlatMap(lambda bundle: bundle.source.read(
#                 bundle.source.get_range_tracker(bundle.start_position, bundle.stop_position))))
#           # | 'Read' >> core.FlatMap(lambda split: source.read(source.get_range_tracker(split.start_position, split.stop_position))))
#           # | 'Read' >> beam.ParDo(lambda split: source.read(source.get_range_tracker(split.start_position, split.stop_position))))
#     else:
#       # Treat Read itself as a primitive.
#       return pvalue.PCollection(self.pipeline)
#
#
# def run(argv=[]):
#
#   project_id = 'grass-clump-479'
#   instance_id = 'python-write-2'
#
#   table_id = 'test-2kkk-write-20190417-125057'
#   jobname = 'test-2kkk-read-{}'.format(datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d-%H%M%S'))
#
#   argv.extend([
#     '--experiments=beam_fn_api',
#     '--project={}'.format(project_id),
#     '--instance={}'.format(instance_id),
#     '--table={}'.format(table_id),
#     '--projectId={}'.format(project_id),
#     '--instanceId={}'.format(instance_id),
#     '--tableId={}'.format(table_id),
#     '--job_name={}'.format(jobname),
#     '--requirements_file=bigtableio_test_requirements.txt',
#     '--disk_size_gb=100',
#     '--region=us-central1',
#     '--runner=dataflow',
#     '--autoscaling_algorithm=NONE',
#     '--num_workers=300',
#     '--staging_location={}'.format(STAGING_LOCATION),
#     '--temp_location={}'.format(TEMP_LOCATION),
#     '--setup_file=C:\\git\\beam_bigtable\\beam_bigtable_package\\setup.py',
#     '--extra_package=C:\\git\\beam_bigtable\\beam_bigtable_package\\dist\\beam_bigtable-0.3.117.tar.gz'
#   ])
#   parser = argparse.ArgumentParser(argv)
#   parser.add_argument('--projectId')
#   parser.add_argument('--instanceId')
#   parser.add_argument('--tableId')
#   (known_args, pipeline_args) = parser.parse_known_args(argv)
#
#   print 'ProjectID: ', project_id
#   print 'InstanceID:', instance_id
#   print 'TableID:   ', table_id
#   print 'JobID:     ', jobname
#
#   pipeline_options = PipelineOptions(argv)
#   pipeline_options.view_as(SetupOptions).save_main_session = True
#
#   with beam.Pipeline(options=pipeline_options) as p:
#     second_step = (p
#                    | 'Bigtable Read' >> BigtableRead(project_id=project_id,
#                                                      instance_id=instance_id,
#                                                      table_id=table_id))
#     count = (second_step
#              | 'Count' >> beam.combiners.Count.Globally())
#     row_count = 100000
#     assert_that(count, equal_to([row_count]))
#     result = p.run()
#     result.wait_until_finish()
#
#
# if __name__ == '__main__':
#   run()


from __future__ import absolute_import
import argparse
import datetime
import time
import uuid
import math
from sys import platform

import apache_beam as beam
from apache_beam.io import Read
from apache_beam.pvalue import PBegin
from apache_beam.pvalue import PCollection
from apache_beam.transforms import PTransform
from apache_beam.transforms.combiners import Count
from apache_beam.transforms.core import FlatMap
from apache_beam.transforms.core import Impulse
from apache_beam.transforms.util import Reshuffle
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from google.cloud.bigtable import Client
from google.cloud._helpers import _microseconds_from_datetime
from google.cloud._helpers import UTC

from beam_bigtable import BigtableSource as BigTableSource

GUID = str(uuid.uuid1())
TIME_STAMP = datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d-%H%M%S')
# TABLE_INFO = ('testmillioned113e20', 10)
# TABLE_INFO = ('testmillion2ee87b99', 10000)
# TABLE_INFO = ('testmillion11daf1bf', 24543)
# TABLE_INFO = ('testbothae323947',    10000)
TABLE_INFO = ('testmillion1c1d2c39', 781000)
# TABLE_INFO = ('testmillionc0a4f355', 881000)
# TABLE_INFO = ('testmillion9a0b1127', 6000000)
# TABLE_INFO = ('testmillionb38c02c4', 10000000)
# TABLE_INFO = ('testmillione320108f', 500000000)

EXPERIMENTS = 'beam_fn_api'
PROJECT_ID = 'grass-clump-479'
INSTANCE_ID = 'python-write-2'

TABLE_ID = TABLE_INFO[0]
ROW_COUNT = TABLE_INFO[1]
JOB_NAME = 'read-' + str(ROW_COUNT) + '-' + TABLE_ID + '-' + TIME_STAMP
ROW_COUNT = TABLE_INFO[1]

# TABLE_ID = 'test-2kkk-write-20190417-125057'
# JOB_NAME = 'test-2kkk-read-{}'.format(datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d-%H%M%S'))
# ROW_COUNT = 100000

# TABLE_ID = 'test-200kkk-write-20190417-174735'
# JOB_NAME = 'test-200kkk-write-20190417-174735-{}'.format(datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d-%H%M%S'))
# ROW_COUNT = 1000000

# TABLE_ID = 'test-2000kkk-write-20190417-133838'
# JOB_NAME = 'test-2000kkk-write-20190417-133838-{}'.format(datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d-%H%M%S'))
# ROW_COUNT = 100000000

REQUIREMENTS_FILE = 'bigtableio_test_requirements.txt'
DISK_SIZE_GB = 100
REGION = 'us-central1'
# RUNNER = 'dataflow'
RUNNER = 'direct'
NUM_WORKERS = 300
AUTOSCALING_ALGORITHM = 'NONE'
STAGING_LOCATION = 'gs://mf2199/stage'
TEMP_LOCATION = 'gs://mf2199/temp'
SETUP_FILE = 'C:\\git\\beam-Ark\\beam_bigtable-master\\beam_bigtable_package\\setup.py'
EXTRA_PACKAGE = 'C:\\git\\BBT-temporary\\beam_bigtable_package\\dist\\beam_bigtable-0.3.118.tar.gz'
# EXTRA_PACKAGE='C:\\git\\beam-Ark\\beam_bigtable-master\\beam_bigtable-0.3.116.tar.gz'


class BigtableRead(PTransform):
  def __init__(self, project_id, instance_id, table_id):
    super(self.__class__, self).__init__()
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id}

  def expand(self, pbegin):
    from apache_beam.options.pipeline_options import DebugOptions
    from apache_beam.transforms import util

    assert isinstance(pbegin, PBegin)
    self.pipeline = pbegin.pipeline

    debug_options = self.pipeline._options.view_as(DebugOptions)
    if debug_options.experiments and 'beam_fn_api' in debug_options.experiments:
      source = BigTableSource(project_id=self.beam_options['project_id'],
                              instance_id=self.beam_options['instance_id'],
                              table_id=self.beam_options['table_id'])

      def split_source(unused_impulse):
        return source.split()

      return (
          pbegin
          | 'Core Impulse' >> Impulse()
          | 'Split' >> FlatMap(split_source)
          | 'Reshuffle' >> Reshuffle()
          | 'Read Bundles' >> FlatMap(lambda split: split.source.read(
              split.source.get_range_tracker(start_position=split.start_position,
                                             stop_position=split.stop_position))))
    else:
      # Treat Read itself as a primitive.
      return PCollection(self.pipeline)


def run():
  print 'Project ID: ', PROJECT_ID
  print 'Instance ID:', INSTANCE_ID
  print 'Table ID:   ', TABLE_ID
  print 'Job ID:     ', JOB_NAME

  pipeline_options = PipelineOptions([
    '--experiments={}'.format(EXPERIMENTS),
    '--project={}'.format(PROJECT_ID),
    '--instance={}'.format(INSTANCE_ID),
    '--table={}'.format(TABLE_ID),
    '--job_name={}'.format(JOB_NAME),
    '--requirements_file={}'.format(REQUIREMENTS_FILE),
    '--disk_size_gb={}'.format(DISK_SIZE_GB),
    '--region={}'.format(REGION),
    '--runner={}'.format(RUNNER),
    '--autoscaling_algorithm={}'.format(AUTOSCALING_ALGORITHM),
    '--num_workers={}'.format(NUM_WORKERS),
    '--staging_location={}'.format(STAGING_LOCATION),
    '--temp_location={}'.format(TEMP_LOCATION),
    '--setup_file={}'.format(SETUP_FILE),
    '--extra_package={}'.format(EXTRA_PACKAGE)
  ])
  pipeline_options.view_as(SetupOptions).save_main_session = True

  p = beam.Pipeline(options=pipeline_options)

  source = BigTableSource(project_id=PROJECT_ID,
                          instance_id=INSTANCE_ID,
                          table_id=TABLE_ID)
  count = (p
            # | 'Read from Bigtable' >> BigtableRead(project_id=PROJECT_ID,
            #                                        instance_id=INSTANCE_ID,
            #                                        table_id=TABLE_ID)
            | 'Read from Bigtable' >> Read(source)
            | 'Count Rows' >> Count.Globally()
           )
  assert_that(count, equal_to([ROW_COUNT]))
  p.run()


if __name__ == '__main__':
  run()
