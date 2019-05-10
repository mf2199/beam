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

import apache_beam as beam
from apache_beam import DoFn
from apache_beam.metrics import Metrics
from apache_beam.transforms.display import DisplayDataItem

try:
  from google.cloud import spanner
  from google.cloud.spanner import Client
  from google.cloud.spanner_v1.session import Session
except ImportError:
  Client = None
  Session = None

__all__ = ['WriteToSpanner']


class WriteToSpanner(beam.PTransform):
  """ A transform to write to the Bigtable Table.

  A PTransform that write a list of `DirectRow` into the Bigtable Table

  """
  def __init__(self, project, instance, database, table, columns):
    """ The PTransform to access the Bigtable Write connector
    Args:
      project_id(str): GCP Project of to write the Rows
      instance_id(str): GCP Instance to write the Rows
      table_id(str): GCP Table to write the `DirectRows`
    """
    super(self.__class__, self).__init__()
    self.beam_options = {'project': project,
                         'instance': instance,
                         'database': database,
                         'table': table,
                         'columns': columns}

  def expand(self, pvalue):
    beam_options = self.beam_options
    return (pvalue
            | beam.ParDo(_SpannerWriteFn(beam_options['project'],
                                         beam_options['instance'],
                                         beam_options['database'],
                                         beam_options['table'],
                                         beam_options['columns'])))


class _SpannerWriteFn(DoFn):
  def __init__(self, project, instance, database, table, columns,
				 max_num_mutations=10000, batch_size_bytes=0):

    super(self.__class__, self).__init__()
    self.options = {'project_id': project,
										'instance_id': instance,
										'database_id': database,
										'table_id': table,
										'columns': columns,
										'max_num_mutations': max_num_mutations,
										'batch_size_bytes': batch_size_bytes}
    self.session = Session(Client(project=self.options['project_id'])
													 .instance(self.options['instance_id'])
													 .database(self.options['database_id']))
    self.session.create()
    self.transaction = None
    self.values = None
    self.row_counter = None

  def __getstate__(self):
    return self.options

  def __setstate__(self, options):
    self.options = options
    self.session = Session(Client(project=self.options['project_id'])
                           .instance(self.options['instance_id'])
                           .database(self.options['database_id']))
    self.session.create()
    self.row_counter = Metrics.counter(self.__class__, 'Rows written')

  def start_bundle(self):
    self.transaction = self.session.transaction()
    self.transaction.begin()
    self.values = []

  def _insert(self):
    if len(self.values) > 0:
      self.transaction.insert(table=self.options['table_id'],
                              columns=self.options['columns'],
                              values=self.values)
      self.transaction.commit()
      self.row_counter.inc(len(self.values))
    self.values = []

  def process(self, element):
    if len(self.values) >= self.options['max_num_mutations']:
      self._insert()
      self.transaction = self.session.transaction()
      self.transaction.begin()

    self.values.append(element)

  def finish_bundle(self):
    self._insert()
    self.transaction = None
    self.values = []

  def display_data(self):
    return {
      'projectId': DisplayDataItem(value=self.options['project_id'],
                                   label='Spanner Project Id'),
      'instanceId': DisplayDataItem(value=self.options['instance_id'],
                                    label='Spanner Instance Id'),
      'databaseId': DisplayDataItem(value=self.options['database_id'],
                                    label='Spanner Database Id'),
      'tableId': DisplayDataItem(value=self.options['table_id'],
                                 label='Spanner Table Id'),
    }
