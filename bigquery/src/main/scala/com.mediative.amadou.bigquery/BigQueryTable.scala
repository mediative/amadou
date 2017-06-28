/*
 * Copyright 2017 Mediative
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mediative.amadou
package bigquery

import com.google.api.services.bigquery.model.TableReference

object BigQueryTable {
  type PartitionMapper = DateInterval => DateInterval
  type Reference       = com.google.api.services.bigquery.model.TableReference

  val PartitionByMonth = BigQueryTable.PartitionStrategy(Month.apply)
  val PartitionByDay   = BigQueryTable.PartitionStrategy(Day.apply)

  case class PartitionStrategy(mapper: PartitionMapper) {
    def partitionSuffix(date: DateInterval): String = "$" + mapper(date).format("yyyyMMdd")
  }
}

/**
 * Holds information about how to load data into BigQuery.
 *
 * Example:
 * {{{
 * scala> val date = com.mediative.amadou.Day(2016, 9, 7)
 * scala> val table = BigQueryTable("company_bi", "finance", "billing")
 * scala> table.referenceFor(date)
 * res1: BigQueryTable.Reference = {datasetId=finance, projectId=company_bi, tableId=billing}
 * scala> table.partitionedByDay.referenceFor(date)
 * res1: BigQueryTable.Reference = {datasetId=finance, projectId=company_bi, tableId=billing$20160907}
 * scala> table.partitionedByMonth.referenceFor(date)
 * res1: BigQueryTable.Reference = {datasetId=finance, projectId=company_bi, tableId=billing$20160901}
 * }}}
 *
 * @define 20160907 20160907
 * @define 20160901 20160901
 */
case class BigQueryTable(
    project: String,
    dataset: String,
    table: String,
    partitionBy: Option[BigQueryTable.PartitionStrategy] = None) {

  def partitionedByDay   = copy(partitionBy = Some(BigQueryTable.PartitionByDay))
  def partitionedByMonth = copy(partitionBy = Some(BigQueryTable.PartitionByMonth))

  def reference(): BigQueryTable.Reference =
    new TableReference()
      .setProjectId(project)
      .setDatasetId(dataset)
      .setTableId(table)

  def referenceFor(date: DateInterval): BigQueryTable.Reference =
    reference.setTableId(table + partitionBy.fold("")(_.partitionSuffix(date)))
}
