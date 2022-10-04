/*
 * Copyright (c) 2021 IBA Group, a.s. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package by.iba.vf.spark.transformation.plan

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.config.ProcessConfiguration
import by.iba.vf.spark.transformation.exception.InvalidStageInputException
import by.iba.vf.spark.transformation.exception.NodeNotFoundException
import by.iba.vf.spark.transformation.exception.UnknownStageException
import by.iba.vf.spark.transformation.stage.Stage
import by.iba.vf.spark.transformation.stage.StageBuilder
import by.iba.vf.spark.transformation.stage.function.{CacheStageBuilder, ChangeDataCaptureStageBuilder, FilterStageBuilder, GroupByStageBuilder, JoinStageBuilder, RemoveDuplicatesStageBuilder, SliceStageBuilder, SortStageBuilder, TransformStageBuilder, UnionStageBuilder}
import by.iba.vf.spark.transformation.stage.read.{CassandraReadStageBuilder, DataGeneratorStageBuilder, ElasticReadStageBuilder, JdbcReadStageBuilder, MongoReadStageBuilder, ObjectStorageReadCOSStageBuilder, ObjectStorageReadS3StageBuilder, RedisReadStageBuilder, RedshiftReadStageBuilder}
import by.iba.vf.spark.transformation.stage.write.{CassandraWriteStageBuilder, ElasticWriteStageBuilder, JdbcWriteStageBuilder, MongoWriteStageBuilder, ObjectStorageWriteCOSStageBuilder, ObjectStorageWriteS3StageBuilder, RedisWriteStageBuilder, RedshiftWriteStageBuilder, StdoutWriteStageBuilder}

object ExecutionPlanBuilder {

  private val StageBuilders: Seq[StageBuilder] =
    Seq(
      DataGeneratorStageBuilder,
      CacheStageBuilder,
      RedisReadStageBuilder,
      RedisWriteStageBuilder,
      ElasticReadStageBuilder,
      ElasticWriteStageBuilder,
      JdbcReadStageBuilder,
      JdbcWriteStageBuilder,
      MongoReadStageBuilder,
      MongoWriteStageBuilder,
      ObjectStorageReadCOSStageBuilder,
      ObjectStorageReadS3StageBuilder,
      ObjectStorageWriteCOSStageBuilder,
      ObjectStorageWriteS3StageBuilder,
      CassandraWriteStageBuilder,
      CassandraReadStageBuilder,
      RedshiftReadStageBuilder,
      RedshiftWriteStageBuilder,
      StdoutWriteStageBuilder,
      FilterStageBuilder,
      TransformStageBuilder,
      GroupByStageBuilder,
      JoinStageBuilder,
      UnionStageBuilder,
      ChangeDataCaptureStageBuilder,
      RemoveDuplicatesStageBuilder,
      SortStageBuilder,
      SliceStageBuilder
    )

  def buildExecutionPlan(pc: ProcessConfiguration): ExecutionPlan = {
    val dependencyMap = buildDependencyMap(pc)
    val nodeMapper: Node => Stage = mapNodeToStage(dependencyMap)
    val plan =
      buildRawExecutionPlan(dependencyMap)
        .map(_.map(id => nodeMapper(pc.nodes.find(_.id == id).getOrElse(throw NodeNotFoundException(id)))))

    ExecutionPlan(plan, dependencyMap)
  }

  /**
   * Builds dependency map of nodes
   * e.g. if you have chart like `stage1 -> stage2 -> stage3` then you'll have Map(stage2 -> Seq(stage1), stage3 -> Seq(stage2))
   *
   * @param pc process configuration (nodes/edges)
   * @return map of node dependencies (id to ids)
   */
  private def buildDependencyMap(pc: ProcessConfiguration): Map[String, Seq[String]] =
    pc.nodes
      .map(_.id)
      .map(n => n -> pc.edges.filter(_.target == n).map(_.source))
      .toMap

  private def buildRawExecutionPlan(dependencyMap: Map[String, Seq[String]]): Seq[Set[String]] = {
    def buildLayersFromDeps(deps: Map[String, Seq[String]], processed: Set[String]): Seq[Set[String]] = {
      if (deps.isEmpty) {
        Nil
      } else {
        val (nextLayerDeps, otherDeps) = deps.partition { case (_, v) => v.forall(processed.contains) }
        val nextLayer = nextLayerDeps.keySet

        Seq(nextLayer) ++ buildLayersFromDeps(otherDeps, processed ++ nextLayer)
      }
    }

    buildLayersFromDeps(dependencyMap, Set.empty)
  }

  private def mapNodeToStage(dependencyMap: Map[String, Seq[String]])(n: Node): Stage = {
    val id = n.id
    val stage =
      StageBuilders
        .flatMap {
          case sb: StageBuilder if sb.validateAndConvert(n).isDefined => sb.validateAndConvert(n)
          case _ => None
        }
        .headOption
        .getOrElse(throw UnknownStageException(id))

    val depsSize = dependencyMap.get(id).map(_.size).getOrElse(default = 0)

    if (depsSize != stage.inputsRequired) {
      throw InvalidStageInputException(id, stage.inputsRequired, depsSize)
    }

    stage
  }

}
