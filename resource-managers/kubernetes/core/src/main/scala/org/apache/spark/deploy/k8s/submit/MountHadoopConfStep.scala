/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.deploy.k8s.submit

import io.fabric8.kubernetes.api.model.{Container, ContainerBuilder}

import org.apache.spark.deploy.k8s.constants._

/**
 * Bootstraps a container with hadoop conf mounted.
 */
private[spark] trait MountHadoopConfStep {
  /**
   * Mount hadoop conf into the given container.
   *
   * @param container the container into which volumes are being mounted.
   * @return the updated container with hadoop conf volumes mounted.
   */
  def mountHadoopConf(container: Container): Container
}

private[spark] class MountHadoopConfStepImpl extends MountHadoopConfStep {
  def mountHadoopConf(container: Container): Container = {
    new ContainerBuilder(container)
      .addNewVolumeMount()
        .withName(HADOOP_FILE_VOLUME)
        .withMountPath(HADOOP_CONF_DIR_PATH)
      .endVolumeMount()
      .addNewEnv()
        .withName(ENV_HADOOP_CONF_DIR)
        .withValue(HADOOP_CONF_DIR_PATH)
      .endEnv()
      .build()
  }
}

