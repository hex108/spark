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
package org.apache.spark.scheduler.cluster.k8s

import java.io.File
import java.util.UUID

import scala.collection.JavaConverters._

import com.google.common.base.Charsets
import com.google.common.io.Files
import io.fabric8.kubernetes.api.model.{Pod, PodBuilder, VolumeBuilder, VolumeMountBuilder}
import io.fabric8.kubernetes.api.model.KeyToPathBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import org.mockito.{AdditionalAnswers, Mock, Mockito, MockitoAnnotations}
import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{HadoopConfBootstrapImpl, HadoopConfSparkUserBootstrapImpl, HadoopUGIUtilImpl, KerberosTokenConfBootstrapImpl, PodWithDetachedInitContainer, SparkPodInitContainerBootstrap}
import org.apache.spark.deploy.k8s.config._
import org.apache.spark.deploy.k8s.constants._
import org.apache.spark.deploy.k8s.submit.{MountSecretsBootstrapImpl, MountSmallFilesBootstrap, MountSmallFilesBootstrapImpl}
import org.apache.spark.util.Utils

class ExecutorPodFactorySuite extends SparkFunSuite with BeforeAndAfter with BeforeAndAfterEach {
  private val driverPodName: String = "driver-pod"
  private val driverPodUid: String = "driver-uid"
  private val driverUrl: String = "driver-url"
  private val executorPrefix: String = "base"
  private val executorImage: String = "executor-image"
  private val driverPod = new PodBuilder()
    .withNewMetadata()
      .withName(driverPodName)
      .withUid(driverPodUid)
      .endMetadata()
    .withNewSpec()
      .withNodeName("some-node")
      .endSpec()
    .withNewStatus()
      .withHostIP("192.168.99.100")
      .endStatus()
    .build()
  private var baseConf: SparkConf = _

  @Mock
  private var nodeAffinityExecutorPodModifier: NodeAffinityExecutorPodModifier = _

  @Mock
  private var executorLocalDirVolumeProvider: ExecutorLocalDirVolumeProvider = _

  @Mock
  private var hadoopUGI: HadoopUGIUtilImpl = _

  before {
    MockitoAnnotations.initMocks(this)
    baseConf = new SparkConf()
      .set(KUBERNETES_DRIVER_POD_NAME, driverPodName)
      .set(KUBERNETES_EXECUTOR_POD_NAME_PREFIX, executorPrefix)
      .set(EXECUTOR_DOCKER_IMAGE, executorImage)
    when(nodeAffinityExecutorPodModifier.addNodeAffinityAnnotationIfUseful(
      any(classOf[Pod]),
      any(classOf[Map[String, Int]]))).thenAnswer(AdditionalAnswers.returnsFirstArg())
    when(executorLocalDirVolumeProvider.getExecutorLocalDirVolumesWithMounts).thenReturn(Seq.empty)
  }
  private var kubernetesClient: KubernetesClient = _

  test("basic executor pod has reasonable defaults") {
    val factory = new ExecutorPodFactoryImpl(
      baseConf,
      nodeAffinityExecutorPodModifier,
      None,
      None,
      None,
      None,
      None,
      None,
      executorLocalDirVolumeProvider,
      None,
      None,
      None)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())

    verify(nodeAffinityExecutorPodModifier, times(1))
      .addNodeAffinityAnnotationIfUseful(any(classOf[Pod]), any(classOf[Map[String, Int]]))

    // The executor pod name and default labels.
    assert(executor.getMetadata.getName === s"$executorPrefix-exec-1")
    assert(executor.getMetadata.getLabels.size() === 3)

    // There is exactly 1 container with no volume mounts and default memory limits.
    // Default memory limit is 1024M + 384M (minimum overhead constant).
    assert(executor.getSpec.getContainers.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getImage === executorImage)
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.isEmpty)
    assert(executor.getSpec.getContainers.get(0).getResources.getLimits.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getResources
      .getLimits.get("memory").getAmount === "1408Mi")

    // The pod has no node selector, volumes.
    assert(executor.getSpec.getNodeSelector.isEmpty)
    assert(executor.getSpec.getVolumes.isEmpty)

    checkEnv(executor, Map())
    checkOwnerReferences(executor, driverPodUid)
  }

  test("executor pod hostnames get truncated to 63 characters") {
    val conf = baseConf.clone()
    conf.set(KUBERNETES_EXECUTOR_POD_NAME_PREFIX,
      "loremipsumdolorsitametvimatelitrefficiendisuscipianturvixlegeresple")

    val factory = new ExecutorPodFactoryImpl(
      conf,
      nodeAffinityExecutorPodModifier,
      None,
      None,
      None,
      None,
      None,
      None,
      executorLocalDirVolumeProvider,
      None,
      None,
      None)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())

    verify(nodeAffinityExecutorPodModifier, times(1))
      .addNodeAffinityAnnotationIfUseful(any(classOf[Pod]), any(classOf[Map[String, Int]]))

    assert(executor.getSpec.getHostname.length === 63)
  }

  test("secrets get mounted") {
    val conf = baseConf.clone()

    val secretsBootstrap = new MountSecretsBootstrapImpl(Map("secret1" -> "/var/secret1"))
    val factory = new ExecutorPodFactoryImpl(
      conf,
      nodeAffinityExecutorPodModifier,
      Some(secretsBootstrap),
      None,
      None,
      None,
      None,
      None,
      executorLocalDirVolumeProvider,
      None,
      None,
      None)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())

    verify(nodeAffinityExecutorPodModifier, times(1))
      .addNodeAffinityAnnotationIfUseful(any(classOf[Pod]), any(classOf[Map[String, Int]]))

    assert(executor.getSpec.getContainers.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.get(0).getName
      === "secret1-volume")
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.get(0)
      .getMountPath === "/var/secret1")

    // check volume mounted.
    assert(executor.getSpec.getVolumes.size() === 1)
    assert(executor.getSpec.getVolumes.get(0).getSecret.getSecretName === "secret1")

    checkOwnerReferences(executor, driverPodUid)
  }

  test("init-container bootstrap step adds an init container") {
    val conf = baseConf.clone()
    val initContainerBootstrap = mock(classOf[SparkPodInitContainerBootstrap])
    when(initContainerBootstrap.bootstrapInitContainerAndVolumes(
      any(classOf[PodWithDetachedInitContainer]))).thenAnswer(AdditionalAnswers.returnsFirstArg())

    val factory = new ExecutorPodFactoryImpl(
      conf,
      nodeAffinityExecutorPodModifier,
      None,
      None,
      Some(initContainerBootstrap),
      None,
      None,
      None,
      executorLocalDirVolumeProvider,
      None,
      None,
      None)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())

    verify(nodeAffinityExecutorPodModifier, times(1))
      .addNodeAffinityAnnotationIfUseful(any(classOf[Pod]), any(classOf[Map[String, Int]]))

    assert(executor.getSpec.getInitContainers.size() === 1)

    checkOwnerReferences(executor, driverPodUid)
  }

  test("init-container with secrets mount bootstrap") {
    val conf = baseConf.clone()
    val initContainerBootstrap = mock(classOf[SparkPodInitContainerBootstrap])
    when(initContainerBootstrap.bootstrapInitContainerAndVolumes(
      any(classOf[PodWithDetachedInitContainer]))).thenAnswer(AdditionalAnswers.returnsFirstArg())
    val secretsBootstrap = new MountSecretsBootstrapImpl(Map("secret1" -> "/var/secret1"))

    val factory = new ExecutorPodFactoryImpl(
      conf,
      nodeAffinityExecutorPodModifier,
      None,
      None,
      Some(initContainerBootstrap),
      Some(secretsBootstrap),
      None,
      None,
      executorLocalDirVolumeProvider,
      None,
      None,
      None)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())

    verify(nodeAffinityExecutorPodModifier, times(1))
      .addNodeAffinityAnnotationIfUseful(any(classOf[Pod]), any(classOf[Map[String, Int]]))

    assert(executor.getSpec.getInitContainers.size() === 1)
    assert(executor.getSpec.getInitContainers.get(0).getVolumeMounts.get(0).getName
      === "secret1-volume")
    assert(executor.getSpec.getInitContainers.get(0).getVolumeMounts.get(0)
      .getMountPath === "/var/secret1")

    checkOwnerReferences(executor, driverPodUid)
  }

  test("The local dir volume provider's returned volumes and volume mounts should be added.") {
    Mockito.reset(executorLocalDirVolumeProvider)
    val localDirVolume = new VolumeBuilder()
        .withName("local-dir")
        .withNewEmptyDir().endEmptyDir()
        .build()
    val localDirVolumeMount = new VolumeMountBuilder()
        .withName("local-dir")
        .withMountPath("/tmp")
        .build()
    when(executorLocalDirVolumeProvider.getExecutorLocalDirVolumesWithMounts)
        .thenReturn(Seq((localDirVolume, localDirVolumeMount)))
    val factory = new ExecutorPodFactoryImpl(
      baseConf,
      nodeAffinityExecutorPodModifier,
      None,
      None,
      None,
      None,
      None,
      None,
      executorLocalDirVolumeProvider,
      None,
      None,
      None)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())
    assert(executor.getSpec.getVolumes.size === 1)
    assert(executor.getSpec.getVolumes.contains(localDirVolume))
    assert(executor.getSpec.getContainers.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.size === 1)
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.contains(localDirVolumeMount))
  }

  test("Small-files add a secret & secret volume mount to the container") {
    val conf = baseConf.clone()
    val smallFiles = new MountSmallFilesBootstrapImpl("secret1", "/var/secret1")
    val factory = new ExecutorPodFactoryImpl(
      conf,
      nodeAffinityExecutorPodModifier,
      None,
      Some(smallFiles),
      None,
      None,
      None,
      None,
      executorLocalDirVolumeProvider,
      None,
      None,
      None)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())

    verify(nodeAffinityExecutorPodModifier, times(1))
      .addNodeAffinityAnnotationIfUseful(any(classOf[Pod]), any(classOf[Map[String, Int]]))

    assert(executor.getSpec.getContainers.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.get(0)
      .getName === "submitted-files")
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.get(0)
      .getMountPath === "/var/secret1")

    assert(executor.getSpec.getVolumes.size() === 1)
    assert(executor.getSpec.getVolumes.get(0).getSecret.getSecretName === "secret1")

    checkOwnerReferences(executor, driverPodUid)
    checkEnv(executor, Map("SPARK_MOUNTED_FILES_FROM_SECRET_DIR" -> "/var/secret1"))
  }

  test("classpath and extra java options get translated into environment variables") {
    val conf = baseConf.clone()
    conf.set(org.apache.spark.internal.config.EXECUTOR_JAVA_OPTIONS, "foo=bar")
    conf.set(org.apache.spark.internal.config.EXECUTOR_CLASS_PATH, "bar=baz")

    val factory = new ExecutorPodFactoryImpl(
      conf,
      nodeAffinityExecutorPodModifier,
      None,
      None,
      None,
      None,
      None,
      None,
      executorLocalDirVolumeProvider,
      None,
      None,
      None)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)]("qux" -> "quux"), driverPod, Map[String, Int]())

    verify(nodeAffinityExecutorPodModifier, times(1))
      .addNodeAffinityAnnotationIfUseful(any(classOf[Pod]), any(classOf[Map[String, Int]]))

    checkEnv(executor,
      Map("SPARK_JAVA_OPT_0" -> "foo=bar",
          "SPARK_EXECUTOR_EXTRA_CLASSPATH" -> "bar=baz",
          "qux" -> "quux"))
    checkOwnerReferences(executor, driverPodUid)
  }

  test("check that hadoop bootstrap mounts files w/o SPARK_USER") {
    when(hadoopUGI.getShortUserName).thenReturn("test-user")
    val conf = baseConf.clone()
    val configName = "hadoop-test"
    val hadoopFile = createTempFile
    val hadoopFiles = Seq(hadoopFile)
    val hadoopBootsrap = new HadoopConfBootstrapImpl(
      hadoopConfConfigMapName = configName,
      hadoopConfigFiles = hadoopFiles,
      hadoopUGI = hadoopUGI)

    val factory = new ExecutorPodFactoryImpl(
      conf,
      nodeAffinityExecutorPodModifier,
      None,
      None,
      None,
      None,
      None,
      None,
      executorLocalDirVolumeProvider,
      Some(hadoopBootsrap),
      None,
      None)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)]("qux" -> "quux"), driverPod, Map[String, Int]())

    checkEnv(executor,
      Map(ENV_HADOOP_CONF_DIR -> HADOOP_CONF_DIR_PATH,
        "qux" -> "quux"))
    checkOwnerReferences(executor, driverPodUid)
    checkConfigMapVolumes(executor,
      HADOOP_FILE_VOLUME,
      configName,
      hadoopFile.toPath.getFileName.toString)
    checkVolumeMounts(executor, HADOOP_FILE_VOLUME, HADOOP_CONF_DIR_PATH)
  }

  test("check that hadoop bootstrap mounts files w/ SPARK_USER") {
    when(hadoopUGI.getShortUserName).thenReturn("test-user")
    val conf = baseConf.clone()
    val configName = "hadoop-test"
    val hadoopFile = createTempFile
    val hadoopFiles = Seq(hadoopFile)
    val hadoopBootstrap = new HadoopConfBootstrapImpl(
      hadoopConfConfigMapName = configName,
      hadoopConfigFiles = hadoopFiles,
      hadoopUGI = hadoopUGI)
    val hadoopUserBootstrap = new HadoopConfSparkUserBootstrapImpl(hadoopUGI)

    val factory = new ExecutorPodFactoryImpl(
      conf,
      nodeAffinityExecutorPodModifier,
      None,
      None,
      None,
      None,
      None,
      None,
      executorLocalDirVolumeProvider,
      Some(hadoopBootstrap),
      None,
      Some(hadoopUserBootstrap))
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)]("qux" -> "quux"), driverPod, Map[String, Int]())

    checkEnv(executor,
      Map(ENV_SPARK_USER -> "test-user",
        ENV_HADOOP_CONF_DIR -> HADOOP_CONF_DIR_PATH,
        "qux" -> "quux"))
    checkOwnerReferences(executor, driverPodUid)
    checkConfigMapVolumes(executor,
      HADOOP_FILE_VOLUME,
      configName,
      hadoopFile.toPath.getFileName.toString)
    checkVolumeMounts(executor, HADOOP_FILE_VOLUME, HADOOP_CONF_DIR_PATH)
  }

  test("check that hadoop and kerberos bootstrap function properly") {
    when(hadoopUGI.getShortUserName).thenReturn("test-user")
    val conf = baseConf.clone()
    val configName = "hadoop-test"
    val hadoopFile = createTempFile
    val hadoopFiles = Seq(hadoopFile)
    val hadoopBootstrap = new HadoopConfBootstrapImpl(
      hadoopConfConfigMapName = configName,
      hadoopConfigFiles = hadoopFiles,
      hadoopUGI = hadoopUGI)
    val secretName = "secret-test"
    val secretItemKey = "item-test"
    val userName = "sparkUser"
    val kerberosBootstrap = new KerberosTokenConfBootstrapImpl(
      secretName = secretName,
      secretItemKey = secretItemKey,
      userName = userName)
    val factory = new ExecutorPodFactoryImpl(
      conf,
      nodeAffinityExecutorPodModifier,
      None,
      None,
      None,
      None,
      None,
      None,
      executorLocalDirVolumeProvider,
      Some(hadoopBootstrap),
      Some(kerberosBootstrap),
      None)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)]("qux" -> "quux"), driverPod, Map[String, Int]())

    checkEnv(executor,
      Map(ENV_SPARK_USER -> userName,
        ENV_HADOOP_CONF_DIR -> HADOOP_CONF_DIR_PATH,
        ENV_HADOOP_TOKEN_FILE_LOCATION ->
          s"$SPARK_APP_HADOOP_CREDENTIALS_BASE_DIR/$secretItemKey",
        "qux" -> "quux"))
    checkOwnerReferences(executor, driverPodUid)
    checkConfigMapVolumes(executor,
      HADOOP_FILE_VOLUME,
      configName,
      hadoopFile.toPath.getFileName.toString)
    checkSecretVolumes(executor, SPARK_APP_HADOOP_SECRET_VOLUME_NAME, secretName)
    checkVolumeMounts(executor, HADOOP_FILE_VOLUME, HADOOP_CONF_DIR_PATH)
    checkVolumeMounts(executor,
      SPARK_APP_HADOOP_SECRET_VOLUME_NAME,
      SPARK_APP_HADOOP_CREDENTIALS_BASE_DIR)
  }

  // There is always exactly one controller reference, and it points to the driver pod.
  private def checkOwnerReferences(executor: Pod, driverPodUid: String): Unit = {
    assert(executor.getMetadata.getOwnerReferences.size() === 1)
    assert(executor.getMetadata.getOwnerReferences.get(0).getUid === driverPodUid)
    assert(executor.getMetadata.getOwnerReferences.get(0).getController === true)
  }

  // Check that the expected environment variables are present.
  private def checkEnv(executor: Pod, additionalEnvVars: Map[String, String]): Unit = {
    val defaultEnvs = Map(
      ENV_EXECUTOR_ID -> "1",
      ENV_DRIVER_URL -> "dummy",
      ENV_EXECUTOR_CORES -> "1",
      ENV_EXECUTOR_MEMORY -> "1g",
      ENV_APPLICATION_ID -> "dummy",
      ENV_MOUNTED_CLASSPATH -> "/var/spark-data/spark-jars/*",
      ENV_EXECUTOR_POD_IP -> null,
      ENV_EXECUTOR_PORT -> "10000") ++ additionalEnvVars

    assert(executor.getSpec.getContainers.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getEnv().size() === defaultEnvs.size)
    val mapEnvs = executor.getSpec.getContainers.get(0).getEnv.asScala.map {
      x => (x.getName, x.getValue)
    }.toMap
    assert(defaultEnvs === mapEnvs)
  }

  private def checkVolumeMounts(executor: Pod, name: String, mountPath: String) : Unit = {
    assert(executor.getSpec.getContainers.size() === 1)
    val volumeMount = executor.getSpec.getContainers
      .get(0).getVolumeMounts.asScala.find(_.getName == name)
    assert(volumeMount.nonEmpty)
    assert(volumeMount.get.getMountPath == mountPath)
  }

  private def checkConfigMapVolumes(executor: Pod,
    volName: String,
    configMapName: String,
    content: String) : Unit = {
    val volume = executor.getSpec.getVolumes.asScala.find(_.getName == volName)
    assert(volume.nonEmpty)
    assert(volume.get.getConfigMap.getName == configMapName)
    assert(volume.get.getConfigMap.getItems.asScala.find(_.getKey == content).get ==
      new KeyToPathBuilder()
        .withKey(content)
        .withPath(content).build() )
  }

  private def checkSecretVolumes(executor: Pod, volName: String, secretName: String) : Unit = {
    val volume = executor.getSpec.getVolumes.asScala.find(_.getName == volName)
    assert(volume.nonEmpty)
    assert(volume.get.getSecret.getSecretName == secretName)
  }

  // Creates temp files for the purpose of testing file mounting
  private def createTempFile: File = {
    val dir = Utils.createTempDir()
    val file = new File(dir, s"${UUID.randomUUID().toString}")
    Files.write(UUID.randomUUID().toString, file, Charsets.UTF_8)
    file
  }
}
