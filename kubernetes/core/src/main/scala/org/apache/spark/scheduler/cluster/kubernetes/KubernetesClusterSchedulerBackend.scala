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
package org.apache.spark.scheduler.cluster.kubernetes

import java.io.{File, FileInputStream}
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.base.Charsets
import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.api.model.extensions.DaemonSet
import org.apache.commons.codec.binary.Base64
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.sasl.SecretKeyHolder
import org.apache.spark.network.shuffle.kubernetes.KubernetesExternalShuffleClient
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

import org.apache.spark.{SecurityManager, SparkContext, SparkException}
import org.apache.spark.deploy.kubernetes.KubernetesClientBuilder
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointAddress, RpcEnv}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RetrieveSparkProps
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.util.Utils

private[spark] class KubernetesClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    val sc: SparkContext)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {

  import KubernetesClusterSchedulerBackend._

  private val EXECUTOR_MODIFICATION_LOCK = new Object
  private val runningExecutorPods = new mutable.HashMap[String, Pod]

  private val kubernetesMaster = conf
      .getOption("spark.kubernetes.master")
      .getOrElse(
        throw new SparkException("Kubernetes master must be specified in kubernetes mode."))

  private val executorDockerImage = conf
      .get("spark.kubernetes.executor.docker.image", s"spark-executor:${sc.version}")

  private val kubernetesNamespace = conf
      .getOption("spark.kubernetes.namespace")
      .getOrElse(
        throw new SparkException("Kubernetes namespace must be specified in kubernetes mode."))

  private val executorPort = conf.get("spark.executor.port", DEFAULT_STATIC_PORT.toString).toInt

  /**
   * Allows for specifying a custom replication controller for the executor runtime. This should
   * only be used if the user really knows what they are doing. Allows for custom behavior on the
   * executors themselves, or for loading extra containers into the executor pods.
   */
  private val executorCustomSpecFile = conf.getOption("spark.kubernetes.executor.custom.spec.file")
  private val executorCustomSpecExecutorContainerName = executorCustomSpecFile.map(_ =>
    conf
      .getOption("spark.kubernetes.executor.custom.spec.container.name")
      .getOrElse(throw new SparkException("When using a custom replication controller spec" +
      " for executors, the name of the container that the executor will run in must be" +
      " specified via spark.kubernetes.executor.custom.spec.container.name")))

  private val blockmanagerPort = conf
      .get("spark.blockmanager.port", DEFAULT_BLOCKMANAGER_PORT.toString)
      .toInt

  private val kubernetesDriverServiceName = conf
      .getOption("spark.kubernetes.driver.service.name")
      .getOrElse(
        throw new SparkException("Must specify the service name the driver is running with"))

  private val executorMemory = conf.getOption("spark.executor.memory").getOrElse("1g")
  private val executorMemoryBytes = Utils.byteStringAsBytes(executorMemory)

  private val memoryOverheadBytes = conf
      .getOption("spark.kubernetes.executor.memoryOverhead")
      .map(overhead => Utils.byteStringAsBytes(overhead))
      .getOrElse(math.max((MEMORY_OVERHEAD_FACTOR * executorMemoryBytes).toInt,
        MEMORY_OVERHEAD_MIN))
  private val executorMemoryWithOverhead = executorMemoryBytes + memoryOverheadBytes

  private val executorCores = conf.getOption("spark.executor.cores").getOrElse("1")

  private implicit val requestExecutorContext = ExecutionContext.fromExecutorService(
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("kubernetes-executor-requests-%d")
              .build))

  private val kubernetesClient = KubernetesClientBuilder
    .buildFromWithinPod(kubernetesMaster, kubernetesNamespace)

  private val externalShuffleServiceEnabled = conf.getBoolean(
    "spark.shuffle.service.enabled", defaultValue = false)

  override val minRegisteredRatio =
    if (conf.getOption("spark.scheduler.minRegisteredResourcesRatio").isEmpty) {
      0.8
    } else {
      super.minRegisteredRatio
    }

  protected var totalExpectedExecutors = new AtomicInteger(0)
  private val maybeShuffleService = if (externalShuffleServiceEnabled) {
      val daemonSetName = conf
        .getOption("spark.kubernetes.shuffle.service.daemonset.name")
        .getOrElse(throw new IllegalArgumentException("When using the shuffle" +
          " service, must specify the shuffle service daemon set name.")
      )
      val daemonSetNamespace = conf
        .getOption("spark.kubernetes.shuffle.service.daemonset.namespace")
        .getOrElse(throw new IllegalArgumentException("When using the shuffle service," +
          " must specify the shuffle service daemon set namespace."))
      Some(ShuffleServiceDaemonSetMetadata(daemonSetName, daemonSetNamespace))
    } else {
      Option.empty[ShuffleServiceDaemonSetMetadata]
    }

  private val driverUrl = RpcEndpointAddress(
    System.getenv(s"${convertToEnvMode(kubernetesDriverServiceName)}_SERVICE_HOST"),
    sc.getConf.get("spark.driver.port").toInt,
    CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString

  private def convertToEnvMode(value: String): String =
    value.toUpperCase.map { c => if (c == '-') '_' else c }

  private val initialExecutors = getInitialTargetExecutorNumber(1)
  private val authEnabled = conf.getBoolean("spark.authenticate", defaultValue = false)
  private val authSecret = if (authEnabled) {
    conf.getOption("spark.authenticate.secret")
      .getOrElse(
        throw new IllegalArgumentException("No secret provided though spark.authenticate is true."))
  } else {
    "unused"
  }

  private val kubernetesSecretName = s"spark-secret-${applicationId()}"

  override def sufficientResourcesRegistered(): Boolean = {
    totalRegisteredExecutors.get() >= initialExecutors * minRegisteredRatio
  }

  override def start(): Unit = {
    super.start()
    setupAuth()
    if (!Utils.isDynamicAllocationEnabled(sc.conf)) {
      doRequestTotalExecutors(initialExecutors)
    }
  }

  private def setupAuth(): Unit = {
    if (authEnabled) {
      val baseSecret = new SecretBuilder()
        .withNewMetadata()
          .withName(kubernetesSecretName)
          .withNamespace(kubernetesNamespace)
          .endMetadata()
        .withData(Map(SHUFFLE_SECRET_NAME ->
          Base64.encodeBase64String(authSecret.getBytes(Charsets.UTF_8))).asJava)
        .build()
      kubernetesClient.secrets().create(baseSecret)
      maybeShuffleService.foreach(service => {
        if (service.daemonSetNamespace != kubernetesNamespace) {
          val shuffleServiceNamespaceSecret = new SecretBuilder(baseSecret)
            .editMetadata()
              .withNamespace(service.daemonSetNamespace)
              .endMetadata()
            .build()
          kubernetesClient
            .secrets()
            .inNamespace(service.daemonSetNamespace)
            .create(shuffleServiceNamespaceSecret)
        }
      })
    }
  }

  private def allocateNewExecutorPod(): (String, Pod) = {
    val executorId = UUID.randomUUID().toString.replaceAll("-", "")
    val name = s"exec$executorId"
    val selectors = Map(SPARK_EXECUTOR_SELECTOR -> executorId,
      SPARK_APP_SELECTOR -> applicationId()).asJava
    val executorMemoryQuantity = new QuantityBuilder(false)
      .withAmount(executorMemoryBytes.toString)
      .build()
    val executorMemoryLimitQuantity = new QuantityBuilder(false)
      .withAmount(executorMemoryWithOverhead.toString)
      .build()
    val requiredEnv = new ArrayBuffer[EnvVar]
    requiredEnv += new EnvVarBuilder()
      .withName("SPARK_EXECUTOR_PORT")
      .withValue(executorPort.toString)
      .build()
    requiredEnv += new EnvVarBuilder()
      .withName("SPARK_DRIVER_URL")
      .withValue(driverUrl)
      .build()
    requiredEnv += new EnvVarBuilder()
      .withName("SPARK_EXECUTOR_CORES")
      .withValue(executorCores)
      .build()
    requiredEnv += new EnvVarBuilder()
      .withName("SPARK_EXECUTOR_MEMORY")
      .withValue(executorMemory)
      .build()
    requiredEnv += new EnvVarBuilder()
      .withName("SPARK_APPLICATION_ID")
      .withValue(applicationId())
      .build()
    requiredEnv += new EnvVarBuilder()
      .withName("SPARK_EXECUTOR_ID")
      .withValue(executorId)
      .build()
    requiredEnv += new EnvVarBuilder()
      .withName("SPARK_AUTH_ENABLED")
      .withValue(authEnabled.toString)
      .build()

    if (authEnabled) {
      requiredEnv += new EnvVarBuilder()
        .withName(SecurityManager.ENV_AUTH_SECRET)
        .withNewValueFrom()
          .withNewSecretKeyRef(SHUFFLE_SECRET_NAME, kubernetesSecretName)
          .endValueFrom()
        .build()
    }

    val shuffleServiceVolume = maybeShuffleService.map(service => {
      val shuffleServiceDaemonSet = getShuffleServiceDaemonSet(service)
      val shuffleDir = shuffleServiceDaemonSet
        .getSpec
        .getTemplate
        .getSpec
        .getVolumes
        .asScala
        .find(_.getName == "shuffles-volume")
        .getOrElse(throw new IllegalStateException("Expected to find a host path" +
          " shuffles-service volume."))
      new VolumeBuilder()
        .withName("shuffles-volume")
        .withNewHostPath().withPath(shuffleDir.getHostPath.getPath).endHostPath()
        .build()
    })

    shuffleServiceVolume.foreach(volume => {
      requiredEnv += new EnvVarBuilder()
        .withName("SPARK_LOCAL_DIRS")
        .withValue(volume.getHostPath.getPath)
        .build()
    })

    val requiredPorts = new ArrayBuffer[ContainerPort]
    requiredPorts += new ContainerPortBuilder()
      .withName(EXECUTOR_PORT_NAME)
      .withContainerPort(executorPort)
      .build()
    requiredPorts += new ContainerPortBuilder()
      .withName(BLOCK_MANAGER_PORT_NAME)
      .withContainerPort(blockmanagerPort)
      .build()
    executorCustomSpecFile match {
      case Some(filePath) =>
        val file = new File(filePath)
        if (!file.exists()) {
          throw new SparkException(s"Custom executor spec file not found at $filePath")
        }
        val providedPodSpec = Utils.tryWithResource(new FileInputStream(file)) { is =>
          kubernetesClient.pods().load(is)
        }
        val resolvedContainers = providedPodSpec.get.getSpec.getContainers.asScala
        var foundExecutorContainer = false
        for (container <- resolvedContainers) {
          if (container.getName == executorCustomSpecExecutorContainerName.get) {
            foundExecutorContainer = true

            val resolvedEnv = new ArrayBuffer[EnvVar]
            resolvedEnv ++= container.getEnv.asScala
            resolvedEnv ++= requiredEnv
            container.setEnv(resolvedEnv.asJava)

            val resolvedPorts = new ArrayBuffer[ContainerPort]
            resolvedPorts ++= container.getPorts.asScala
            resolvedPorts ++= requiredPorts
            container.setPorts(resolvedPorts.asJava)

            val resolvedVolumeMounts = new ArrayBuffer[VolumeMount]
            resolvedVolumeMounts ++= container.getVolumeMounts.asScala
            shuffleServiceVolume.foreach(volume => {
              resolvedVolumeMounts += new VolumeMountBuilder()
                .withMountPath(volume.getHostPath.getPath)
                .withName(volume.getName)
                .build()
            })
          }
        }
        val providedVolumes = providedPodSpec.get.getSpec.getVolumes.asScala
        val resolvedVolumes = shuffleServiceVolume.map(volume => {
          Seq(volume) ++ providedVolumes
        }).getOrElse(providedVolumes)

        if (!foundExecutorContainer) {
          throw new SparkException("Expected container"
            + s" ${executorCustomSpecExecutorContainerName.get}" +
            " to be provided as the executor container in the custom" +
            " executor replication controller, but it was not found in" +
            " the provided spec file.")
        }
        val editedPod = new PodBuilder(providedPodSpec.get())
          .editMetadata()
            .withName(name)
            .addToLabels(selectors)
            .endMetadata()
          .editSpec()
            .withContainers(resolvedContainers.asJava)
            .withVolumes(resolvedVolumes.asJava)
            .endSpec()
          .build()
        (executorId, kubernetesClient.pods().create(editedPod))
      case None =>
        (executorId, kubernetesClient.pods().createNew()
          .withNewMetadata()
            .withName(name)
            .withLabels(selectors)
            .endMetadata()
          .withNewSpec()
            .withVolumes(shuffleServiceVolume.map(Seq(_)).getOrElse(Seq[Volume]()).asJava)
            .addNewContainer()
              .withName(s"exec-${applicationId()}-container")
              .withImage(executorDockerImage)
              .withImagePullPolicy("IfNotPresent")
              .withVolumeMounts(shuffleServiceVolume.map(volume => {
                Seq(new VolumeMountBuilder()
                  .withName(volume.getName)
                  .withMountPath(volume.getHostPath.getPath)
                  .build())
              }).getOrElse(Seq[VolumeMount]()).asJava)
              .withNewResources()
                .addToRequests("memory", executorMemoryQuantity)
                .addToLimits("memory", executorMemoryLimitQuantity)
                .endResources()
              .withEnv(requiredEnv.asJava)
              .withPorts(requiredPorts.asJava)
              .endContainer()
            .endSpec()
          .done())
    }
  }

  override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = Future[Boolean] {
    EXECUTOR_MODIFICATION_LOCK.synchronized {
      if (requestedTotal > totalExpectedExecutors.get) {
        logInfo(s"Requesting ${requestedTotal - totalExpectedExecutors.get}"
          + s" additional executors, expecting total $requestedTotal and currently" +
          s" expected ${totalExpectedExecutors.get}")
        for (i <- 0 until (requestedTotal - totalExpectedExecutors.get)) {
          runningExecutorPods += allocateNewExecutorPod()
        }
      }
      totalExpectedExecutors.set(requestedTotal)
    }
    true
  }

  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = Future[Boolean] {
    EXECUTOR_MODIFICATION_LOCK.synchronized {
      for (executor <- executorIds) {
        runningExecutorPods.remove(executor) match {
          case Some(pod) => kubernetesClient.pods().delete(pod)
          case None => logWarning(s"Unable to remove pod for unknown executor $executor")
        }
      }
    }
    true
  }

  private def getInitialTargetExecutorNumber(defaultNumExecutors: Int = 1): Int = {
    if (Utils.isDynamicAllocationEnabled(conf)) {
      val minNumExecutors = conf.getInt("spark.dynamicAllocation.minExecutors", 0)
      val initialNumExecutors = Utils.getDynamicAllocationInitialExecutors(conf)
      val maxNumExecutors = conf.getInt("spark.dynamicAllocation.maxExecutors", 1)
      require(initialNumExecutors >= minNumExecutors && initialNumExecutors <= maxNumExecutors,
        s"initial executor number $initialNumExecutors must between min executor number " +
          s"$minNumExecutors and max executor number $maxNumExecutors")

      initialNumExecutors
    } else {
      conf.getInt("spark.executor.instances", defaultNumExecutors)
    }
  }

  override def stop(): Unit = {
    // TODO investigate why Utils.tryLogNonFatalError() doesn't work in this context.
    // When using Utils.tryLogNonFatalError some of the code fails but without any logs or
    // indication as to why.
    try {
      runningExecutorPods.values.foreach(kubernetesClient.pods().delete(_))
    } catch {
      case e: Throwable => logError("Uncaught exception while shutting down controllers.", e)
    }
    try {
      kubernetesClient.services().withName(kubernetesDriverServiceName).delete()
    } catch {
      case e: Throwable => logError("Uncaught exception while shutting down driver service.", e)
    }
    if (authEnabled) {
      try {
        kubernetesClient.secrets().withName(kubernetesSecretName).delete()
      } catch {
        case e: Throwable => logError("Uncaught exception while delete app secret.", e)
      }
      maybeShuffleService.foreach(service => {
        try {
          sendApplicationCompleteToShuffleService(service)
        } catch {
          case e: Throwable => logError("Uncaught exception while cleaning up" +
            " shuffle service secret.", e)
        }
      })
    }
    try {
      kubernetesClient.close()
    } catch {
      case e: Throwable => logError("Uncaught exception closing Kubernetes client.", e)
    }
    super.stop()
  }

  private def sendApplicationCompleteToShuffleService(
      service: ShuffleServiceDaemonSetMetadata): Unit = {
    if (service.daemonSetNamespace != kubernetesNamespace) {
      logInfo("Sending application complete message to shuffle service.")
      val serviceDaemonSet = kubernetesClient
        .inNamespace(service.daemonSetNamespace)
        .extensions()
        .daemonSets()
        .withName(service.daemonSetName)
        .get
      val podLabels = serviceDaemonSet.getSpec.getSelector.getMatchLabels
      // An interesting note is that it would be preferable to put the shuffle service pods
      // behind a K8s service and just query the service to pick a pod for us. Unfortunately,
      // services wouldn't work well with SASL authentication, since SASL requires a series
      // of exchanged messages to be sent between the pods, but repeated messages sent to the
      // same service may resolve to different pods and corrupt the handshake process.
      val shuffleServicePods = kubernetesClient
        .inNamespace(service.daemonSetNamespace)
        .pods()
        .withLabels(podLabels)
        .list()
        .getItems
        .asScala
      val securityManager = sc.env.securityManager
      val port = conf.getInt("spark.shuffle.service.port", 7337)
      var success = false
      val shuffleClient = new KubernetesExternalShuffleClient(
          SparkTransportConf.fromSparkConf(conf, "shuffle"),
          securityManager,
          securityManager.isAuthenticationEnabled(),
          securityManager.isSaslEncryptionEnabled())
      try {
        shuffleClient.init(applicationId())
        for (pod <- shuffleServicePods) {
          if (!success) {
            val host = pod.getStatus.getPodIP
            logInfo(s"Sending application complete message to $host")
            try {
              shuffleClient.sendApplicationComplete(host, port)
              success = true
              logInfo("Successfully sent application complete message.")
            } catch {
              case e: Throwable => logError(s"Failed to send application complete to" +
                s" $host:$port. Will try another pod if possible.", e)
            }
          }
        }
      } finally {
        shuffleClient.close()
      }
      if (!success) {
        throw new IllegalStateException("Failed to send application complete" +
          " message to any shuffle service pod.")
      }
    }
  }

  override def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    new KubernetesDriverEndpoint(rpcEnv, properties)
  }

  private class KubernetesDriverEndpoint(override val rpcEnv: RpcEnv,
      sparkProperties: Seq[(String, String)])
    extends DriverEndpoint(rpcEnv, sparkProperties) {

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      new PartialFunction[Any, Unit]() {
        override def isDefinedAt(x: Any): Boolean = {
          x match {
            case RetrieveSparkProps(executorHostname) => true
            case _ => false
          }
        }

        override def apply(v1: Any): Unit = {
          v1 match {
            case RetrieveSparkProps(executorId) =>
              EXECUTOR_MODIFICATION_LOCK.synchronized {
                var resolvedProperties = sparkProperties
                maybeShuffleService.foreach(service => {
                  // Refresh the pod so we get the status, particularly what host it's running on
                  val runningExecutorPod = kubernetesClient
                    .pods()
                    .withName(runningExecutorPods(executorId).getMetadata.getName)
                    .get()
                  val shuffleServiceDaemonSet = kubernetesClient
                    .extensions()
                    .daemonSets()
                    .inNamespace(service.daemonSetNamespace)
                    .withName(service.daemonSetName)
                    .get()
                  val shuffleServiceForPod = kubernetesClient
                    .inNamespace(service.daemonSetNamespace)
                    .pods()
                    .inNamespace(service.daemonSetNamespace)
                    .withLabels(shuffleServiceDaemonSet.getSpec.getSelector.getMatchLabels)
                    .list()
                    .getItems
                    .asScala
                    .filter(_.getStatus.getHostIP == runningExecutorPod.getStatus.getHostIP)
                    .head
                  resolvedProperties = resolvedProperties ++ Seq(
                    ("spark.shuffle.service.host", shuffleServiceForPod.getStatus.getPodIP))
                })
                if (authEnabled) {
                  // Don't pass the secret here, it's been set in the executor environment
                  // already.
                  resolvedProperties = resolvedProperties
                    .filterNot(_._1 == "spark.authenticate.secret")
                    .filterNot(_._1 == "spark.authenticate")
                }
                context.reply(resolvedProperties)
              }
          }
        }
      }.orElse(super.receiveAndReply(context))
    }
  }

  private def getShuffleServiceDaemonSet(serviceMetadata: ShuffleServiceDaemonSetMetadata)
      : DaemonSet = {
    kubernetesClient
      .extensions()
      .daemonSets()
      .inNamespace(serviceMetadata.daemonSetNamespace)
      .withName(serviceMetadata.daemonSetName)
      .get
  }
}

private object KubernetesClusterSchedulerBackend {
  private val SPARK_EXECUTOR_SELECTOR = "spark-exec"
  private val SPARK_APP_SELECTOR = "spark-app"
  private val DEFAULT_STATIC_PORT = 10000
  private val DEFAULT_BLOCKMANAGER_PORT = 7079
  private val BLOCK_MANAGER_PORT_NAME = "blockmanager"
  private val EXECUTOR_PORT_NAME = "executor"
  private val MEMORY_OVERHEAD_FACTOR = 0.10
  private val MEMORY_OVERHEAD_MIN = 384L
  private val SHUFFLE_SECRET_NAME = "spark-shuffle-secret"
}

private case class ShuffleServiceDaemonSetMetadata(
  val daemonSetName: String,
  val daemonSetNamespace: String)