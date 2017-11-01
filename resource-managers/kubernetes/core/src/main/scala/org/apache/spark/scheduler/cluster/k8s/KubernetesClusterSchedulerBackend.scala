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

import java.io.Closeable
import java.net.InetAddress
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}
import javax.annotation.concurrent.GuardedBy

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

import org.apache.spark.SparkException
import org.apache.spark.deploy.k8s.config._
import org.apache.spark.deploy.k8s.constants._
import org.apache.spark.rpc.{RpcAddress, RpcEndpointAddress, RpcEnv}
import org.apache.spark.scheduler.{ExecutorExited, SlaveLost, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, SchedulerBackendUtils}
import org.apache.spark.util.Utils

private[spark] class KubernetesClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    rpcEnv: RpcEnv,
    executorPodFactory: ExecutorPodFactory,
    kubernetesClient: KubernetesClient,
    allocatorExecutor: ScheduledExecutorService,
    requestExecutorsService: ExecutorService)
  extends CoarseGrainedSchedulerBackend(scheduler, rpcEnv) {

  import KubernetesClusterSchedulerBackend._

  private val EXECUTOR_ID_COUNTER = new AtomicLong(0L)
  private val RUNNING_EXECUTOR_PODS_LOCK = new Object
  // Indexed by executor IDs
  @GuardedBy("RUNNING_EXECUTOR_PODS_LOCK")
  private val runningExecutorsToPods = new mutable.HashMap[String, Pod]
  // Indexed by executor pod names
  @GuardedBy("RUNNING_EXECUTOR_PODS_LOCK")
  private val runningPodsToExecutors = new mutable.HashMap[String, String]
  private val executorPodsByIPs = new ConcurrentHashMap[String, Pod]()
  private val podsWithKnownExitReasons = new ConcurrentHashMap[String, ExecutorExited]()
  private val disconnectedPodsByExecutorIdPendingRemoval = new ConcurrentHashMap[String, Pod]()

  private val kubernetesNamespace = conf.get(KUBERNETES_NAMESPACE)

  private val kubernetesDriverPodName = conf
    .get(KUBERNETES_DRIVER_POD_NAME)
    .getOrElse(throw new SparkException("Must specify the driver pod name"))
  private implicit val requestExecutorContext = ExecutionContext.fromExecutorService(
    requestExecutorsService)

  private val driverPod = kubernetesClient.pods()
    .inNamespace(kubernetesNamespace)
    .withName(kubernetesDriverPodName)
    .get()

  override val minRegisteredRatio =
    if (conf.getOption("spark.scheduler.minRegisteredResourcesRatio").isEmpty) {
      0.8
    } else {
      super.minRegisteredRatio
    }

  private val executorWatchResource = new AtomicReference[Closeable]
  protected val totalExpectedExecutors = new AtomicInteger(0)

  private val driverUrl = RpcEndpointAddress(
    conf.get("spark.driver.host"),
    conf.getInt("spark.driver.port", DEFAULT_DRIVER_PORT),
    CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString

  private val initialExecutors = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)

  private val podAllocationInterval = conf.get(KUBERNETES_ALLOCATION_BATCH_DELAY)
  require(podAllocationInterval > 0, s"Allocation batch delay " +
    s"${KUBERNETES_ALLOCATION_BATCH_DELAY} " +
    s"is ${podAllocationInterval}, should be a positive integer")

  private val podAllocationSize = conf.get(KUBERNETES_ALLOCATION_BATCH_SIZE)
  require(podAllocationSize > 0, s"Allocation batch size " +
    s"${KUBERNETES_ALLOCATION_BATCH_SIZE} " +
    s"is ${podAllocationSize}, should be a positive integer")

  private val allocatorRunnable = new Runnable {

    // Maintains a map of executor id to count of checks performed to learn the loss reason
    // for an executor.
    private val executorReasonCheckAttemptCounts = new mutable.HashMap[String, Int]

    override def run(): Unit = {
      handleDisconnectedExecutors()
      val executorsToAllocate = mutable.Map[String, Pod]()
      val currentTotalRegisteredExecutors = totalRegisteredExecutors.get
      val currentTotalExpectedExecutors = totalExpectedExecutors.get
      val currentNodeToLocalTaskCount = getNodesWithLocalTaskCounts
      if (currentTotalRegisteredExecutors < runningExecutorsToPods.size) {
        logDebug("Waiting for pending executors before scaling")
      } else if (currentTotalExpectedExecutors <= runningExecutorsToPods.size) {
        logDebug("Maximum allowed executor limit reached. Not scaling up further.")
      } else {
        val nodeToLocalTaskCount = getNodesWithLocalTaskCounts
        for (i <- 0 until math.min(
          currentTotalExpectedExecutors - runningExecutorsToPods.size, podAllocationSize)) {
          val executorId = EXECUTOR_ID_COUNTER.incrementAndGet().toString
          val executorPod = executorPodFactory.createExecutorPod(
            executorId,
            applicationId(),
            driverUrl,
            conf.getExecutorEnv,
            driverPod,
            nodeToLocalTaskCount)
          executorsToAllocate(executorId) = executorPod
          logInfo(
            s"Requesting a new executor, total executors is now ${runningExecutorsToPods.size}")
        }
      }
      val allocatedExecutors = executorsToAllocate.mapValues { pod =>
        Utils.tryLog {
          kubernetesClient.pods().create(pod)
        }
      }
      RUNNING_EXECUTOR_PODS_LOCK.synchronized {
        allocatedExecutors.map {
          case (executorId, attemptedAllocatedExecutor) =>
            attemptedAllocatedExecutor.map { successfullyAllocatedExecutor =>
              runningExecutorsToPods.put(executorId, successfullyAllocatedExecutor)
              runningPodsToExecutors.put(
                successfullyAllocatedExecutor.getMetadata.getName, executorId)
            }
        }
      }
    }

    def handleDisconnectedExecutors(): Unit = {
      // For each disconnected executor, synchronize with the loss reasons that may have been found
      // by the executor pod watcher. If the loss reason was discovered by the watcher,
      // inform the parent class with removeExecutor.
      disconnectedPodsByExecutorIdPendingRemoval.asScala.foreach {
        case (executorId, executorPod) =>
          val knownExitReason = Option(podsWithKnownExitReasons.remove(
            executorPod.getMetadata.getName))
          knownExitReason.fold {
            removeExecutorOrIncrementLossReasonCheckCount(executorId)
          } { executorExited =>
            logWarning(s"Removing executor $executorId with loss reason " + executorExited.message)
            removeExecutor(executorId, executorExited)
            // We keep around executors that have exit conditions caused by the application. This
            // allows them to be debugged later on. Otherwise, mark them as to be deleted from the
            // the API server.
            if (!executorExited.exitCausedByApp) {
              logInfo(s"Executor $executorId failed because of a framework error.")
              deleteExecutorFromClusterAndDataStructures(executorId)
            } else {
              logInfo(s"Executor $executorId exited because of the application.")
            }
          }
      }
    }

    def removeExecutorOrIncrementLossReasonCheckCount(executorId: String): Unit = {
      val reasonCheckCount = executorReasonCheckAttemptCounts.getOrElse(executorId, 0)
      if (reasonCheckCount >= MAX_EXECUTOR_LOST_REASON_CHECKS) {
        removeExecutor(executorId, SlaveLost("Executor lost for unknown reasons."))
        deleteExecutorFromClusterAndDataStructures(executorId)
      } else {
        executorReasonCheckAttemptCounts.put(executorId, reasonCheckCount + 1)
      }
    }

    def deleteExecutorFromClusterAndDataStructures(executorId: String): Unit = {
      disconnectedPodsByExecutorIdPendingRemoval.remove(executorId)
      executorReasonCheckAttemptCounts -= executorId
      podsWithKnownExitReasons -= executorId
      val maybeExecutorPodToDelete = RUNNING_EXECUTOR_PODS_LOCK.synchronized {
        runningExecutorsToPods.remove(executorId).map { pod =>
          runningPodsToExecutors.remove(pod.getMetadata.getName)
          pod
        }.orElse {
          logWarning(s"Unable to remove pod for unknown executor $executorId")
          None
        }
      }
      maybeExecutorPodToDelete.foreach(pod => kubernetesClient.pods().delete(pod))
    }
  }

  override def sufficientResourcesRegistered(): Boolean = {
    totalRegisteredExecutors.get() >= initialExecutors * minRegisteredRatio
  }

  override def start(): Unit = {
    super.start()
    executorWatchResource.set(
      kubernetesClient
        .pods()
        .withLabel(SPARK_APP_ID_LABEL, applicationId())
        .watch(new ExecutorPodsWatcher()))

    allocatorExecutor.scheduleWithFixedDelay(
      allocatorRunnable, 0L, podAllocationInterval, TimeUnit.SECONDS)

    if (!Utils.isDynamicAllocationEnabled(conf)) {
      doRequestTotalExecutors(initialExecutors)
    }
  }

  override def stop(): Unit = {
    // stop allocation of new resources and caches.
    allocatorExecutor.shutdown()

    // send stop message to executors so they shut down cleanly
    super.stop()

    // then delete the executor pods
    // TODO investigate why Utils.tryLogNonFatalError() doesn't work in this context.
    // When using Utils.tryLogNonFatalError some of the code fails but without any logs or
    // indication as to why.
    Utils.tryLogNonFatalError {
      val executorPodsToDelete = RUNNING_EXECUTOR_PODS_LOCK.synchronized {
        val runningExecutorPodsCopy = Seq(runningExecutorsToPods.values.toSeq: _*)
        runningExecutorsToPods.clear()
        runningPodsToExecutors.clear()
        runningExecutorPodsCopy
      }
      kubernetesClient.pods().delete(executorPodsToDelete: _*)
      executorPodsByIPs.clear()
      val resource = executorWatchResource.getAndSet(null)
      if (resource != null) {
        resource.close()
      }
    }
    Utils.tryLogNonFatalError {
      logInfo("Closing kubernetes client")
      kubernetesClient.close()
    }
  }

  /**
   * @return A map of K8s cluster nodes to the number of tasks that could benefit from data
   *         locality if an executor launches on the cluster node.
   */
  private def getNodesWithLocalTaskCounts() : Map[String, Int] = {
    val nodeToLocalTaskCount = mutable.Map[String, Int]() ++
      synchronized {
        hostToLocalTaskCount
      }
    for (pod <- executorPodsByIPs.values().asScala) {
      // Remove cluster nodes that are running our executors already.
      // TODO: This prefers spreading out executors across nodes. In case users want
      // consolidating executors on fewer nodes, introduce a flag. See the spark.deploy.spreadOut
      // flag that Spark standalone has: https://spark.apache.org/docs/latest/spark-standalone.html
      nodeToLocalTaskCount.remove(pod.getSpec.getNodeName).nonEmpty ||
        nodeToLocalTaskCount.remove(pod.getStatus.getHostIP).nonEmpty ||
        nodeToLocalTaskCount.remove(
          InetAddress.getByName(pod.getStatus.getHostIP).getCanonicalHostName).nonEmpty
    }
    nodeToLocalTaskCount.toMap[String, Int]
  }

  override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = Future[Boolean] {
    totalExpectedExecutors.set(requestedTotal)
    true
  }

  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = Future[Boolean] {
    val podsToDelete = mutable.Buffer[Pod]()
    RUNNING_EXECUTOR_PODS_LOCK.synchronized {
      for (executor <- executorIds) {
        val maybeRemovedExecutor = runningExecutorsToPods.remove(executor)
        maybeRemovedExecutor.foreach { executorPod =>
          disconnectedPodsByExecutorIdPendingRemoval.put(executor, executorPod)
          runningPodsToExecutors.remove(executorPod.getMetadata.getName)
          podsToDelete += executorPod
        }
        if (maybeRemovedExecutor.isEmpty) {
          logWarning(s"Unable to remove pod for unknown executor $executor")
        }
      }
    }
    kubernetesClient.pods().delete(podsToDelete: _*)
    true
  }

  def getExecutorPodByIP(podIP: String): Option[Pod] = {
    val pod = executorPodsByIPs.get(podIP)
    Option(pod)
  }

  private class ExecutorPodsWatcher extends Watcher[Pod] {

    private val DEFAULT_CONTAINER_FAILURE_EXIT_STATUS = -1

    override def eventReceived(action: Action, pod: Pod): Unit = {
      if (action == Action.MODIFIED && pod.getStatus.getPhase == "Running"
          && pod.getMetadata.getDeletionTimestamp == null) {
        val podIP = pod.getStatus.getPodIP
        val clusterNodeName = pod.getSpec.getNodeName
        logInfo(s"Executor pod $pod ready, launched at $clusterNodeName as IP $podIP.")
        executorPodsByIPs.put(podIP, pod)
      } else if ((action == Action.MODIFIED && pod.getMetadata.getDeletionTimestamp != null) ||
        action == Action.DELETED || action == Action.ERROR) {
        val podName = pod.getMetadata.getName
        val podIP = pod.getStatus.getPodIP
        logDebug(s"Executor pod $podName at IP $podIP was at $action.")
        if (podIP != null) {
          executorPodsByIPs.remove(podIP)
        }
        if (action == Action.ERROR) {
          logWarning(s"Received pod $podName exited event. Reason: " + pod.getStatus.getReason)
          handleErroredPod(pod)
        } else if (action == Action.DELETED) {
          logWarning(s"Received delete pod $podName event. Reason: " + pod.getStatus.getReason)
          handleDeletedPod(pod)
        }
      }
    }

    override def onClose(cause: KubernetesClientException): Unit = {
      logDebug("Executor pod watch closed.", cause)
    }

    def getExecutorExitStatus(pod: Pod): Int = {
      val containerStatuses = pod.getStatus.getContainerStatuses
      if (!containerStatuses.isEmpty) {
        // we assume the first container represents the pod status. This assumption may not hold
        // true in the future. Revisit this if side-car containers start running inside executor
        // pods.
        getExecutorExitStatus(containerStatuses.get(0))
      } else DEFAULT_CONTAINER_FAILURE_EXIT_STATUS
    }

    def getExecutorExitStatus(containerStatus: ContainerStatus): Int = {
      Option(containerStatus.getState).map(containerState =>
        Option(containerState.getTerminated).map(containerStateTerminated =>
          containerStateTerminated.getExitCode.intValue()).getOrElse(UNKNOWN_EXIT_CODE)
      ).getOrElse(UNKNOWN_EXIT_CODE)
    }

    def isPodAlreadyReleased(pod: Pod): Boolean = {
      RUNNING_EXECUTOR_PODS_LOCK.synchronized {
        !runningPodsToExecutors.contains(pod.getMetadata.getName)
      }
    }

    def handleErroredPod(pod: Pod): Unit = {
      val containerExitStatus = getExecutorExitStatus(pod)
      // container was probably actively killed by the driver.
      val exitReason = if (isPodAlreadyReleased(pod)) {
        ExecutorExited(containerExitStatus, exitCausedByApp = false,
          s"Container in pod ${pod.getMetadata.getName} exited from explicit termination" +
            " request.")
      } else {
        val containerExitReason = s"Pod ${pod.getMetadata.getName}'s executor container " +
          s"exited with exit status code $containerExitStatus."
        ExecutorExited(containerExitStatus, exitCausedByApp = true, containerExitReason)
      }
      podsWithKnownExitReasons.put(pod.getMetadata.getName, exitReason)
    }

    def handleDeletedPod(pod: Pod): Unit = {
      val exitMessage = if (isPodAlreadyReleased(pod)) {
        s"Container in pod ${pod.getMetadata.getName} exited from explicit termination request."
      } else {
        s"Pod ${pod.getMetadata.getName} deleted or lost."
      }
      val exitReason = ExecutorExited(
        getExecutorExitStatus(pod), exitCausedByApp = false, exitMessage)
      podsWithKnownExitReasons.put(pod.getMetadata.getName, exitReason)
    }
  }

  override def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    new KubernetesDriverEndpoint(rpcEnv, properties)
  }

  private class KubernetesDriverEndpoint(
    rpcEnv: RpcEnv,
    sparkProperties: Seq[(String, String)])
    extends DriverEndpoint(rpcEnv, sparkProperties) {

    override def onDisconnected(rpcAddress: RpcAddress): Unit = {
      addressToExecutorId.get(rpcAddress).foreach { executorId =>
        if (disableExecutor(executorId)) {
          RUNNING_EXECUTOR_PODS_LOCK.synchronized {
            runningExecutorsToPods.get(executorId).foreach { pod =>
              disconnectedPodsByExecutorIdPendingRemoval.put(executorId, pod)
            }
          }
        }
      }
    }
  }
}

private object KubernetesClusterSchedulerBackend {
  private val UNKNOWN_EXIT_CODE = -1
  // Number of times we are allowed check for the loss reason for an executor before we give up
  // and assume the executor failed for good, and attribute it to a framework fault.
  val MAX_EXECUTOR_LOST_REASON_CHECKS = 10
}

