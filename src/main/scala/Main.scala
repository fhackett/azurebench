package com.github.fhackett.azurebench

import com.azure.core.http.policy.HttpLogDetailLevel
import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.core.management.{AzureEnvironment, Region}
import com.azure.core.management.profile.AzureProfile
import com.azure.resourcemanager.AzureResourceManager
import com.azure.resourcemanager.compute.models.{ImageReference, PowerState, VirtualMachine, VirtualMachineSizeTypes}
import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.connection.channel.direct.Signal
import net.schmizz.sshj.transport.verification.PromiscuousVerifier
import org.rogach.scallop.{ScallopConf, ScallopOption}
import reactor.core.publisher.{Flux, Mono}

import java.net.ConnectException
import scala.annotation.tailrec
import scala.concurrent.{Await, Future, blocking}
import scala.jdk.CollectionConverters._
import scala.util.Using
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import upickle.default._

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.util.control.NonFatal

object Main {
  final class Config(args: Seq[String]) extends ScallopConf(args) {
    val azureTenantId: ScallopOption[String] = opt[String]()
    val azureSubscription: ScallopOption[String] = opt[String]()

    val parallelClusters: ScallopOption[Int] = opt[Int](default = Some(1))
    val settlingDelay: ScallopOption[Int] = opt[Int](default = Some(2), descr = "amount of time to wait for servers to be up (in seconds)")
    val maxRuntime: ScallopOption[Int] = opt[Int](default = Some(5), descr = "maximum time to let an experiment run (in minutes)")

    val resourceGroupPrefix: ScallopOption[String] = opt[String](default = Some("azbench"))

    val benchmarkFolder: ScallopOption[String] = trailArg[String]()

    verify()
  }

  implicit val vmSizeReader: Reader[VirtualMachineSizeTypes] = reader[String].map(VirtualMachineSizeTypes.fromString)

  final case class ExperimentsData(name: String,
                                   experimentRepetitions: Int,
                                   vmSize: VirtualMachineSizeTypes = VirtualMachineSizeTypes.STANDARD_B4MS,
                                   provisionCmd: String,
                                   experiments: List[ExperimentData]) {
    def experimentInstances: List[ExperimentDataInstance] = {
      val instances = experiments.flatMap(_.instances(this))
      assert(instances.iterator.map(_.fullName).distinct.size == instances.size, "some experiments have the same identifier, which is bad. add keyConfigs to fix")
      instances
    }
  }
  object ExperimentsData {
    implicit val r: Reader[ExperimentsData] = macroR
  }

  final case class ExperimentData(name: String,
                                  clientCmd: String,
                                  serverCmd: String,
                                  keyConfigs: List[String] = Nil,
                                  serverCount: Quantity[Int],
                                  config: Map[String,AnyQuantity] = Map.empty) {
    def instances(experimentsData: ExperimentsData): List[ExperimentDataInstance] =
      serverCount.values
        .flatMap { serverCount =>
          config.foldLeft(Iterator.single(Map.empty[String,String])) { (acc, kv) =>
            acc.flatMap { acc =>
              kv._2.values.map { value =>
                acc.updated(kv._1, value)
              }
            }
          }
          .flatMap { config =>
            (1 to experimentsData.experimentRepetitions)
              .iterator
              .map { repeatIdx =>
                ExperimentDataInstance(
                  name = name,
                  clientCmd = clientCmd,
                  serverCmd = serverCmd,
                  keyConfigs = keyConfigs,
                  repeatIdx = repeatIdx,
                  serverCount = serverCount,
                  config = config)
              }
          }
        }
  }
  object ExperimentData {
    implicit val r: Reader[ExperimentData] = macroR
  }

  final case class ExperimentDataInstance(name: String,
                                          clientCmd: String,
                                          serverCmd: String,
                                          keyConfigs: List[String],
                                          repeatIdx: Int,
                                          serverCount: Int,
                                          config: Map[String,String]) {
    def fullName: String =
      s"$name-$serverCount-$repeatIdx${
        keyConfigs.iterator
          .map(config)
          .map(key => s"-$key")
          .mkString
      }"
  }
  object ExperimentDataInstance {
    implicit val rw: ReadWriter[ExperimentDataInstance] = macroRW
  }

  class ImageFolder(path: os.Path) {
    require(os.isDir(path))
    val root: os.Path = path
  }

  class FolderStructure(root: os.Path) {
    val image = new ImageFolder(root / "image")

    val resultsFolder: os.Path = root / "results"

    val experimentsData: ExperimentsData =
      read[ExperimentsData](os.read.stream(root / "experiments.json"), trace = true)

    val sshKeyPublic: String = os.read(root / "id_rsa.pub")
    val sshKeyPrivate: String = os.read(root / "id_rsa")
  }

  private def ensureStarted(vm: VirtualMachine): Mono[Unit] =
    Mono.defer { () =>
      vm.powerState() match {
        case PowerState.RUNNING => Mono.just(())
        case PowerState.STOPPED => vm.startAsync().map(_ => ()).retry()
        case PowerState.DEALLOCATED => vm.startAsync().map(_ => ()).retry()
        case _ => ???
      }
    }

  private val rootUserName: String = "azbench"

  def main(args: Array[String]): Unit = {
    val config = new Config(args)
    val benchFolder = os.Path(config.benchmarkFolder(), os.pwd)

    val folderStructure = new FolderStructure(benchFolder)

    val resourceManager = {
      val credentialBuilder = new DefaultAzureCredentialBuilder()
      if (config.azureTenantId.isDefined) {
        credentialBuilder.tenantId(config.azureTenantId())
      }

      val resourceManager = AzureResourceManager.configure()
        .withLogLevel(HttpLogDetailLevel.BASIC)
        .authenticate(credentialBuilder.build(), new AzureProfile(AzureEnvironment.AZURE))

      if (config.azureSubscription.isDefined) {
        resourceManager.withSubscription(config.azureSubscription())
      } else {
        resourceManager.withDefaultSubscription()
      }
    }

    val rootName = new Name(config.resourceGroupPrefix())
      .sub(folderStructure.experimentsData.name)

    val groupedExperimentInstances = folderStructure.experimentsData
      .experimentInstances
      .iterator
      .filter { experimentalData =>
        // use results.txt as a marker that the necessary work has been done
        // (and, because we do this pre-group, we will correctly parallelize remaining work)
        val shouldSkip = os.isFile(folderStructure.resultsFolder / experimentalData.fullName / "results.txt")
        if(shouldSkip) {
          println(s"skipping ${experimentalData.fullName}. results.txt already exists")
        }
        !shouldSkip
      }
      .grouped(config.parallelClusters())
      .toList

    val experimentInstancesByParallelism: List[(Seq[ExperimentDataInstance],Int)] =
      (0 until config.parallelClusters()).iterator
        .map { parallelismIdx =>
          (groupedExperimentInstances
            .flatMap(_.lift(parallelismIdx))
            .sortBy(-_.serverCount), // do widest reps first
            parallelismIdx)
        }
        .toList

    val tasks = Future.sequence {
      experimentInstancesByParallelism.map {
        case (experimentDataInstances, parallelismIdx) =>
          Future {
            blocking {
              experimentDataInstances.foreach { experimentData =>
                runExperiment(
                  config = config,
                  resourceManager = resourceManager,
                  rootName = rootName.sub((parallelismIdx + 1).toString),
                  folderStructure = folderStructure,
                  experimentData = experimentData,
                  resultsFolder = folderStructure.resultsFolder / experimentData.fullName)
              }
            }
          }
      }
    }
    Await.result(tasks, Duration.Inf)
  }

  def runExperiment(config: Config, resourceManager: AzureResourceManager, rootName: Name,
                    folderStructure: FolderStructure, experimentData: ExperimentDataInstance,
                    resultsFolder: os.Path): Unit = {
    val region = Region.US_EAST
    val experimentsData = folderStructure.experimentsData

    Using.resource(os.write.over.outputStream(resultsFolder / "config.json", createFolders = true)) { out =>
      writeToOutputStream(experimentData, out)
    }

    println(s"create/find resource group $rootName")

    val resourceGroup = resourceManager.resourceGroups()
      .define(rootName)
      .withRegion(region)
      .createAsync()
      .retry()
      .block()

    val networkName = rootName.sub("network")
    val subnetName = rootName.sub("subnet")
    println(s"create/find network $networkName...")
    val network = resourceManager.networks()
      .define(networkName)
      .withRegion(region)
      .withExistingResourceGroup(resourceGroup)
      .withAddressSpace("10.0.0.0/16")
      .withSubnet(subnetName, "10.0.0.0/24")
      .createAsync()
      .retry()
      .block()

    def mkMachine(vmName: Name): Mono[VirtualMachine] = {
      println(s"create/find machine $vmName...")
      resourceManager.virtualMachines()
        .getByResourceGroupAsync(resourceGroup.name(), vmName)
        .onErrorResume { err =>
          println(s"error looking up $vmName: ${err.getMessage}. abandoning fast path, try to create/update it instead")
          for {
            publicIP <- resourceManager.publicIpAddresses()
              .define(vmName.sub("public-ip"))
              .withRegion(region)
              .withExistingResourceGroup(resourceGroup)
              .withDynamicIP()
              .createAsync()
            networkInterface <- resourceManager.networkInterfaces()
              .define(vmName.sub("net"))
              .withRegion(region)
              .withExistingResourceGroup(resourceGroup)
              .withExistingPrimaryNetwork(network)
              .withSubnet(subnetName)
              .withPrimaryPrivateIPAddressDynamic()
              .withExistingPrimaryPublicIPAddress(publicIP)
              .createAsync()
            virtualMachine <- resourceManager.virtualMachines()
              .define(vmName)
              .withRegion(region)
              .withExistingResourceGroup(resourceGroup)
              .withExistingPrimaryNetworkInterface(networkInterface)
              .withSpecificLinuxImageVersion(
                new ImageReference()
                  .withPublisher("Canonical")
                  .withOffer("0001-com-ubuntu-server-focal")
                  .withSku("20_04-lts-gen2")
                  .withVersion("20.04.202205100"))
              .withRootUsername(rootUserName)
              .withSsh(folderStructure.sshKeyPublic)
              .withComputerName(vmName)
              .withSize(folderStructure.experimentsData.vmSize)
              .createAsync()
          } yield virtualMachine
        }
    }

    val machineRoot = rootName.sub("vm")

    val (clientVM, serverVMs) = locally {
      val resultPair =
        Mono.zip(
          mkMachine(machineRoot.sub("client")),
          Flux.mergeSequential(
            (1 to experimentData.serverCount).view
              .map(idx => machineRoot.sub(idx.toString))
              .map(mkMachine)
              .map(_.retry())
              .asJava: java.lang.Iterable[Mono[VirtualMachine]]).collectList())
          .block()
      (resultPair.getT1, resultPair.getT2.asScala.toList)
    }

    // for use later, see the finally branch and the "run experiment" section
    val serverClosersAndReaders = mutable.ListBuffer.empty[(() => Unit, Future[Unit])]
    try {
      println("ensuring vms are started...")
      Flux.merge(ensureStarted(clientVM), Flux.merge(serverVMs.view.map(ensureStarted).asJava))
        .blockLast()
      println("...vms are now started")
      println(s"machines started:${
        (Iterator.single(clientVM) ++ serverVMs.iterator)
          .map(vm => s"\n - ${vm.name()} (public IP: ${vm.getPrimaryPublicIPAddress.ipAddress()})")
          .mkString
      }")

      // provisioning block:
      locally {
        println("provisioning...")
        val provisioningTasks = (clientVM :: serverVMs).map { serverVM =>
          Future {
            blocking {
              provisionVM(
                name = serverVM.name(),
                image = folderStructure.image,
                resultsFolder = resultsFolder,
                provisionCmd = experimentsData.provisionCmd,
                publicKey = folderStructure.sshKeyPublic,
                privateKey = folderStructure.sshKeyPrivate,
                publicIP = serverVM.getPrimaryPublicIPAddress.ipAddress())
            }
          }
        }

        Await.result(Future.sequence(provisioningTasks), Duration.Inf)
        println("...done provisioning")
      }

      // run the experiment
      locally {
        val serverIps = serverVMs
          .iterator
          .map(_.getPrimaryNetworkInterface.primaryPrivateIP())
          .mkString(",")
        val configKVs = experimentData.config
          .iterator
          .map {
            case key -> value => s"AZ_CONF_${key.toUpperCase}=$value"
          }
          .mkString(" ")

        // run servers (accumulating stoppers/closers in serverClosersAndReaders)
        serverVMs.iterator.zipWithIndex.foreach {
          case (serverVM, serverIdx) =>
            serverClosersAndReaders += locally {
              val publicIP = serverVM.getPrimaryPublicIPAddress.ipAddress()
              println(s"starting server on ${serverVM.name()} ($publicIP) via SSH...")
              val sshClient = connectSSH(privateKey = folderStructure.sshKeyPrivate, publicKey = folderStructure.sshKeyPublic, host = publicIP)
              val session = sshClient.startSession()
              val cmd = session.exec(s"export AZ_SERVER_IPS=$serverIps AZ_SERVER_IDX=$serverIdx $configKVs && cd image && ${experimentData.serverCmd} 2>&1")
              val reader: Future[Unit] = Future {
                blocking {
                  Using.resource(os.write.over.outputStream(resultsFolder / s"run-${serverVM.name()}.txt", createFolders = true)) { out =>
                    ignoreNonFatalExceptions(cmd.getInputStream.transferTo(out))
                  }
                }
              }
              ({ () =>
                ignoreNonFatalExceptions {
                  try {
                    cmd.signal(Signal.TERM)
                    cmd.join()
                  } finally {
                    try {
                      cmd.close()
                    } finally {
                      try {
                        session.close()
                      } finally {
                        sshClient.close()
                      }
                    }
                  }
                }
              }, reader)
            }
        }

        // run client
        val clientIP = clientVM.getPrimaryPublicIPAddress.ipAddress()
        println(s"starting client on ${clientVM.name()} ($clientIP) via SSH...")
        withConnectedSSH(privateKey = folderStructure.sshKeyPrivate, publicKey = folderStructure.sshKeyPublic, host = clientIP) { sshClient =>
          println(s"waiting ${config.settlingDelay()} seconds for things to settle")
          Thread.sleep(config.settlingDelay() * 1000)

          Using.resource(sshClient.startSession()) { session =>
            Using.resource(session.exec(s"export AZ_SERVER_IPS=$serverIps AZ_CLIENT_IP=${
              clientVM.getPrimaryNetworkInterface.primaryPrivateIP()
            } $configKVs && cd image && ${experimentData.clientCmd} 2>&1")) { cmd =>
              val readerFuture: Future[Unit] = Future {
                blocking {
                  Using.resource(os.write.over.outputStream(resultsFolder / "client-progress.txt", createFolders = true)) { out =>
                    val buffer = new Array[Byte](4096)
                    ignoreNonFatalExceptions {
                      // tee client progress to file and console
                      Iterator.continually(cmd.getInputStream.read(buffer))
                        .takeWhile(_ != -1)
                        .foreach { readCount =>
                          Console.out.synchronized {
                            // assuming nothing else writes to the console, this should generate... presentable (?) output
                            Console.out.write(buffer, 0, readCount)
                          }
                          out.write(buffer, 0, readCount)
                        }
                    }
                  }
                }
              }
              serverClosersAndReaders += ((() => (), readerFuture))

              cmd.join(config.maxRuntime(), TimeUnit.MINUTES)
              cmd.close()
              if(cmd.getExitStatus == 0) {
                os.move(from = resultsFolder / "client-progress.txt", to = resultsFolder / "results.txt", replaceExisting = true)
                println(s"...client ${clientVM.name()} ($clientIP) finished successfully")
              } else {
                println(s"!!!client ${clientVM.name()} ($clientIP) did not finish successfully; check client-progress.txt to see what happened")
              }
            }
          }
        }
      }
    } finally {
      println("closing all SSH sessions...")
      Await.result(Future.sequence(serverClosersAndReaders.map(pair => Future(blocking(pair._1())))), Duration.Inf)
      println("waiting for all server SSH connections to drop...")
      Await.result(Future.sequence(serverClosersAndReaders.map(_._2)), Duration.Inf)
      println("...all server SSH connections dropped")
    }
  }

  private def ignoreNonFatalExceptions(body: =>Unit): Unit = {
    try {
      body
    } catch {
      case NonFatal(err) =>
        println(s"ignoring nonfatal exception ${err.getMessage}")
    }
  }

  private def connectSSH(privateKey: String, publicKey: String, host: String): SSHClient = {
    val sshClient = new SSHClient()
    sshClient.addHostKeyVerifier(new PromiscuousVerifier)
    sshClient.useCompression()

    connectWithRetry(sshClient = sshClient, host = host)

    val keyProvider = sshClient.loadKeys(privateKey, publicKey, null)
    sshClient.authPublickey(rootUserName, keyProvider)
    sshClient
  }

  private def withConnectedSSH[T](privateKey: String, publicKey: String, host: String)(fn: SSHClient => T): T =
    Using.resource(connectSSH(privateKey = privateKey, publicKey = publicKey, host = host))(fn)

  @tailrec
  private def connectWithRetry(sshClient: SSHClient, host: String): Unit = {
    try {
      sshClient.connect(host)
    } catch {
      case err: ConnectException =>
        println(s"exception while connecting to $host: ${err.getMessage}. retrying in hopes that it's transient")
        Thread.sleep(200)
        connectWithRetry(sshClient, host)
    }
  }

  private def provisionVM(name: String, image: ImageFolder, resultsFolder: os.Path, provisionCmd: String, publicKey: String, privateKey: String, publicIP: String): Unit = {
    println(s"provisioning $name ($publicIP) via SSH...")
    withConnectedSSH(privateKey = privateKey, publicKey = publicKey, host = publicIP) { sshClient =>
      // copy over / update all files via SFTP
      locally {
        println(s"sending files to $name ($publicIP) via SFTP...")
        val imageRoot: os.Path = image.root

        val mtimes = mutable.HashMap.empty[os.RelPath, Long]
        os.walk.stream(imageRoot, preOrder = false, includeTarget = true).foreach { path =>
          val relpath = path.relativeTo(imageRoot)
          Iterator.iterate(relpath)(_ / os.up)
            .takeWhile(_.ups == 0)
            .foreach { relpath =>
              mtimes(relpath) = os.stat(path).mtime.to(TimeUnit.SECONDS) max mtimes.getOrElse(relpath, Long.MinValue)
            }
        }

        implicit final class PathHelpers(path: os.Path) {
          def asRemote: String = s"image/${path.relativeTo(imageRoot).toString()}"
          def mtime: Long = mtimes(path.relativeTo(imageRoot))
        }

        Using.resource(sshClient.newSFTPClient()) { sftpClient =>
          def impl(path: os.Path): Future[Unit] =
            Future {
              val localInfo = blocking(os.stat(path))
              val shouldSkip = blocking {
                sftpClient.statExistence(path.asRemote) match {
                  case null => false
                  case remoteStat =>
                    val shouldSkip = remoteStat.getMtime >= path.mtime &&
                      remoteStat.getSize == localInfo.size

                    shouldSkip
                }
              }
              (localInfo, shouldSkip)
            }.flatMap {
              case (localInfo, shouldSkip) =>
                (path, localInfo) match {
                  case _ if shouldSkip =>
                    Future {
                      //println(s"at $name ($publicIP) skipping unmodified ${path.asRemote}")
                    }
                  case (file, info) if info.isFile =>
                    Future {
                      blocking {
                        println(s"at $name ($publicIP) updating file ${file.asRemote}")
                        sftpClient.put(file.toString(), file.asRemote)
                        sftpClient.chmod(file.asRemote, os.perms(file).value)
                      }
                    }
                  case (dir, info) if info.isDir =>
                    (if(sftpClient.statExistence(dir.asRemote) == null) {
                      Future {
                        println(s"at $name ($publicIP) mkdir ${dir.asRemote}")
                        blocking(sftpClient.mkdir(dir.asRemote))
                      }
                    } else Future.unit).flatMap { _ =>
                      Future.sequence(os.list.stream(dir).map(impl).toList).map(_ => ())
                    }
                  case (other, _) =>
                    Future {
                      println(s"ignoring ${other.relativeTo(imageRoot)}; neither a file nor a directory")
                    }
                }
            }

          Await.result(impl(imageRoot), Duration.Inf)
        }
        println(s"...sent files to $name ($publicIP)")
      }

      Using.resource(sshClient.startSession()) { session =>
        Using.resource(session.exec(s"cd image && $provisionCmd 2>&1")) { cmd =>
          val cmdOutput: Future[Unit] = Future {
            blocking {
              Using.resource(os.write.over.outputStream(resultsFolder / s"provision-$name.txt", createFolders = true)) { out =>
                cmd.getInputStream.transferTo(out)
              }
            }
          }
          cmd.join()
          cmd.close()
          assert(cmd.getExitStatus == 0, s"!!!check $name ($publicIP). something went wrong")
          println(s"...provisioned $name ($publicIP) via SSH")

          Await.result(cmdOutput, Duration.Inf)
        }
      }
    }
  }
}
