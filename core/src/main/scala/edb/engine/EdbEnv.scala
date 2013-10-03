package edb.engine

import scala.collection.mutable.{HashMap, HashSet}
import spark.SparkContext
import spark.scheduler.StatsReportListener
import spark.{Logging}

/** A singleton object for the master program. The slaves should not access this. */
object EdbEnv extends Logging {

@transient  var sc: SparkContext = _

  def init(): SparkContext = {
    if (sc == null) {
      sc = new SparkContext(
        if (System.getenv("MASTER") == null) "local" 
          else System.getenv("MASTER"),
          "EDB::" + java.net.InetAddress.getLocalHost.getHostName,
          System.getenv("SPARK_HOME"),
        List("/server/edb/core/target/scala-2.9.3/engine.jar"),
          executorEnvVars)
        sc.addSparkListener(new StatsReportListener())
      }
      sc
    }

    logInfo("Initializing EdbEnv")

    System.setProperty("spark.serializer", 
      classOf[spark.KryoSerializer].getName)

    val executorEnvVars = new HashMap[String, String]
    executorEnvVars.put("SCALA_HOME", getEnv("SCALA_HOME"))
  executorEnvVars.put("SPARK_MEM", getEnv("SPARK_MEM"))
executorEnvVars.put("SPARK_CLASSPATH", getEnv("SPARK_CLASSPATH"))
    executorEnvVars.put("HADOOP_HOME", getEnv("HADOOP_HOME"))
  executorEnvVars.put("JAVA_HOME", getEnv("JAVA_HOME"))
executorEnvVars.put("MESOS_NATIVE_LIBRARY", 
  getEnv("MESOS_NATIVE_LIBRARY"))
executorEnvVars.put("TACHYON_MASTER", getEnv("TACHYON_MASTER"))
    executorEnvVars.put("TACHYON_WAREHOUSE_PATH", 
      getEnv("TACHYON_WAREHOUSE_PATH"))

    // Keeps track of added JARs and files so that we don't add them twice in consecutive queries.
    val addedFiles = HashSet[String]()
    val addedJars = HashSet[String]()

    /** Cleans up and shuts down the EDB environments. */
    def stop() {
      logInfo("Shutting down EDB Environment")
      // Stop the SparkContext
      if (EdbEnv.sc != null) {
        sc.stop()
        sc = null
      }
    }

    /** Return the value of an environmental variable as a string. */
    def getEnv(varname: String) = if (System.getenv(varname) == null) "" else System.getenv(varname)

  }




