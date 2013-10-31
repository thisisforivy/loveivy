/*
 * Copyright (C) 2013 The Regents of Mingxi Wu
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edb.enviroment

import edb.engine._

import scala.collection.mutable.{HashMap, HashSet}

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.StatsReportListener
import org.apache.spark.{Logging}
import org.apache.spark.serializer.{KryoSerializer => SparkKryoSerializer}

/** A singleton object for the master program. The slaves should not access this. */
object EdbEnv extends Logging {

  @transient  var sc: SparkContext = _

  def init(): SparkContext = {
    sc = new SparkContext(
      "spark://127.0.0.1:7077",
      "EDB::" + java.net.InetAddress.getLocalHost.getHostName,
      "/server/spark",
      List("/server/edb/core/target/scala-2.9.3/core_2.9.3-0.0.1.jar"),
      executorEnvVars)
    sc
  }

  logInfo("Initializing EdbEnv")

  System.setProperty("spark.KryoSerializer", 
    classOf[SparkKryoSerializer].getName)


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




