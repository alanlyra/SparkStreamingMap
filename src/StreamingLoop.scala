import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.PrintWriter
import java.nio.file.FileSystems
import java.nio.file.Path
import java.nio.file.StandardWatchEventKinds.ENTRY_CREATE
import java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY
import java.nio.file.StandardWatchEventKinds.OVERFLOW
import java.nio.file.WatchEvent
import java.util.Calendar
import java.util.UUID

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.io.Source
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import java.nio.file.WatchKey

object StreamingLoop {

  /*
   * Filesystem vars & functions
   */

  val base_dir: String = "/home/jonny/git/SparkStreamingLoop/exec/"
  val data_dir: String = /*base_dir +*/ "/data/"
  val query_dir: String = /*base_dir +*/ "/query/"
  val prov_dir: String = /*base_dir +*/ "/prov/"
  val out_dir: String =  /*base_dir +*/ "/out/"
  
  // Map: (step_dir => (data_in, query_file, data_out))
  val steps_map: Map[File,(String,File,String)] = Map()
 
  def getListOfSubDirectories(dir: String): Array[File] = {
    val f = Option(new File(dir).listFiles)  //.filter(_.isDirectory).map(_.getName)
    // avoiding NullPointerException
    if (f == None)
      return Array.empty[File]
    f.get
  }
  def getInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => null
    }
  }

  def checkFiles(): Unit = {
    
    // Directories
    
    val steps = getListOfSubDirectories(base_dir)
      .filter(s => s.isDirectory())
      .filter(s => getInt(s.getName) != null)
      .sortBy(s => s.getName.toInt)    
      
    if (steps.length <= 0) {
      println ("No steps detected. Exiting...")
      return
    }
    println("Detected " + steps.length + " steps for this workflow: ")
    steps.foreach(s => println(s.getAbsolutePath))
    
    // Queries / Data
    
    println("\r\nScanning query and data files...")
    
   var queryFiles: Map[File,File] = Map()
   
    steps.foreach { s =>
      val files = getListOfSubDirectories(s.getAbsolutePath + query_dir)
      .filter(s => s.isFile)
      .filter(s => s.getName.endsWith(".sql"))
      if (files.length > 0)
        queryFiles += (s -> files.last)
    }
    
    queryFiles.keys.foreach(k => 
      println("Step: " + k.getName + " | Query: " + queryFiles(k).getName)
    )
          
    if (queryFiles.size < steps.length) {
      val missing = steps.filterNot(queryFiles.keys.toList.contains(_))
      println ("Missing query files in:")
      missing.foreach(s => println(s.getAbsolutePath + query_dir))
      return
    }
    
    val hasData = getListOfSubDirectories(steps(0).getAbsolutePath + data_dir)
    .filter(s => s.isFile)
    .length > 0
    if (!hasData) {
      println("Missing initial data in " + steps(0).getAbsolutePath + data_dir)
      return
    }
    else
      println("Data found in " +  steps(0).getAbsolutePath + data_dir)
      
     // Map: (step_dir => (data_in, query_file, data_out))
         
    steps.sliding(1).foreach{ s =>
      steps_map.+(s(0) -> (s(0).getAbsolutePath + data_dir, queryFiles(s(0)), s(1).getAbsolutePath + data_dir))
    }
    steps_map.+(steps.last -> (steps.last.getAbsolutePath + data_dir, queryFiles(steps.last), base_dir + out_dir))
  }
    
  /*
   *  DirectoryWatcher: watch file changes
   */

  class DirectoryWatcher(val path: Path, val query_id: UUID) extends Runnable {

    val watchService = path.getFileSystem().newWatchService()
    path.register(watchService, ENTRY_CREATE, ENTRY_MODIFY, OVERFLOW)

    def EventDetected(event: WatchEvent[_]): Unit = {
      val kind = event.kind
      val event_path = event.context().asInstanceOf[Path]
      if ((kind.equals(ENTRY_CREATE) || kind.equals(ENTRY_MODIFY))
        &&
        !(event_path.getFileName.toString().startsWith(".")) // Discard temporary files (starting with ".")
        ) {
        println("New query: " + event_path)

        val text = "\nDetected Human In The Loop in '" + event_path + "' at the moment: " + Calendar.getInstance().getTime()
        val write = new PrintWriter(new FileOutputStream(new File(prov_dir + "hil.txt"), true))
        write.write(text)
        write.close()

        session.streams.get(query_id).stop()
        watchService.close()
        createNewQuery(event_path.getFileName.toString())

      }
    }

    override def run(): Unit = {
      try {
        //breakable {
        while (true) {
          var watchKey: WatchKey = null
          try { watchKey = watchService.take() } catch { case ie: InterruptedException => println("InterruptedException: " + ie) }
          var events = watchKey.pollEvents().asScala
          events.foreach(e => EventDetected(e))
          try { Thread.sleep(500); } catch { case ie: InterruptedException => println("InterruptedException: " + ie) }
          var reset: Boolean = watchKey.reset()

          if (!reset) {
            watchKey.cancel()
            watchService.close()
            return
          }
          //  }
        }
      } catch {
        case ie: InterruptedException => println("InterruptedException: " + ie)
        case ioe: IOException => println("IOException: " + ioe)
        case e: Exception => println("Exception: " + e)
      }
    }
  }

  /*
   * JSON schema definition
   * W/o schema definition: 20+ minutes lost on schema inference 
   */

  val veiculoType = StructType(
    Array(
      StructField("codigo", StringType),
      StructField("linha", StringType),
      StructField("latitude", DoubleType),
      StructField("longitude", DoubleType),
      StructField("datahora", DoubleType),
      StructField("velocidade", DoubleType),
      StructField("id_migracao", DoubleType),
      StructField("sentido", StringType),
      StructField("trajeto", StringType)))

  val veiculosType = StructType(
    Array(StructField("veiculos", ArrayType(veiculoType))))

  /*
   * Spark vars & functions
   */

  // Map: step_dir -> (streaming_query, query_id)
  var stream_map: Map[File,(StreamingQuery,UUID)] = Map()
    
  var session: SparkSession = null
  var session_name: String = "StreamingLoop"
  var session_number: Int = 0

  def startSession(): Unit = {

    val spark = SparkSession
      .builder
      .appName("StreamingLoop")
      .master("local[1]")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "true")
      .getOrCreate()

    session = spark

  }

  def createNewQuery(in: String, query_file: File, out: String): StreamingQuery = {

    // Query file

    val query_text = Source.fromFile(query_file.getAbsolutePath)
      .getLines.map(_.+(" ")).mkString // Space instead of newline

    // Data input

    val data = session.readStream
      .schema(veiculosType)
      .option("maxFilesPerTrigger", 5)
      .option("multiLine", true)
      .json("file://" + in)

    // Data preprocessing

    val pre1 = data.select(explode(col("veiculos")).as("veiculo"))

    val pre2 = pre1
      .withColumn("codigo", (col("veiculo.codigo")))
      .withColumn("datahora", to_timestamp(from_unixtime(col("veiculo.datahora") / 1000L)))
      .withColumn("codlinha", (col("veiculo.linha")))
      .withColumn("latitude", (col("veiculo.latitude")))
      .withColumn("longitude", (col("veiculo.longitude")))
      .withColumn("velocidade", (col("veiculo.velocidade")))
      .withColumn("sentido", (col("veiculo.sentido")))
      .withColumn("nome", (col("veiculo.trajeto")))
      .drop(col("veiculo"))

    val pre3 = pre2
      .filter(!(col("nome").isNull) && !(col("codlinha").isNull))
      .filter((col("codlinha").like("5_____") || col("codlinha").like("__A") || col("codlinha").like("__")))
    val pre4 = pre3
      .withColumn("linha", trim(split(col("nome"), "-")(0)))
      .filter(col("linha").like("___") || col("linha").like("__"))
      .withColumn("corredor",
        when(col("linha").like("1%") or col("linha").like("2%"), "TransOeste").otherwise(
          when(col("linha").like("3%") or col("linha").like("4%"), "TransCarioca").otherwise(
            when(col("linha").like("5%"), "TransOlímpica").otherwise(""))))
    val pre5 = pre4
      .withColumnRenamed("nome", "trajeto")
      .drop(col("codlinha"))
    val pre6 = pre5
      .withWatermark("datahora", "1 minute")
      .dropDuplicates("codigo", "datahora")

    val query_data = pre6

    query_data.createOrReplaceTempView(session_name)

    // Executing query over data

    val queryDF = session.sql(query_text)

    // Data output (console)

//    val console_sq = queryDF.writeStream
//      .queryName(session_name + session_number)
//      .outputMode("update")
//      .format("console")
//      .start()

    // Data output (file)

    val output_sq = queryDF.writeStream
      .queryName(session_name + session_number)
      .outputMode("append")
      .format("csv")
      //.option("checkpointLocation", "checkpoint")
      .start("file://" + query_file.getParent + prov_dir + "batches/")

    // Start new watcher thread to query dir

    val dir_watcher: DirectoryWatcher = new DirectoryWatcher(query_file.getParentFile.toPath(), output_sq.id)
    val watch_thread: Thread = new Thread(dir_watcher)
    watch_thread.start()

    session_number += 1

    output_sq.awaitTermination()

    output_sq

  }

  /*
   * Main function
   */
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    LogManager.getRootLogger().setLevel(Level.WARN)

    checkFiles()
    startSession()
    
    
    steps_map.keys.foreach { s =>
      val sq = createNewQuery(steps_map(s)._1, steps_map(s)._2, steps_map(s)._3)
      stream_map.+(s -> (sq, sq.id))
    }

  }
}
