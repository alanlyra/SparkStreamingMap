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
import org.apache.spark.sql.streaming.StreamingQueryListener
import scala.util.parsing.json.JSON
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.ProcessingTime

object StreamingLoop {

  /*
   * Filesystem vars & functions
   */

  val base_dir: String = "/home/jonny/git/SparkStreamingLoop/exec"
  val data_dir: String = /*base_dir +*/ "/data"
  val query_dir: String = /*base_dir +*/ "/query"
  val prov_dir: String = /*base_dir +*/ "/prov"
  val out_dir: String =  /*base_dir +*/ "/out"
  
  // Arrays containing directories and query files
  var steps: Int = 0
  var data_in: Array[String] = Array.empty[String]
  var data_out: Array[String] = Array.empty[String]
  var data_prov: Array[String] = Array.empty[String]
  var query_file: Array[File] = Array.empty[File]
  
 
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
  def convertJSONtoCSV(): Unit = {
    print("JSON to CSV conversion: ")
    
    val files = Option(new File(data_in(0)).listFiles
        .filter(s => s.getName.endsWith(".json"))).getOrElse(Array.empty[File])
    if (files.isEmpty) {
      println("no JSON files to convert.")
      return
    }
    else
      for (file <- files){
        val source = Source.fromFile(file)
        val lines = try source.mkString finally source.close()
        val json = JSON.parseFull(lines)
        val veiculos = json.get.asInstanceOf[Map[String, List[Map[String,Any]]]]
        val veiculosList = veiculos.get("veiculos").getOrElse(null)
        if (veiculosList != null) {
          val veiculosList = veiculos.get("veiculos").get
          val header = veiculosList(0).keys.mkString(",")
          
          val dest = new PrintWriter(
              new File(file.getPath.substring(0,file.getPath.lastIndexOf(".")) + ".csv"))
          dest.write(header + "\r\n")
          for (v <- veiculosList)
             dest.write(v.values.mkString(",") + "\r\n")
          dest.flush
          dest.close
        }
        file.delete
      }
    println("OK.")
  }

  def checkFiles(verbose: Boolean = false): Unit = {
    
    // Directories
    
    val stepDirs = getListOfSubDirectories(base_dir)
      .filter(s => s.isDirectory())
      .filter(s => getInt(s.getName) != null)
      .sortBy(s => s.getName.toInt)    
      
    if (stepDirs.length <= 0) {
      println ("No steps detected. Exiting...")
      return
    }
    println("Detected " + stepDirs.length + " steps for this workflow. ")
//    stepDirs.foreach(s => println(s.getAbsolutePath))
    
    steps = stepDirs.length
    data_in = new Array[String](stepDirs.length)
    data_out = new Array[String](stepDirs.length)
    data_prov = new Array[String](stepDirs.length)
    query_file = new Array[File](stepDirs.length)
    
    // Queries / Data
    
   var queryFiles: Map[File,File] = Map()
   
    stepDirs.foreach { s =>
      val files = getListOfSubDirectories(s.getAbsolutePath + query_dir)
      .filter(s => s.isFile)
      .filter(s => s.getName.endsWith(".sql"))
      if (files.length > 0)
        queryFiles += (s -> files.last)
    }
    
//    queryFiles.keys.foreach(k => 
//      println("Step: " + k.getName + " | Query: " + queryFiles(k).getName)
//    )
          
    if (queryFiles.size < stepDirs.length) {
      val missing = stepDirs.filterNot(queryFiles.keys.toList.contains(_))
      println ("Missing query files in:")
      missing.foreach(s => println(s.getAbsolutePath + query_dir))
      return
    }
    
    val hasData = getListOfSubDirectories(stepDirs(0).getAbsolutePath + data_dir)
    .filter(s => s.isFile)
    .length > 0
    if (!hasData) {
      println("Missing initial data in " + stepDirs(0).getAbsolutePath + data_dir)
      return
    }
//    else
//      println("Data found in " +  stepDirs(0).getAbsolutePath + data_dir)

    var i: Int = 0
    stepDirs.sliding(2).foreach { s =>
      data_in(i) = s(0).getAbsolutePath + data_dir
      query_file(i) = queryFiles(s(0))
      
      new File(s(1).getAbsolutePath + data_dir).mkdirs
      data_out(i) = s(1).getAbsolutePath + data_dir
      new File(s(0).getAbsolutePath + prov_dir).mkdirs
      data_prov(i) = s(0).getAbsolutePath + prov_dir
      i+=1
    }
    data_in(i) = stepDirs.last.getAbsolutePath + data_dir
    query_file(i) = queryFiles(stepDirs.last)
    
    new File(base_dir + out_dir).mkdirs
    data_out(i) = base_dir + out_dir
    new File(stepDirs.last.getAbsolutePath + prov_dir).mkdirs
    data_prov(i) = stepDirs.last.getAbsolutePath + prov_dir
    
//    println()
//    for(i <- 0 to stepDirs.length -1){
//      println("Step: " + (i+1) + " | In: " + data_in(i) 
//          + " | Query: " + query_file(i).getAbsolutePath + " | Out: " + data_out(i))
//    }
    
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
        
        val stepNumber = getInt(event_path.toFile.getParentFile.getName.toString).get
        println("Detected human-in-the-loop: step " + stepNumber + ", query file: " + event_path.toFile.getPath)
        

        val text = "\nDetected Human In The Loop in '" + event_path + 
        "' at the moment: " + Calendar.getInstance().getTime()
        
        val write = new PrintWriter(new FileOutputStream(
            new File(data_prov(stepNumber) + "/hil.txt"), true))
        val file = Source.fromFile(event_path.toFile.getPath).mkString
        write.write(text)
        write.write("\r\n")
        write.write(file)
        write.write("\r\n")
        write.close()
        
        createWorkflow(from=stepNumber)

        watchService.close()

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

//  /*
//   * JSON schema definition
//   * W/o schema definition: 20+ minutes lost on schema inference 
//   */
//
//  val veiculoType = StructType(
//    Array(
//      StructField("codigo", StringType),
//      StructField("linha", StringType),
//      StructField("latitude", DoubleType),
//      StructField("longitude", DoubleType),
//      StructField("datahora", DoubleType),
//      StructField("velocidade", DoubleType),
//      StructField("id_migracao", DoubleType),
//      StructField("sentido", StringType),
//      StructField("trajeto", StringType)))
//
//  val veiculosType = StructType(
//    Array(StructField("veiculos", ArrayType(veiculoType))))

  /*
   * Spark vars & functions
   */
  
  var query_ids: Array[UUID] = Array.empty[UUID]
  var streaming_queries: Array[StreamingQuery] = Array.empty[StreamingQuery]
      
  var session: SparkSession = null
  var session_name: String = "StreamingLoop"
  var session_number: Int = 0

  def startSession(verbose: Boolean = false): Unit = {

    val spark = SparkSession
      .builder
      .appName("StreamingLoop")
      .master("local[1]")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "true")
      .config("spark.ui.showConsoleProgress", "false")
      .getOrCreate()

    session = spark
    
    session.streams.addListener(queryListener)
    
    println ("SparkSession started. \r\nAdded listener to streaming queries.")

  }
  
  
    def foreachWriterToFile(filePath: String, fieldNames: Array[String]) = new ForeachWriter[Row] {
      var fileWriter: PrintWriter = null
      
      def open(partitionId: Long, version: Long): Boolean = {
        val file = new File(filePath)
        fileWriter = new PrintWriter(file.getAbsolutePath)
        if (!file.exists) 
          fileWriter.write(fieldNames.mkString(",") + "\r\n")
        true
      }

      def process(record: Row) = {
        fileWriter.append(record.mkString(",") + "\r\n")
      }

      def close(errorOrNull: Throwable): Unit = {
        // close the connection
        fileWriter.flush
        fileWriter.close
      }
    }

     
  val queryListener = new StreamingQueryListener() {
    
    override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit =  {
      //println(s"Query ${event.id} (${event.name}) started")  
    }
    override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
      val stepNumber = query_ids.indexOf(event.progress.id)
      if (stepNumber > -1) {
        //println(s"Query ${event.progress.id} (${event.progress.name}) progress")
        val stepNumber = query_ids.indexOf(event.progress.id)
        val progressJson = event.progress.prettyJson
        val write = new PrintWriter(new FileOutputStream(
            new File(data_prov(stepNumber) + "/queryExecution_" + event.progress.id + "_" + event.progress.batchId + ".json"), true))
        write.write(progressJson)
        write.close()
      }
    }
    override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit =  {
      //println(s"Query ${event.id} terminated")
    }
  }
  
  
  def createWorkflow(from: Int = 0, verbose: Boolean = false): Unit = {
    
    // If already active, stop steps
    if (session.streams.active.length > 0){
      
      for (i <- from to query_ids.length){
        session.streams.get(query_ids(i)).stop()
        println("Stopping step " + (i+1))
      }
      session_number += 1
    }
    
    // If not, create all new
    else {
      streaming_queries = new Array[StreamingQuery](steps)
      query_ids = new Array[UUID](steps)
      session_number = 0
    }
    
    // Delete outs
    for (i <- from to data_out.length-1){
      for {
        files <- Option(new File(data_out(i)).listFiles)
        file <- files
      } file.delete()
    }
    // Delete prov & Spark files
    for (i <- from to data_out.length-1){
      for {
        files <- Option(new File(data_prov(i) + "/batches").listFiles)
        file <- files
      } file.delete()
            for {
        files <- Option(new File(data_prov(i) + "/batches/_spark_metadata").listFiles)
        file <- files
      } file.delete()
    new File(data_prov(i) + "/batches_spark_metadata").delete()
    new File(data_prov(i) + "/batches").delete()
    }
    
    for (i <- from to steps-1) {
      
      println("Starting step " + (i+1))
      println("IN: " + data_in(i))
      println("QUERY: " + query_file(i).getAbsolutePath)
      println("OUT: " + data_out(i))
      val sq = createNewQuery((i+1), data_in(i), query_file(i), data_out(i))
      streaming_queries(i) = sq
      query_ids(i) = sq.id
      println("Started step " + (i+1) + ". ID: " + sq.id)
    }
    
    println("Workflow created. Streaming query IDs:")
    println(query_ids)
    val fileWriter = new PrintWriter(new File(base_dir + "pid.txt"))
    fileWriter.write(query_ids.mkString(" \r\n"))
    fileWriter.close
  }
  
  
  

  def createNewQuery(stepNumber: Int, in: String, query_file: File, out: String, verbose: Boolean = false): StreamingQuery = {

    // Query file

    print("Query file: ")
    val query_text = Source.fromFile(query_file.getAbsolutePath)
      .getLines.map(_.+(" ")).mkString // Space instead of newline
    println ("OK.")
    // Data input

    print("Input folder: ")
    val data = session.readStream
      .option("maxFilesPerTrigger", 5)
//      .option("inferSchema", true)
//      .schema(veiculosType)
//      .option("multiLine", true)
//      .json("file://" + in)
      .option("header", true)
      .csv("file://" + in)
    println("read started.")

    // Data preprocessing
      
//    val pre1 = data.select(explode(col("veiculos")).as("veiculo"))
//
//    val pre2 = pre1
//      .withColumn("codigo", (col("veiculo.codigo")))
//      .withColumn("datahora", to_timestamp(from_unixtime(col("veiculo.datahora") / 1000L)))
//      .withColumn("codlinha", (col("veiculo.linha")))
//      .withColumn("latitude", (col("veiculo.latitude")))
//      .withColumn("longitude", (col("veiculo.longitude")))
//      .withColumn("velocidade", (col("veiculo.velocidade")))
//      .withColumn("sentido", (col("veiculo.sentido")))
//      .withColumn("nome", (col("veiculo.trajeto")))
//      .drop(col("veiculo"))
//
//    val pre3 = pre2
//      .filter(!(col("nome").isNull) && !(col("codlinha").isNull))
//      .filter((col("codlinha").like("5_____") || col("codlinha").like("__A") || col("codlinha").like("__")))
//    val pre4 = pre3
//      .withColumn("linha", trim(split(col("nome"), "-")(0)))
//      .filter(col("linha").like("___") || col("linha").like("__"))
//      .withColumn("corredor",
//        when(col("linha").like("1%") or col("linha").like("2%"), "TransOeste").otherwise(
//          when(col("linha").like("3%") or col("linha").like("4%"), "TransCarioca").otherwise(
//            when(col("linha").like("5%"), "TransOlímpica").otherwise(""))))
//    val pre5 = pre4
//      .withColumnRenamed("nome", "trajeto")
//      .drop(col("codlinha"))
      
    val pre5 = data.withColumn("datahora", to_timestamp(from_unixtime(col("datahora") / 1000L)))

//    val pre6 = pre5
//      .withWatermark("datahora", "1 minute")
//      .dropDuplicates("codigo", "datahora")

    val query_data = pre5

    query_data.createOrReplaceTempView(session_name + stepNumber)

    // Executing query over data

    print("Query over data: ")
    val queryDF = session.sql(query_text)
    println("started.")

    // Data output (console)

//    val console_sq = queryDF.writeStream
//      .queryName(session_name + session_number + "_S" + stepNumber + "_console")
//      .outputMode("append")
//      .format("console")
//      .start()

//    // Prov output (batch files)
//
//    val batch_sq = queryDF.writeStream
//      .queryName(session_name + session_number + "_S" + stepNumber + "_batch")
//      .outputMode("append")
//      .format("csv")
//      .option("checkpointLocation", "checkpoint")
//      .start("file://" + data_prov(stepNumber-1) + "/batches/")
      
    // Data output (file)
    
    print("Output sink: ")
    val output_sq = queryDF.writeStream
      .queryName(session_name + session_number + "_S" + stepNumber)
      .outputMode("append")
      .format("csv")
      .option("header",true)
      .option("checkpointLocation", data_prov(stepNumber-1) + "/checkpoint")
      .option("path", "file://" + data_out(stepNumber-1))
      .trigger(ProcessingTime("5 seconds"))
      .start()
    println("started.")
           
//      val output_sq = queryDF.writeStream
//      .queryName(session_name + session_number + "_S" + stepNumber + "_out")
//      .outputMode("append")
//      .foreach(foreachWriterToFile(data_out(stepNumber-1) + "/out.csv", queryDF.schema.fieldNames))
//      .start()
            
    // Start new watcher thread to query dir
    print("Query folder monitoring: ")
    val dir_watcher: DirectoryWatcher = new DirectoryWatcher(query_file.getParentFile.toPath(), output_sq.id)
    val watch_thread: Thread = new Thread(dir_watcher)
    watch_thread.start()
    println("OK.")
    
    //output_sq.awaitTermination()
    
    output_sq
  }

  /*
   * Main function
   */
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    LogManager.getRootLogger().setLevel(Level.OFF)

    checkFiles()
    
    // Convert JSON to CSV
    convertJSONtoCSV()

    startSession()
    createWorkflow()
    
  }
}
