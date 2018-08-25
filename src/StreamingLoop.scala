import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.PrintWriter
import java.nio.file.Path
import java.nio.file.StandardWatchEventKinds.ENTRY_CREATE
import java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY
import java.nio.file.StandardWatchEventKinds.OVERFLOW
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.util.Calendar
import java.util.UUID

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.io.Source
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable
import scala.util.parsing.json.JSON

import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.StreamingQueryListener

object StreamingLoop {
  
  
  //Execution time

  def time[R](block: => R): R = {  
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + ((t1 - t0)/1e9d) + "s")
    result
}

  // Filesystem vars & functions
   
  val base_dir: String = "/home/alan/git/SparkStreamingMap/exec"
  val data_dir: String = "/data"
  val query_dir: String = "/query"
  val out_dir: String =  "/out"
  
  // Arrays containing directories and query files
  var steps: Int = 0
  var data_in: Array[String] = Array.empty[String]
  var data_out: Array[String] = Array.empty[String]
  var query_file: Array[File] = Array.empty[File]
  
 
  def getListOfSubDirectories(dir: String): Array[File] = {
    val f = Option(new File(dir).listFiles)  //.filter(_.isDirectory).map(_.getName)
    // avoiding NullPointerException
    if (f == None)
      return Array.empty[File]
    f.get
  }
  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
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
    
    steps = stepDirs.length
    data_in = new Array[String](stepDirs.length)
    data_out = new Array[String](stepDirs.length)
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

    var i: Int = 0
    stepDirs.sliding(2).foreach { s =>
      data_in(i) = s(0).getAbsolutePath + data_dir
      query_file(i) = queryFiles(s(0))
      
      new File(s(1).getAbsolutePath + data_dir).mkdirs
      data_out(i) = s(1).getAbsolutePath + data_dir 
      i+=1
    }
    data_in(i) = stepDirs.last.getAbsolutePath + data_dir
    query_file(i) = queryFiles(stepDirs.last)
    
    new File(base_dir + out_dir).mkdirs
    data_out(i) = base_dir + out_dir   
  }
       
  // DirectoryWatcher: watch file changes 

  class DirectoryWatcher(val stepNumber: Int, val path: Path, val query_id: UUID) extends Runnable {

    val watchService = path.getFileSystem().newWatchService()
    path.register(watchService, ENTRY_CREATE, ENTRY_MODIFY, OVERFLOW)

    def EventDetected(event: WatchEvent[_]): Unit = {
      val kind = event.kind
      val event_path = event.context().asInstanceOf[Path]
      if ((kind.equals(ENTRY_CREATE) || kind.equals(ENTRY_MODIFY))
        &&
        !(event_path.getFileName.toString().startsWith(".")) // Discard temporary files (starting with ".")
        ) {

        println("Detected human-in-the-loop: step " + stepNumber + ", query file: " + event_path.toFile.getPath)
        
        val text = "\nDetected human-in-the-loop: '" + event_path + 
        "' at the moment: " + Calendar.getInstance().getTime()
 
        query_file(stepNumber-1) = new File(query_file(stepNumber-1).getParent + "/" + event_path.toString)
        createWorkflow(from=stepNumber-1)
        watchService.take().cancel()
        watchService.close()

      }
    }

    override def run(): Unit = {
      try {
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
        }
      } catch {
        case ie: InterruptedException => println("InterruptedException: " + ie)
        case ioe: IOException => println("IOException: " + ioe)
        case e: Exception => println("Exception: " + e)
      }
    }
  }
 
  // Spark vars & functions
  
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
    
    println ("SparkSession started. Added listener to streaming queries.")

  }
       
  val queryListener = new StreamingQueryListener() {
    
    override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit =  {     
    }
    override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
      val stepNumber = query_ids.indexOf(event.progress.id)
      if (stepNumber > -1) {
        val stepNumber = query_ids.indexOf(event.progress.id)
        val progressJson = event.progress.prettyJson
        
      }
    }
    override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit =  {     
    }
  }
  
  
  def createWorkflow(from: Int = 0, verbose: Boolean = false): Unit = {
    
    // If already active, stop steps
    if (session.streams.active.length > 0){
      
      for (i <- from to query_ids.length-1){
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
    // Delete Spark files
    for (i <- from to data_out.length-1){ 
      deleteRecursively(new File(data_out(i)))
    }
    
    for (i <- from to steps-1) {
      
      println("\r\nStarting step " + (i+1))
      println("IN: " + data_in(i))
      println("QUERY: " + query_file(i).getAbsolutePath)
      println("OUT: " + data_out(i))
      val nq_time  = time {createNewQuery((i+1), data_in(i), query_file(i), data_out(i))}
      
      print("Waiting for data output... ")
      // Wait for first batch processing
      breakable {
        while (true) {
          Thread.sleep(500)
          if (streaming_queries(i).recentProgress.length > 0) {
          println("OK.")
          break
          }
        }
      }
    
    }
    
    println("\r\nWorkflow created successfully")
  }

  def createNewQuery(stepNumber: Int, in: String, query_file: File, out: String, verbose: Boolean = false): Unit = {

    // Query file

    print("Query file: ")
    val query_text = Source.fromFile(query_file.getAbsolutePath)
      .getLines.map(_.+(" ")).mkString // Space instead of newline
    println ("OK.")
    // Data input

    print("Read input folder: ")
    val data = session.readStream
      .option("maxFilesPerTrigger", 1)
      .option("inferSchema", true)
      .option("header", true)
      .option("mode","DROPMALFORMED")
      .csv("file://" + in)
    println("OK.")
    
    val dataTimestamp = 
      if (stepNumber == 1)
        data.withColumn("datahora", to_timestamp(from_unixtime(col("datahora") / 1000L)))
      else
        data

    val query_data = dataTimestamp

    query_data.createOrReplaceTempView(session_name + stepNumber)

    // Executing query over data

    print("Execute query over data: ")
    val queryDF = session.sql(query_text)
    println("OK.")

    print("Start stream processing: ")
    val output_sq = queryDF.repartition(1).writeStream
      .queryName(session_name + session_number + "_S" + stepNumber)
      .outputMode("append")
      .format("csv")
      .option("header",true)
      .option("checkpointLocation", data_out(stepNumber-1) + "/checkpoint")
      .option("path", "file://" + data_out(stepNumber-1))
      .trigger(ProcessingTime("5 seconds"))
      .start()
    println("OK.")
                       
    // Start new watcher thread to query dir
    print("Query folder monitoring: ")
    val dir_watcher: DirectoryWatcher = new DirectoryWatcher(stepNumber, query_file.getParentFile.toPath(), output_sq.id)
    val watch_thread: Thread = new Thread(dir_watcher)
    watch_thread.start()
    println("OK.")
        
    streaming_queries(stepNumber-1) = output_sq
    query_ids(stepNumber-1) = output_sq.id
    
    println("Query started successfully. ID: " + output_sq.id)
  }
  
  // Main function
  
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