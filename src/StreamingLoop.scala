import java.io.IOException
import java.nio.file.FileSystems
import java.nio.file.Path
import java.nio.file.StandardWatchEventKinds
import java.nio.file.WatchEvent
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
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import java.io._
import java.io.StringWriter
import au.com.bytecode.opencsv.CSVWriter
import scala.collection.JavaConversions._
import java.io.FileWriter
import java.io.BufferedWriter

import java.util.Calendar



object StreamingLoop {
  
  var query: StreamingQuery = null
  var query_id: UUID = null
  var query_name: String = "StreamingLoop"
  var query_number: Int = 0
  var query_text: String = ""
  
  var session: SparkSession = null
  
  val data_dir: String = "/home/projeto/Downloads/SparkStreamingLoop/data/"
  val query_dir: String = "/home/projeto/Downloads/SparkStreamingLoop/query/"
  val provenance_dir: String = "/home/projeto/Downloads/SparkStreamingLoop/provenance/"
  
  val path: Path = FileSystems.getDefault().getPath(query_dir)
  val dir_watcher = new DirectoryWatcher(path)
  val watch_thread = new Thread(dir_watcher)
    
  // W/o JSON schema definition: 20+ minutes lost on schema inference 
  
  // JSON data format (single bus vehicle)
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

  // JSON data format (array of vehicles)
  val veiculosType = StructType(
    Array(StructField("veiculos", ArrayType(veiculoType))))
    
    
  class DirectoryWatcher(val path:Path) extends Runnable {
  
    def EventDetected(event:WatchEvent[_]) : Unit = {
      val kind = event.kind
      val event_path = event.context().asInstanceOf[Path]
      if(
         (kind.equals(StandardWatchEventKinds.ENTRY_CREATE) || kind.equals(StandardWatchEventKinds.ENTRY_MODIFY))
         &&
         !(event_path.getFileName.toString().startsWith(".")) // Discard temporary files (starting with ".")
        ) {
        println("New query: " + event_path) 
        
        val text = "\nDetected Humam In The Loop in '" + event_path + "' at the moment: " + Calendar.getInstance().getTime()
        val write = new PrintWriter(new FileOutputStream(new File(provenance_dir + "hil.txt"),true))     
        write.write(text) 
        write.close()

        session.streams.get(query_id).stop()
        createNewQuery(event_path.getFileName.toString())
       
      }
    }
  
    override def run(): Unit = {
      try {
        val watchService = path.getFileSystem().newWatchService()
        path.register(watchService, 
            StandardWatchEventKinds.ENTRY_CREATE,
            StandardWatchEventKinds.ENTRY_MODIFY)
        breakable {
          while (true) {
            val watchKey = watchService.take()
            watchKey.pollEvents().asScala.foreach( e => { EventDetected(e) })
            val valid = watchKey.reset()
            if (!valid) {
              println("No longer valid")
              watchKey.cancel()
              watchService.close()
              break
            }
          }
        }
      } catch {
        case ie: InterruptedException => println("InterruptedException: " + ie)
        case ioe: IOException => println("IOException: " + ioe)
        case e: Exception => println("Exception: " + e)
      }
    }
  }

		
 
  def startSession(): Unit = {
        
    // Session
    val spark = SparkSession
    .builder
    .appName("StreamingLoop")
    .master("local[1]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()
    
    session = spark
   
  }

  def createNewQuery(query_file: String): StreamingQuery = { 
    
 
      println("Thread alive? " + watch_thread.isAlive())
    
    // Query
    query_text = Source.fromFile(query_dir + query_file)
                .getLines.map(_.+(" ")).mkString   // Space instead of newline

    // Data
    
    val data = session.readStream
    .schema(veiculosType)
    .option("maxFilesPerTrigger",10)
    .json("file://" + data_dir)
    
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

    // Filtering here
//    val final_data = pre6.filter(col("corredor") === filter)
//    .groupBy(col("linha")).count()
//    .drop("codigo","datahora","latitude","longitude","velocidade","sentido","trajeto")
      
    val query_data = pre6    
            
    query_data.createOrReplaceTempView(query_name)
        
    val queryDF = session.sql(query_text)
    
    // Console
    query = queryDF.writeStream
      .queryName(query_name + query_number)
      .outputMode("append")
      .format("console")
      .start()
    
    // CSV (Provenance)
/*    query = queryDF.writeStream
      .queryName(query_name + query_number)
      .outputMode("append")
      .format("csv")
      .option("checkpointLocation", "checkpoint")
      .start("file://" + provenance_dir + "batches/")*/
    
    // Arquivo
//    query = queryDF.writeStream
//      .queryName(query_name + query_number)
//      .outputMode("append")
//      .format("csv")
//      .start()
      
    /*pre6.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("file:///home/projeto/Downloads/SparkStreamingLoop/mydata.csv")*/
      
    query_id = query.id
      
    query_number += 1
    
    query.awaitTermination()
    
    query
    
   
    
  }
  

  

  def main(args: Array[String]) {
     
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    LogManager.getRootLogger().setLevel(Level.WARN)
    
    watch_thread.start()
    startSession()
    createNewQuery("query.sql")
    

  }
}
