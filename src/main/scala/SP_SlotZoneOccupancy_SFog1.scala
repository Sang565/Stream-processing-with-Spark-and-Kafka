/**
 * ======================================================================================
 * - Functions:
 *   + Real-time stream processing for streaming park-slot occupancy data sent from multiple gateways (at the edge layer)
 *      --> after procssing, the whole carpark occupancy is streamed to the A-Fog tier for further long-term storage and data analytics. *
 * - Technologies: Spark-Kafka Integration, Spark Structured Streaming
 * - Language: Scala
 * =======================================================================================
 * 25/04/2020:
 *   + Mapping with each carpark's zone (in CSV file) --> Visualise the zone-based occupancy in Python.
 *    --> display the zone-based occupancy results at this cluster
 * 29/05/2020
 *  Add 2 args for generating speed on topic 2 and topic 3 in order to get data faster ("12 seconds" = 5x faster) for simulation, but the generating speed in practice is "1 minute" (at topic 1)
 *
 * 11/06/2020: Big changes
 *   - Change to Spark 2.4.5. in build.sbt
 *   - Change from "lpc" to "sfog", "hpc" to "afog"
 *
 *  Note: if "sbt clean package" has an error: https://users.scala-lang.org/t/sbt-error-some-keys-were-defined-with-the-same-name-but-different-types/4585/2
 **/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.streaming.Trigger 

object SP_SlotZoneOccupancy_SFog1 {

  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage: SP_SlotZoneOccupancy_SFog1 <generating_speed_2>, <generating_speed_3>")
      System.exit(1)
    }

    val generating_speed_2 = args(0)
    val generating_speed_3 = args(1)

    //***************************************************************************************************
    //=== Step 1: Create a Spark session and variables: topic, kafka brokers =======
    val sparkMaster = "spark://sfog1-master:7077"

    //---- for local --------------
    val checkpointLocation = "file:///home/pi/spark-applications/5th-paper/checkpoint" //SANG: use local storage on Spark's nodes --> copy the same files and folder to ALL nodes
    val carparkZoneCSV = "file:///home/pi/spark-applications/5th-paper/carpark1_zones.csv"

    /*//---- for HDFS  --------------
    //val checkpointLocation = "/checkpoint" //- edit to HDFS path
    //val carparkZoneCSV = "/carpark_zones/carpark1_zones.csv"  //SANG: edit to HDFS path*/

    val kafkaBrokers_sfog1 = "sfog1-kafka01:9092"
    val kafkaBrokers_afog = "afog-kafka01:9092"

    //--- from SFog Kafka
    val inputTopic = "sp-occupancy-21"
    val inputTopic2 = "sp-occupancy-21-2" 
    val inputTopic3 = "sp-occupancy-21-3" 


    val zoneDisplayTopic = "sp-zone-occupancy-display"

    //--- to AFog Kafka
    val outputDisplayTopic = "sp-slot-occupancy-display"
    val outputTopic = "sp-slot-occupancy"
    val outputTopic2 = "sp-slot-occupancy-2" 
    val outputTopic3 = "sp-slot-occupancy-3" 

    val spark = SparkSession.builder()
      .appName("SP_SlotZoneOccupancy_SFog1")
      .master(sparkMaster)
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.sql.streaming.checkpointLocation", checkpointLocation) 
      .getOrCreate()

    import spark.implicits._
    val rootLogger = Logger.getRootLogger().setLevel(Level.ERROR) //only display ERROR, not display WARN

    //##########################################################################################
    //****** Step 2: Read streaming messages from Kafka topics, select desired data and parse on this data for processing ********
    //=== Step 2.1: Read and parse node mapping files (CSV file) -===========
    val zoneSchema = new StructType()
      .add("nodeID", StringType)
      .add("zoneID", IntegerType)

    val zoneDataFrame = spark
      .read
      .format("csv")
      .option("header", false)
      .schema(zoneSchema)
      .load(carparkZoneCSV)
      .as("zoneDataFrame")
    //zoneDataFrame.show()	// for debugging

    //=== Step 2.2: Read streaming messages and extract desired elements (e.g. Value) from Kafka topics ============
    //--- 2.2.1: Setup connection to Kafka
    val kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers_sfog1)
      .option("subscribe", inputTopic) //subscribe Kafka topic name: sp-topic
      .option("startingOffsets", "latest")
      //.option("maxOffsetsPerTrigger", 200)  // IMPORTANT: just ignored, which means read records as soon as they are in Kafka topic
      .load()

    // --- add more streams collecting on inputTopic-2 and -3
    val kafka2 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers_sfog1)
      .option("subscribe", inputTopic2) 
      .option("startingOffsets", "latest")
      //.option("maxOffsetsPerTrigger", 200)
      .load()

    val kafka3 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers_sfog1)
      .option("subscribe", inputTopic3) 
      .option("startingOffsets", "latest")
      //.option("maxOffsetsPerTrigger", 200)
      .load()

    //--- 2.2.2: Selecting desired data read from Kafka topic
    val kafkaData = kafka
      /*
      .withColumn("Key", $"key".cast(StringType))	
      .withColumn("Topic", $"topic".cast(StringType))
      .withColumn("Offset", $"offset".cast(LongType))
      .withColumn("Partition", $"partition".cast(IntegerType))*/
      .withColumn("Timestamp", $"timestamp".cast(TimestampType)) //Noted: this timestamp belongs to Kafka
      .withColumn("Value", $"value".cast(StringType))
      //.select("topic", "Key", "Value", "Partition", "Offset", "Timestamp")
      .select("Value", "Timestamp")
    val rawData = kafkaData.selectExpr("CAST(Value as STRING)")

    // --- add more streams collecting on inputTopic-2 and -3
    val kafkaData2 = kafka2
      .withColumn("Timestamp", $"timestamp".cast(TimestampType))
      .withColumn("Value", $"value".cast(StringType))
      //.select("topic", "Key", "Value", "Partition", "Offset", "Timestamp")
      .select("Value", "Timestamp")
    val rawData2 = kafkaData2.selectExpr("CAST(Value as STRING)")

    val kafkaData3 = kafka3
      .withColumn("Timestamp", $"timestamp".cast(TimestampType)) //SANG: this timestamp belongs to Kafka
      .withColumn("Value", $"value".cast(StringType))
      //.select("topic", "Key", "Value", "Partition", "Offset", "Timestamp")
      .select("Value", "Timestamp")
    val rawData3 = kafkaData3.selectExpr("CAST(Value as STRING)")


    //=== Step 2.3: parse data for processing in the next stage ==============
    //--- 2.3.1: define a schema for parsing
    val dataStreamSchema = new StructType()
      .add("timestamp", StringType)
      .add("nodeID", StringType)
      .add("payload", (new StructType)
        .add("occupied", StringType)
      )

    //--- 2.3.2: parse data for doing processing in the next stage
    val parkingData = rawData
      .select(from_json(col("Value"), dataStreamSchema).alias("parkingData")) // value from Kafka topic
      .select("parkingData.*")
      .select($"timestamp", $"nodeID", $"payload.occupied")

      .withColumn("timestamp", $"timestamp".cast(TimestampType))	
      .withColumn("nodeID", $"nodeID".cast(StringType))
      .withColumn("occupied", $"occupied".cast(IntegerType))
      .withColumn("processing-time", current_timestamp()) 

      .select("nodeID", "occupied", "processing-time")  
      //.select("nodeID", "occupied", "timestamp") //use processing-time, instead of event-time
    parkingData.printSchema()

    // - add more streams collecting on inputTopic-2 and -3
    val parkingData2 = rawData2
      .select(from_json(col("Value"), dataStreamSchema).alias("parkingData2")) 
      .select("parkingData2.*")
      .select($"timestamp", $"nodeID", $"payload.occupied")
      .withColumn("event-time", $"timestamp".cast(TimestampType))	
      .withColumn("nodeID", $"nodeID".cast(StringType))
      .withColumn("occupied", $"occupied".cast(IntegerType))
      .withColumn("processing-time", current_timestamp()) 
      .select("nodeID", "occupied", "event-time", "processing-time") 
    //parkingData2.printSchema()

    val parkingData3 = rawData3
      .select(from_json(col("Value"), dataStreamSchema).alias("parkingData3")) 
      .select("parkingData3.*")
      .select($"timestamp", $"nodeID", $"payload.occupied")
      .withColumn("event-time", $"timestamp".cast(TimestampType))	
      .withColumn("nodeID", $"nodeID".cast(StringType))
      .withColumn("occupied", $"occupied".cast(IntegerType))
      .withColumn("processing-time", current_timestamp()) 
      .select("nodeID", "occupied", "event-time", "processing-time") 
    //println("\n=== parkingData3 schema ====")
    //parkingData3.printSchema()

    //=============================================================================
    //=== Step 4.2: Process and output data for each application  =============
    //--------------------------------------------------------------------
    //--- 4.2.1: ZONE MAPPING VISUALISATION -------------
    // update slot occupancy status in each zone every 1 min
    //--- 4.2.1.1: join streaming data and zone map
    val parkingZoneDataDisplay = parkingData
      .join(zoneDataFrame, "nodeID").where($"nodeID" === $"zoneDataFrame.nodeID")	

    //--- 4.2.1.2: process data 
    val parkingZoneDisplay = parkingZoneDataDisplay
      .withWatermark("processing-time", "10 milliseconds")
      .groupBy("zoneID", "processing-time" )
      .sum("occupied") //.as("slotOccupancy")
      .withColumn("zoneOccupancy", $"sum(occupied)".cast(IntegerType))
      .select($"processing-time", $"zoneID", $"zoneOccupancy")  

    println("\n=== parkingZoneDisplay schema ====")
    parkingZoneDisplay.printSchema()

    //--- 4.2.1.3: output result --> .trigger(Trigger.ProcessingTime("1 minute"))
    parkingZoneDisplay
      .selectExpr ("CAST(zoneID AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .outputMode("update")   
      .option("kafka.bootstrap.servers", kafkaBrokers_sfog1)
      .option("topic", zoneDisplayTopic)	
      .trigger(Trigger.ProcessingTime("1 minute"))  
      .start()
    //.awaitTermination()

    //===============================================================================
    //=== 4.2.2: PROCESS AND SEND TO A-FOG CLUSTER ==================================
    //---------------------------------------------------------------------
    // update slot occupancy status for the whole CP every 1 min
    //--- 4.2.2.1-a: process data  --> Noted: .withColumn("carparkID", lit(1))  --> add any literal
    val currentSlotOccupancyDisplay = parkingData  
      .withWatermark("processing-time", "10 milliseconds")  
      .groupBy("processing-time")
      .sum("occupied") //.as("slotOccupancy")

      //---- Option1: For console/terminal display purpose, not use LED board -----------
      /*
      .withColumn("slotOccupancy", $"sum(occupied)".cast(IntegerType))
      .withColumn("carparkID", lit(1))	// --- add carparID into dataFrame 
      //.withColumn("current-time", $"processing-time".cast(TimestampType))
      //.select("carparkID", "current-time", "slotOccupancy") 
      .select("carparkID", "processing-time", "slotOccupancy") 
      */

      //---- Option2: For LED display purpose -----------
      .withColumn("nodeID", lit("80:7d:3a:f3:91:c0"))  
      // -- nodeID always uses MAC address of LED board (default)
      .withColumn("text", concat(lit(" "), $"sum(occupied)".cast(StringType), lit("  "))) 
      .select("nodeID", "text")

    println("\n=== currentSlotOccupancyDisplay schema ====")
    currentSlotOccupancyDisplay.printSchema()

    //--- 4.2.2.2-a: output result 
    //----- LED board --> access to a topic on S-Fog cluster
    currentSlotOccupancyDisplay
      .selectExpr ("CAST(nodeID AS STRING) AS key", "to_json(struct(*)) AS value")  
      .writeStream
      .format("kafka")
      .outputMode("update")   
      .option("kafka.bootstrap.servers", kafkaBrokers_afog)      
      .option("topic", outputDisplayTopic)	
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()
    //.awaitTermination()


    //--- 4.2.2.1-b: process data   --> .withColumn("carparkID", lit(1))  --> add any literal
    val currentSlotOccupancy = parkingData
      .withWatermark("processing-time", "5 seconds")
      .groupBy("processing-time")
      .sum("occupied") //.as("slotOccupancy")
      .withColumn("slotOccupancy", $"sum(occupied)".cast(IntegerType))
      .withColumn("carparkID", lit(1))	//add carparID into dataFrame  
      .select("carparkID", "processing-time", "slotOccupancy") 

    println("\n=== currentSlotOccupancy schema ====")
    currentSlotOccupancy.printSchema()

    // - add more streams collecting on inputTopic-2 and -3
    val currentSlotOccupancy2 = parkingData2
      // .withWatermark("processing-time", "5 seconds") 
      .withWatermark("event-time", "10 milliseconds") // use event-time in this case for fastening process
      //.groupBy("processing-time") 
      .groupBy("event-time") 
      .sum("occupied")   
      .withColumn("slotOccupancy", $"sum(occupied)".cast(IntegerType))
      .withColumn("carparkID", lit(1))	//- add carparID into dataFrame  
      .select("carparkID", "event-time", "slotOccupancy") 
    println("\n=== currentSlotOccupancy2 schema ====")
    //currentSlotOccupancy2.printSchema()

    val currentSlotOccupancy3 = parkingData3
      // .withWatermark("processing-time", "5 seconds") 
      .withWatermark("event-time", "10 milliseconds") // use event-time in this case for fastening process
      //.groupBy("processing-time") 
      .groupBy("event-time") 
      .sum("occupied")   
      .withColumn("slotOccupancy", $"sum(occupied)".cast(IntegerType))
      .withColumn("carparkID", lit(1))  //- add carparID into dataFrame  
      .select("carparkID", "event-time", "slotOccupancy") 
    println("\n=== currentSlotOccupancy3 schema ====")
    currentSlotOccupancy3.printSchema()


    //--- 4.2.2.2-b: output result 
    currentSlotOccupancy
      .selectExpr ("CAST(carparkID AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .outputMode("append") 
      .option("kafka.bootstrap.servers", kafkaBrokers_afog)
      .option("topic", outputTopic)	
      .option("truncate", false) 
      .trigger(Trigger.ProcessingTime("1 minute"))  
      .start()
    //.awaitTermination()   // - uncomment if it is the last line

    // add more streams collecting on inputTopic-2 and -3
    //val generating_speed_2 = args(0)
    currentSlotOccupancy2
      .selectExpr ("CAST(carparkID AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .outputMode("append")
      .option("kafka.bootstrap.servers", kafkaBrokers_afog)
      .option("topic", outputTopic2)	
      .option("truncate", false)  
      .trigger(Trigger.ProcessingTime(generating_speed_2)) // could be "30 seconds", "12 seconds", "6 seconds" for the corresponding "2x", "5x", "10x" faster
      .start()
    //.awaitTermination()   //- uncomment if it is the last line

    //val generating_speed_3 = args(1)
    currentSlotOccupancy3
      .selectExpr ("CAST(carparkID AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")

      .outputMode("append")
      .option("kafka.bootstrap.servers", kafkaBrokers_afog)
      .option("topic", outputTopic3)	
      .option("truncate", false)  
      .trigger(Trigger.ProcessingTime(generating_speed_3)) // could be "30 seconds", "12 seconds", "6 seconds" for the corresponding "2x", "5x", "10x" faster
      .start()

      .awaitTermination()   

  }
}