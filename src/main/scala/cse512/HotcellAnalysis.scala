package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  println("[Hot cell]")
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // Count the trips per cell
  var countPerCell = pickupInfo.groupBy("x", "y", "z").count()
  countPerCell.createOrReplaceTempView("cellcount")
  countPerCell = spark.sql("select x, y, z, count as attribute, count*count as attributepower from cellcount")
  countPerCell.createOrReplaceTempView("cellcount")
  countPerCell.show()

  val attributeSum = spark.sql("select sum(attribute) from cellcount").collect()(0).get(0).asInstanceOf[Long]
  val attributePowerSum = spark.sql("select sum(attributepower) from cellcount").collect()(0).get(0).asInstanceOf[Long]
  println(attributeSum)
  println(attributePowerSum)

  // Self join to find the neighbors' attribute for each cell
  spark.udf.register("rightIsNeighborOfLeft",(xLeft:Int, yLeft:Int, zLeft:Int, xRight:Int, yRight:Int, zRight:Int) => HotcellUtils.isNeighbor(xLeft, yLeft, zLeft, xRight, yRight, zRight))
  var cellWithNeighbors = spark.sql("select cc1.x, cc1.y, cc1.z, cc2.attribute from cellcount cc1 left join cellcount cc2 on rightIsNeighborOfLeft(cc1.x, cc1.y, cc1.z, cc2.x, cc2.y, cc2.z)")
  cellWithNeighbors.createOrReplaceTempView("cellwithneighbors")
  cellWithNeighbors = spark.sql("select x, y, z, sum(attribute) as neighborattributesum, count(attribute) as nonzeroneighbors from cellwithneighbors group by x, y, z")

  //Start calculating G*
  val calculateGstar = udf(
    (attributeSum: Double, attributePowerSum: Double, numCells: Double, neighborAttributeSum:Double, nonZeroNeighbors:Double)=>
  HotcellUtils.calculateGstar(attributeSum,attributePowerSum,numCells,neighborAttributeSum,nonZeroNeighbors)
  )
  cellWithNeighbors = cellWithNeighbors.withColumn("gstar",calculateGstar(lit(attributeSum),lit(attributePowerSum),lit(numCells),cellWithNeighbors("neighborattributesum"),cellWithNeighbors("nonzeroneighbors")))
  cellWithNeighbors.createOrReplaceTempView("cellwithgstar")

  // Sort the cells descending based on g star
  cellWithNeighbors = spark.sql("select x, y, z from cellwithgstar order by gstar desc")

  return cellWithNeighbors
}
}
