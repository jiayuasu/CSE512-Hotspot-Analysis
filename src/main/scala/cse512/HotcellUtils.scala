package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // Check whether the right cell is a neighbor of the left cell
  def isNeighbor(xLeft:Int, yLeft:Int, zLeft:Int, xRight:Int, yRight:Int, zRight:Int): Boolean =
  {
    if ( (xLeft == xRight - 1 || xLeft == xRight || xLeft == xRight + 1)
      && (yLeft == yRight - 1 || yLeft == yRight || yLeft == yRight + 1 )
      && (zLeft == zRight - 1 || zLeft == zRight || zLeft == zRight + 1)
    ) true else false
  }

  // Calculate g*
  def calculateGstar(attributeSum: Double, attributePowerSum: Double, numCells: Double, neighborAttributeSum:Double, nonZeroNeighbors:Double): Double =
  {
    val averageX = attributeSum*1.0/numCells
    val S = Math.sqrt(attributePowerSum*1.0/numCells - averageX*averageX)
    val numerator = neighborAttributeSum - averageX*nonZeroNeighbors
    val denominator = S*Math.sqrt((numCells*nonZeroNeighbors - nonZeroNeighbors*nonZeroNeighbors)*1.0/(numCells-1))
    val result = numerator*1.0/denominator
    return result
  }
}
