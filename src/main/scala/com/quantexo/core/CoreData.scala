package com.quantexo.core

import scala.beans.BeanProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import com.quantexo.utils.JobConfig

import org.apache.log4j.Logger


class CoreData(spark: SparkSession) extends Serializable { 
  @BeanProperty var flightDF: DataFrame = _
  @BeanProperty var passDF: DataFrame = _
  @BeanProperty var fSchema: StructType = _
  @BeanProperty var pSchema: StructType = _
  
val log = Logger.getLogger(getClass.getName)

  import JobConfig._
  try {
    flightDF = spark.read.schema(FlightSchema.apply).option("header", "true")
      .csv(JobConfig.flightFile).na.fill("")

    flightDF.cache()

    passDF = spark.read.option("header", "true").schema(PassengerSchema.apply)
      .csv(JobConfig.passFile).withColumnRenamed("passengerId", "passId").na.fill("")

    passDF.cache()

  } catch {
    case ex: Exception =>
      throw ex
  } finally {

  }

}

object CoreData {
  def apply(spark: SparkSession) =  {
    val cd = new CoreData(spark)
    cd
  }
}