package com.quantexo.core

import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger


object Report extends Serializable { 
  
   val log = Logger.getLogger(getClass.getName)

  def apply(writeDF: DataFrame, reportFile: String) = {
    var status = false
    try {
      writeDF.coalesce(1).write.option("header", "true").mode("overwrite").csv(reportFile)
      status = true
    } catch {
      case ex: Exception => {
        status = false
        throw ex
      }
    }
    status

  }

}