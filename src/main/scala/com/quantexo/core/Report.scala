package com.quantexo.core

import org.apache.spark.sql.DataFrame

object Report extends Serializable { 
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