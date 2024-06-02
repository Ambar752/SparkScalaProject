package com.ambar.models

object FileFormat extends Enumeration {
    type FileFormat = Value
    val CSV, PARQUET = Value

  def getFormat(format: FileFormat): String = {
    format match {
      case CSV => FileFormat.CSV.toString.toLowerCase
      case PARQUET => FileFormat.PARQUET.toString.toLowerCase
    }
  }
}
