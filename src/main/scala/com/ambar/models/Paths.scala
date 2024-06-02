package com.ambar.models

import com.ambar.models

object Paths extends  Enumeration {
  type Paths = Value
  val ResoureBasePath, TestPath: models.Paths.Value = Value

  def getPathString(enum : Paths): String = enum match {
      case ResoureBasePath => "D:\\ambar\\IntelliJProjects\\SparkScalaProject\\src\\main\\resources"
      case TestPath => "D:\\ambar\\IntelliJProjects\\SparkScalaProject\\src\\test"
    }
}
