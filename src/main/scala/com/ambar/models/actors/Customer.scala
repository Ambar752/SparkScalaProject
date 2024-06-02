package com.ambar.models.actors

import com.ambar.models.Gender.Gender
import com.ambar.models._

import java.sql.Date

case class Customer(CustomerID: Int, Name: String, DOB: Date, Gender: String, Address:String, CIN: String)
