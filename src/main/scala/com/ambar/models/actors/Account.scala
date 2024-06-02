package com.ambar.models.actors

import java.sql.Date

case class Account(AccountID: Int, CustomerID: Int, BranchID: Int, AccountType: String, AccountOpenDate: Date)
