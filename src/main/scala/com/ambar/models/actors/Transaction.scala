package com.ambar.models.actors

import java.sql.Date

case class Transaction(TxnID:Int, FromAccountID: Int, ToAccountID: Int, TxnAmount: Int,TxnType: String, TxnDate: java.sql.Date)
