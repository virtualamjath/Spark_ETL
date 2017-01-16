package catetl

case class Histogram( Machine_hist : String , date_hist : java.sql.Date , SMU_hist : Double ) 

case class RepairDate ( Machine_Repair : String , date_repair : java.sql.Date , SMU_repair : Double )

case class Cumulative ( Machine_Cumulative : String , date_cumulative : java.sql.Date , SMU_cumulative : Double )

case class Events ( Machine_events : String , date_events : java.sql.Date , SMU_events : Double )

case class Payload( Machine_payload : String, date_payload : java.sql.Date , SMU_payload : Double )

case class Fluid ( Machine_fluid : String , date_fluid : java.sql.Date , SMU_Fluid : Double ) 

case class LagDateDiff( serial1 : String , date1 : Option[java.sql.Date] , lagDate1 : Option[java.sql.Date] , repair_SMU1 : Option[Double] , Max_SMU1 : Option[Double] , dayDiff1 : Option[Double] ) 

case class LagSMUDiff( serial2 : String , date2 : Option[java.sql.Date] ,  lagDate2 : Option[java.sql.Date] , repair_SMU2 : Option[Double] , SMU : Option[Double] , lagSMU : Option[Double] , diffSMU : Option[Double] , dateDiff2 : Option[Double] )

case class Temp1 ( serial1 : String , date1 : Option[java.sql.Date] , repair_SMU1 : Option[Double] , Max_SMU1 : Option[Double] , dayDiff1 : Option[Double] , date2 : Option[java.sql.Date] ,  lagDate2 : Option[java.sql.Date] , repair_SMU2 : Option[Double] , SMU : Option[Double] , lagSMU : Option[Double] , diffSMU : Option[Double] , dateDiff2 : Option[Double] , fillUp : Option[Double]  )

case class Temp2 ( serial1 : String , date1 : Option[java.sql.Date] , repair_SMU1 : Option[Double] , Max_SMU1 : Option[Double] , dayDiff1 : Option[Double] , date2 : Option[java.sql.Date] ,  lagDate2 : Option[java.sql.Date] , repair_SMU2 : Option[Double] , SMU : Option[Double] , lagSMU : Option[Double] , diffSMU : Option[Double] , dateDiff2 : Option[Double] , fillUp : Option[Double] , NULL_sno : Option[Double] , sno : Option[Double] )

case class Temp3 ( serial1 : String , date1 : Option[java.sql.Date] , repair_SMU1 : Option[Double] , Max_SMU1 : Option[Double] , dayDiff1 : Option[Double] , date2 : Option[java.sql.Date] ,  lagDate2 : Option[java.sql.Date] , repair_SMU2 : Option[Double] , SMU : Option[Double] , lagSMU : Option[Double] , diffSMU : Option[Double] , dateDiff2 : Option[Double] , fillUp : Option[Double] , NULL_sno : Option[Double] , sno : Option[Double] , L_SMU : Option[Double] , SMU_N : Option[ Double ]  )

case class Temp4(  serial1 : String , date1 : Option[java.sql.Date] , repair_SMU1 : Option[Double] , Max_SMU1 : Option[Double] , dayDiff1 : Option[Double] , date2 : Option[java.sql.Date] ,  lagDate2 : Option[java.sql.Date] , repair_SMU2 : Option[Double] , SMU : Option[Double] , lagSMU : Option[Double] , diffSMU : Option[Double] , dateDiff2 : Option[Double] , fillUp : Option[Double] , NULL_sno : Option[Double] , sno : Option[Double] , L_SMU : Option[Double] ,  SMU_N : Option[ Double ] , fillUp2 : Option[Double] )


case class Temp5(  serial1 : String , date1 : Option[java.sql.Date] , repair_SMU1 : Option[Double] , Max_SMU1 : Option[Double] , dayDiff1 : Option[Double] , date2 : Option[java.sql.Date] ,  lagDate2 : Option[java.sql.Date] , repair_SMU2 : Option[Double] , SMU : Option[Double] , lagSMU : Option[Double] , diffSMU : Option[Double] , dateDiff2 : Option[Double] , fillUp : Option[Double] , NULL_sno : Option[Double] , sno : Option[Double],L_SMU : Option[Double] ,  SMU_N : Option[ Double ] , fillUp2 : Option[Double] , New_SMU : Option[Double] )


case class Temp6 ( serial : String , date : java.sql.Date , SMU : Double , diff_Days : Option[Double] , diff_SMU : Double , bin : Int )