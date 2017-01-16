package catetl


case class Cumulative_class ( Serial : Option[ String] , VIMS : Option[java.sql.Timestamp ] , SMU : Option[Double] , CUMID : Option[Double]  , Description : Option[String] , DataValue : Option[Double] , ErrorCount : Option[Double] , UnitId : Option[Double] , scaleFactor : Option[Double] , date : Option[java.sql.Date] ) 


case class CumulativeIdle ( SerialIdle : Option[String] , VIMS_Idle : Option[java.sql.Date] , SMU_Idle : Option[Double] , deltaSMUIdle : Option[Double] , DeltaIdleTime : Option[Double] )
 
case class CumulativeEST ( SerialEST : Option[String] , VIMS_EST : Option[java.sql.Date] , SMU_EST : Option[Double] , deltaSMUEST : Option[Double] , DeltaESTTime : Option[Double] )

case class CumulativeStarts( SerialStarts : Option[String] , VIMS_Starts : Option[java.sql.Date] , SMU_Starts : Option[Double] , deltaSMUStarts : Option[Double] , DeltaStartsTime : Option[Double] )


case class IdleSummaryTemp ( SerialIdle : Option[String] , dateIdle : Option[java.sql.Date] , SMUIdle : Option[Double] , sum_deltaSMUIdle : Option[Double] , sum_SMUIdleTime : Option[Double] , SMUEngineIdleRatio : Option[Double] , dayDiffIdle : Option[Long] , Avg_SMU : Option[Double] , Avg_Value : Option [Double] , avg_value_ : Option[Double] , bin1 : Option[Double]  )

case class ESTSummaryTemp( SerialEST : Option[String] , dateEST : Option[java.sql.Date] , SMUEST : Option[Double] , sum_deltaSMUEst : Option[Double] , sum_SMUEstTime : Option[Double] , LitrePerHour : Option[Double] , dayDiffEST : Option[Long] , Avg_SMU : Option[Double] , Avg_Value : Option[Double] , bin2 : Option[Double]  )


case class StartsSummaryTemp( SerialStarts : Option[String] , dateStarts : Option[java.sql.Date] , SMUStarts : Option[Double] , sum_deltaSMUStarts : Option[Double] ,  sum_SMUStartsTime : Option[Double] , dayDiff : Option[Long] , Avg_SMU : Option[Double] , Avg_Value : Option[Double] , avg_value_ : Option[Double] , bin3 : Option[Double]   )

case class masterTableTemp( Serial : String , startDate : java.sql.Date , EndDate : java.sql.Date , dateValid : java.sql.Date )
