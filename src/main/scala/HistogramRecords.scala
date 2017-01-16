package catetl



case class HistogramRecord ( Serial : String , CELL_1_DESC : String , CELL_1_SEQ_NO : Int , HSTGR_ID : String , CELL_1_LWR_BNDRY2 : Double , CELL_1_UP_BNDRY2 : Double , VIMSTIME : java.sql.Timestamp , SMU : Double , DATAVALUE : Double )

case class Lag1( serial : String , c_desc : String , cell_seq : Int , hist_ID : Int , Lwr_B : Double , Up_B : Double , VIMS : java.sql.Timestamp , SMU : Double , DataValue : Double , Act_DataValue : Double )

case class TimeLag ( serial : String , cell_desc : String , Vims : java.sql.Timestamp , timediff : Double ) 

case class LagSMU ( serial : String , cell_desc : String , SMU : Double , laggedSMU : Double)

case class Join4 ( serial : String , Date : java.sql.Date , TimeSpent : Double , N_Boundary : String  )



