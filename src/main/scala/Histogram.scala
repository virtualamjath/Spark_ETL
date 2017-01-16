package catetl
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive._
import java.util.Properties
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst
import org.apache.spark.rdd._
import org.apache.spark.sql._

object Histogram {

	def getSqlDate( str : String ) : java.sql.Timestamp = { 
		
		java.sql.Timestamp.valueOf( str )		
		//java.sql.Timestamp.valueOf("2001:11:23 12:01:56")
	}
	
	

	def utilDate( str: String ) : java.util.Date = {
		
		val s = str.slice( 0, 19)
		val format = new java.text.SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
		format.parse( str )
		
	}


	def lag1 ( list : List[(Int, String, Double, Double, java.util.Date, Double, Double)] ) : List[(Int, String, Double, Double, java.util.Date, Double, Double , Double)]  = {
			
	var temp: List[(Int, String, Double, Double, java.util.Date, Double, Double , Double)] = List[(Int, String, Double, Double, java.util.Date, Double, Double , Double)]() ;
		
	for ( p <- list ) {
					 
					if( list.indexOf(p) == 0 ){
			temp = List( ( p._1 , p._2 , p._3, p._4 , p._5 , p._6, p._7 , 0.0 ) )						
						
					}
				     else 
			temp = temp ::: List( ( p._1 , p._2 , p._3, p._4 , p._5 , p._6, p._7 , p._7 - list(list.indexOf(p) - 1 )._7  ) )
	}	
        
	temp
						
     }        


	def lag2( list :List[(java.sql.Timestamp)] ) : List[( java.sql.Timestamp , Double ) ] = {
		
		val l = list.sortWith( ( p, q ) => p before q )
		var m = List[(java.sql.Timestamp , Double)]()		
		for ( p <- l ) {
			if( l.indexOf(p) == 0 ) { 
				m = List( ( p , 0.0 ) )			
			}
			else{
				m = m ::: List( (p,  ( ( l( l.indexOf(p) - 1 ).getTime - p.getTime ) /1000.0 ) ) )
			}	
		}  	
		m	
	}	


	def  lag3 ( list : List[Double]) : List[(Double, Double)]  = {
	  val l = list.sortWith( (p,q) => p < q )
	  var m = List[( Double , Double ) ]()
	   for ( p <-l ){
	   	if( l.indexOf( p) == 0 ) {
		   m = List[(Double,Double)]( (p , 0.0) )
		}
		else{
		       m = m ::: List[(Double,Double)]( (p , p - l ( l.indexOf(p) - 1 ))  )
		}
	   }
	   m
	}

	def  list1 ( l : List[	(String , Double)] ) : String = {
		val li = l.map( x => ( x._1 , Math.round( x._2 * 100 ) / 100.0 ) ) . sortBy( _._1)

		//var list : List[Double] = List[Double] () 
		var str = ""
				
		
		for( p <-li) {
			
			str = str + p._2.toString + ","
		 }
		
		 str = str.slice( 0, str.length-1 )	
		str
	}




	
	def evaluteHistogram(  sc : SparkContext , filePath : String ) : org.apache.spark.rdd.RDD[String] = {
	
		  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	     import sqlContext._
	    import sqlContext.createSchemaRDD
	   
	    val HistogramFile = sc.textFile(filePath)
	    val str = HistogramFile.take(1)(0).split(",").toList
	    
	    val indexOfMachine = str.indexOf("SERIAL_NUMBER")
	    val indexOfCELL_1_DESC = str.indexOf("CELL_1_DESC")
	    val indexOfCELL_1_SEQ_NO = str.indexOf("CELL_1_SEQ_NO")
	    val indexOfHSTGR_ID = str.indexOf("HSTGR_ID")
	    val indexOfVIMSTIME = str.indexOf("VIMSTIME")
	    val indexOfSMU = str.indexOf("SMU")
	    val indexOfDataValue = str.indexOf("DATAVALUE")
	    //val indexOfHSTGR_ID = str.indexOf("HSTGR_ID")
	    val indexOfCELL_1_LWR_BNDRY2 = str.indexOf("CELL_1_LWR_BNDRY2")
	    val indexOfCELL_1_UP_BNDRY2 = str.indexOf("CELL_1_UP_BNDRY2")
	    
	    val filteredHistogram = HistogramFile.filter(! _.contains("SERIAL_NUMBER") )
	    
            val HistogramSchema = filteredHistogram .map( line => { val words = line.split(","); 
					             HistogramRecord( words( indexOfMachine ) , words(indexOfCELL_1_DESC ) , words(indexOfCELL_1_SEQ_NO).toInt , words( indexOfHSTGR_ID ), words(indexOfCELL_1_LWR_BNDRY2).toDouble , words(indexOfCELL_1_UP_BNDRY2).toDouble  , getSqlDate ( words( indexOfVIMSTIME  ) ) , words( indexOfSMU).toDouble , words( indexOfDataValue ) .toDouble ) } ).toSchemaRDD


		 HistogramSchema.registerTempTable("Histogram")

		  val histTable_1 = sqlContext.sql("SELECT * FROM Histogram ORDER BY  Serial, CELL_1_DESC, CELL_1_LWR_BNDRY2,VIMSTIME")

	  val hist = histTable_1.map( row => row.mkString(",") ).map( line => { val w = line.split(",");  ( ( w(0) + "," + w(1) + "," + w(4).toString ) ,   ( w(2).toInt, w(3) , w(4).toDouble ,  w(5).toDouble , utilDate( w(6)),  w(7).toDouble ,  w(8).toDouble ) ) } )

           val flatmappedHist = new PairRDDFunctions (hist ).combineByKey( (value) => List( value ) , 
			(aggr: List[(Int, String, Double , Double , java.util.Date , Double , Double )], value) => aggr ::: (value :: Nil),
 			(aggr1: List[(Int, String, Double , Double , java.util.Date , Double , Double )], aggr2: List[(Int, String, Double , Double , java.util.Date , Double , Double )]) => aggr1 ::: aggr2 )



	  val tempRDD = new PairRDDFunctions (flatmappedHist).mapValues( x => x.toList.sortWith(  (p,q) => p._5 before q._5 ) )

	  val laggedRDD =  new PairRDDFunctions ( new PairRDDFunctions (tempRDD ). mapValues( x => lag1( x) ) ) .flatMapValues( x => x )


	   val  lag1SchemaRDD = laggedRDD.map( x => { val w = x._1.split(",") ;  Lag1 ( w(0) , w(1) , x._2._1 , x._2._2.toInt , x._2._3 , x._2._4 , new java.sql.Timestamp( x._2._5.getTime() ) , x._2._6 , x._2._7 , x._2._8  )  } ) .toSchemaRDD

	  lag1SchemaRDD.registerTempTable("lag1Table")
	
	  val lagged_Table = sqlContext.sql("SELECT serial AS ser, c_desc , cell_seq , hist_ID ,Lwr_B ,Up_B ,VIMS ,SMU , DataValue , Act_DataValue AS A_DataValue , DataValue    FROM lag1Table ORDER BY serial,VIMS,Lwr_B  ")
	  
	   lagged_Table.registerTempTable("lag1Table2")

	val s_lagged_Table = sqlContext.sql("SELECT  ser , c_desc , VIMS , sum( A_DataValue ) AS sum_A_DataValue FROM lag1Table2 GROUP BY ser  , VIMS , c_desc ORDER BY ser  , VIMS , c_desc ")

	  s_lagged_Table.registerTempTable("summary1Table")
	  
	  sqlContext.cacheTable("lag1Table2")

	 val joined1 = sqlContext.sql("SELECT lag1Table2.ser , lag1Table2.c_desc ,lag1Table2.SMU , lag1Table2.cell_seq ,lag1Table2.hist_ID, lag1Table2.Lwr_B , lag1Table2.Up_B , lag1Table2.VIMS , lag1Table2.DataValue , A_DataValue, summary1Table.sum_A_DataValue , lag1Table2.ser FROM lag1Table2 LEFT OUTER JOIN  summary1Table ON  lag1Table2.ser = summary1Table.ser AND  lag1Table2.VIMS = summary1Table.VIMS AND  lag1Table2.c_desc = summary1Table.c_desc  " )
	
	joined1.registerTempTable("Joined1Table")
	sqlContext.cacheTable("Joined1Table")

	val distinctLag1Table = sqlContext.sql("SELECT distinct ser ,  VIMS , c_desc FROM lag1Table2 ORDER BY ser , c_desc , VIMS")
	
	val distinctlag1mapped = distinctLag1Table.map( row => ( (row(0).toString , row(2).toString ) , getSqlDate(row(1).toString )) )

	val Temp1 =	new PairRDDFunctions ( distinctlag1mapped).combineByKey( (value) => List( value ) , 
			(aggr: List[( java.sql.Timestamp )], value) => aggr ::: (value :: Nil) ,
 			(aggr1: List[(java.sql.Timestamp )], aggr2: List[( java.sql.Timestamp )]) => aggr1 ::: aggr2 )


		
      	val Timelag = new PairRDDFunctions ( new PairRDDFunctions(Temp1).mapValues( x => lag2(x ) ) ).flatMapValues( x => x ).map( x => TimeLag( x._1._1 , x._1._2 , x._2._1 , x._2._2 ) ).toSchemaRDD
	
	Timelag.registerTempTable("TimelagTable")


	val joinedTable2 = sqlContext.sql("SELECT Joined1Table.ser , Joined1Table.c_desc , Joined1Table.VIMS , Joined1Table.SMU , Joined1Table.cell_seq , Joined1Table.hist_ID , Joined1Table.Lwr_B, Joined1Table.Up_B , Joined1Table.DataValue, Joined1Table.A_DataValue , Joined1Table.sum_A_DataValue, TimelagTable.timediff  FROM Joined1Table LEFT OUTER JOIN TimelagTable ON ser = serial AND c_desc = cell_desc AND VIMS = Vims ")

	val smuTable = sqlContext.sql("SELECT distinct ser , c_desc , SMU FROM lag1Table2  ORDER BY ser , c_desc , SMU")	


	val Temp2 =  new PairRDDFunctions(smuTable.map( row => ( ( row(0).toString , row(1).toString ), row(2).toString.toDouble ) ) ).
		combineByKey( (value) => List( value ) , 
        (aggr: List[( Double )], value) => aggr ::: (value :: Nil) ,
 	(aggr1: List[(Double )], aggr2: List[( Double )]) => aggr1 ::: aggr2 )

	
	val laggedSMU = new PairRDDFunctions ( new PairRDDFunctions (Temp2) . mapValues( x => lag3( x ) ) ).flatMapValues( x => x ).map( x => LagSMU( x._1._1 , x._1._2 , x._2._1 , x._2._2 ) ) .toSchemaRDD 


	laggedSMU.registerTempTable("LaggedSMU")

	joinedTable2.registerTempTable("joinedTable2")


	val  join3 =  sqlContext.sql("SELECT  joinedTable2.ser, joinedTable2.c_desc ,joinedTable2.SMU ,joinedTable2.VIMS , joinedTable2.cell_seq, joinedTable2.hist_ID , joinedTable2.Lwr_B , joinedTable2.Up_B , joinedTable2.DataValue , joinedTable2.A_DataValue , joinedTable2.sum_A_DataValue , joinedTable2.timediff , LaggedSMU.laggedSMU    FROM joinedTable2 LEFT OUTER JOIN LaggedSMU ON  joinedTable2.ser = LaggedSMU.serial AND joinedTable2.c_desc = LaggedSMU.cell_desc AND joinedTable2.SMU = LaggedSMU.SMU " )

	join3.registerTempTable("Joine3Table")
	
	val summary2 = sqlContext.sql("SELECT ser  ser_s  , max( cell_seq )  Max_cell_seq   , hist_ID  hist_ID_s , VIMS  VIMS_s ,c_desc  c_desc_s  FROM Joine3Table GROUP BY ser , hist_ID ,c_desc, VIMS ")

	summary2.registerTempTable("summary2Table")
	
	val join4  = sqlContext.sql("SELECT  Joine3Table.ser , Joine3Table.c_desc , Joine3Table.SMU , Joine3Table.VIMS , Joine3Table.cell_seq , Joine3Table.hist_ID, Joine3Table.Lwr_B , Joine3Table.Up_B , Joine3Table.DataValue , Joine3Table.A_DataValue , Joine3Table.sum_A_DataValue , Joine3Table.timediff , Joine3Table.laggedSMU , summary2Table.Max_cell_seq FROM Joine3Table LEFT OUTER JOIN summary2Table ON ser = ser_s AND c_desc =  c_desc_s AND VIMS =  VIMS_s")

	join4.registerTempTable("Join4Table")

	val join4Table2 = sqlContext.sql("SELECT ser , c_desc , SMU , VIMS , cell_seq , hist_ID , Lwr_B , Up_B , DataValue , A_DataValue , sum_A_DataValue ,timediff , laggedSMU, Max_cell_seq ,  ( CASE  WHEN (cell_seq <= Max_cell_seq/ 3.0) THEN \"1_L\"  WHEN  ( cell_seq > (Max_cell_seq) * (2.0 / 3) ) THEN  \"3_H\"  ELSE \"2_M\" END )  AS Boundary , ( CASE WHEN laggedSMU = 0.0 THEN 0.0 ELSE DataValue/laggedSMU END ) AS TimeSpent   FROM Join4Table ")
	  
	 join4Table2.registerTempTable("join4Table2")
	 

	val joined4Table3 = sqlContext.sql("SELECT ser, c_desc , VIMS , Boundary , min(Lwr_B) AS min_Lwr , max( Up_B) AS max_Up , sum( TimeSpent ) AS TimeSpentPerHour   FROM join4Table2 GROUP BY ser , c_desc , VIMS , Boundary " )

	val joined4Table4 = joined4Table3.map( row => Join4( row(0).toString , java.sql.Date.valueOf(row(2).toString.slice(0,10) ) , row(6).toString.toDouble , row(1) + "_" + row(3) + "_" + row(4).toString + "_" + row(5).toString ) )
	
	joined4Table4.registerTempTable("joined4Table4")
	
	val joined4Table5 = sqlContext.sql("SELECT serial , Date, N_Boundary , sum(TimeSpent) AS Timespent FROM joined4Table4 GROUP BY serial,Date , N_Boundary ORDER BY serial , Date " )

	val combinedjoin4Table = new PairRDDFunctions ( joined4Table5.map( row => ( ( row(0).toString , row(1).toString ) , ( row(2).toString , row(3).toString.toDouble ) ) ) ).
		combineByKey( (value) => List( value ) , 
			(aggr: List[( String , Double )], value) => aggr ::: (value :: Nil) ,
 			(aggr1: List[(String, Double )], aggr2: List[( String , Double )]) => aggr1 ::: aggr2 )


	val returnRDD  =  new PairRDDFunctions ( combinedjoin4Table ).mapValues( x => list1(x) ).sortBy( _._1).map( x => x._1._1 + "," + x._1._2 +","+ x._2 )

	
	returnRDD
	
	}

}
