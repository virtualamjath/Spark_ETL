package catetl

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.sql._


object Cumulative_ERC {


def dropHeader(data: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[String] = {
         	data.mapPartitionsWithIndex((idx, lines) => {
           		if (idx == 0) {
             			lines.drop(1)
           		}
           		lines
         	}	)
  	}		
		
	
	
 	def getString ( x  : Any ) : Option[String] = {
           try{ Some(x.toString) }
	   catch{ case p : java.lang.NullPointerException => None 
		case e : java.lang.Exception => None }

  	}

 	def getDouble( str : Any ) : Option[Double] = {
		
		try  { Some( str.toString.toDouble ) }
		catch { case p : java.lang.NullPointerException => None 
			case e : java.lang.Exception => None  }
    	}


	def sqlDate( d : Any )  : Option[ java.sql.Date] = {

        	try{  Some(java.sql.Date.valueOf( d.toString )) }
        	catch{  case p : java.lang.NullPointerException => None 
			case e : java.lang.Exception => None}
    
    	}


	def sqlTimestamp( d : Any ) : Option[java.sql.Timestamp] = {
		
		try {
			Some( java.sql.Timestamp.valueOf( d.toString.trim ) ) 
		}
		catch { case p : java.lang.NullPointerException => None 
			case e : java.lang.Exception => None
		}
	}

	def mySqlDate( str : Any ) : Option[java.sql.Date] = {
		try{   Some( java.sql.Date.valueOf(str.toString.split(" ")(0) ) )  }
		catch{  case p : java.lang.NullPointerException => None 
			case e : java.lang.Exception =>  None }

	}

	
	def dayDiff( d1 : Any , d2 : Any ) : Option[Long] = {
		try{
			Some( (java.sql.Date.valueOf( d1.toString ) .getTime - java.sql.Date.valueOf( d2.toString ) .getTime ) / (1000 * 24 * 60 * 60))
		
		}
		catch{  case p : java.lang.NullPointerException => None 
			case e : java.lang.Exception => None }
	}

	def max_SMU( a : Double , b : Double , c : Double  ) : Option[Double] = { 
		if(List( a , b , c  ).max == 0 )
			None
		else 
			Some(List( a , b , c  ).max)
   	}
   	
   	
   	def evaluteCumulativeERC ( sc : org.apache.spark.SparkContext  , sqlContext : org.apache.spark.sql.SQLContext , payload_Exception_Events_Fluid_variables_TBFC : org.apache.spark.sql.SchemaRDD  ) : org.apache.spark.sql.SchemaRDD = {
		
		
		import sqlContext._ 
		import sqlContext.createSchemaRDD
		
		payload_Exception_Events_Fluid_variables_TBFC.registerTempTable("payload_Exception_Events_Fluid_variables_TBFC")
		
		val cumulativeFile = sc.textFile("/home/amjath/Sriky/ERC all Data/Inputdata And Outputs/Cumulative/cumulative.csv")
		val cumulativeWithOutHeader = dropHeader( cumulativeFile )

		val machineStartDates = sc.textFile("/home/amjath/Sriky/ERC all Data/Inputdata And Outputs/Cumulative/machine.csv")
		
		val headerList = cumulativeFile.take(1)(0).split(",").toList
		val indexOfSerial = headerList.indexOf("SERIAL_NUMBER")
		val indexOfVIMS = headerList.indexOf("VIMSTIME")
		val indexOfSMU = headerList.indexOf("SMU")
		val indexOfCUMID = headerList.indexOf("CUMID")
		val indexOfDescription = headerList.indexOf("DESCRIPTION")
		val indexOfDataValue = headerList.indexOf("DATAVALUE")
		val indexOfErrorCount = headerList.indexOf("ERRORCOUNT")
		val indexOfUnitId = headerList.indexOf("UNIT_ID")
		val indexOfScaleFactor = headerList.indexOf("SCALEFACTOR")
		val indexOfDate = headerList.indexOf("date") 			
		
		
		
		val idleCumulative = cumulativeWithOutHeader.filter( x => x.split(",")(indexOfDescription).contains("Idle") &&  ! x.split(",")(indexOfDescription).contains("Fuel")  )

		val estTotalFuelCumulative = cumulativeWithOutHeader.filter( x => x.split(",")(indexOfDescription).contains("Est Total Fuel Used") )

		val startsCumulative = cumulativeWithOutHeader.filter( x => x.split(",")(indexOfDescription).contains("Starts") )


		val mappedIdleCumulative = idleCumulative.map( line =>{ val word = line.split(",") 
								Cumulative_class( getString(word(indexOfSerial)) ,  sqlTimestamp( word(indexOfVIMS) ) , getDouble( word(indexOfSMU)) ,getDouble( word(indexOfCUMID)) , getString(word(indexOfDescription))  ,getDouble( word(indexOfDataValue)) ,getDouble( word(indexOfErrorCount)) ,getDouble( word(indexOfUnitId)) , getDouble( word(indexOfScaleFactor)) , sqlDate(word(indexOfDate))   )
							    }
						  )


		val mappedEstTotalCumulative = estTotalFuelCumulative.map( line =>{ val word = line.split(",") 
								Cumulative_class( getString(word(indexOfSerial)) ,  sqlTimestamp( word(indexOfVIMS) ) , getDouble( word(indexOfSMU)) ,getDouble( word(indexOfCUMID)) , getString(word(indexOfDescription))  ,getDouble( word(indexOfDataValue)) ,getDouble( word(indexOfErrorCount)) ,getDouble( word(indexOfUnitId)) , getDouble( word(indexOfScaleFactor)) , sqlDate(word(indexOfDate))   )
							    }
						  )

		val mappedStartsCumulative = startsCumulative.map( line =>{ val word = line.split(",") 
								Cumulative_class( getString(word(indexOfSerial)) ,  sqlTimestamp( word(indexOfVIMS) ) , getDouble( word(indexOfSMU)) ,getDouble( word(indexOfCUMID)) , getString(word(indexOfDescription))  ,getDouble( word(indexOfDataValue)) ,getDouble( word(indexOfErrorCount)) ,getDouble( word(indexOfUnitId)) , getDouble( word(indexOfScaleFactor)) , sqlDate(word(indexOfDate))   )
							    }
						  )	


		


		mappedIdleCumulative.registerTempTable("mappedIdleCumulative")

		val orderedIdleCumulative = sqlContext.sql( "SELECT * FROM mappedIdleCumulative ORDER BY Serial , VIMS " ).zipWithIndex.
			    flatMap( x => { (0 to 1 ).map( i =>  (x._2 - i , ( x._2 , x._1)  ) )  } ).
			    combineByKey( value => List[(Long , org.apache.spark.sql.Row ) ](value ) , ( aggr : List[(Long , org.apache.spark.sql.Row ) ] , value : (Long , org.apache.spark.sql.Row )  ) => aggr ::: List[(Long , org.apache.spark.sql.Row ) ]( value ) , ( aggr1 : List[(Long , org.apache.spark.sql.Row ) ] , aggr2 : List[(Long , org.apache.spark.sql.Row ) ] ) => aggr1 ::: aggr2 ).
mapValues( x => x.sortWith( ( p, q ) => p._1 < q._1 ) ).
mapValues(  x => { 
		if( x.length == 1 )
			( x(0)._1 , x(0)._2 , None , None )
		else if( x(0)._2(0) == x(1)._2(0)  )
			( x(1)._1 , x(1)._2 , Some(x(1)._2(2).toString.toDouble - x(0)._2(2).toString.toDouble) , Some(x(1)._2(5).toString.toDouble - x(0)._2(5).toString.toDouble  ) )
		else
		       (  x(1)._1 , x(1)._2 , None , None  ) 


  }  ).filter( x => x._1 != x._2._1 ).sortBy( _._1 ).
   map( x => ( x._2._2 , x._2._3.getOrElse("None") , x._2._4.getOrElse("None") ) ).
  map( x => (  getString( x._1(0) ) , mySqlDate( x._1(1) ) , getDouble( x._1(2) ) , getDouble( x._2 ) , getDouble( x._3)  )    )




	mappedEstTotalCumulative.registerTempTable("mappedEstTotalCumulative")

	val orderedEstTotalCumulative = sqlContext.sql( "SELECT * FROM mappedEstTotalCumulative ORDER BY Serial , VIMS " ).zipWithIndex.
			    flatMap( x => { (0 to 1 ).map( i =>  (x._2 - i , ( x._2 , x._1)  ) )  } ).
			    combineByKey( value => List[(Long , org.apache.spark.sql.Row ) ](value ) , ( aggr : List[(Long , org.apache.spark.sql.Row ) ] , value : (Long , org.apache.spark.sql.Row )  ) => aggr ::: List[(Long , org.apache.spark.sql.Row ) ]( value ) , ( aggr1 : List[(Long , org.apache.spark.sql.Row ) ] , aggr2 : List[(Long , org.apache.spark.sql.Row ) ] ) => aggr1 ::: aggr2 ).
mapValues( x => x.sortWith( ( p, q ) => p._1 < q._1 ) ).
mapValues(  x => { 
		if( x.length == 1 )
			( x(0)._1 , x(0)._2 , None , None )
		else if( x(0)._2(0) == x(1)._2(0)  )
			( x(1)._1 , x(1)._2 , Some(x(1)._2(2).toString.toDouble - x(0)._2(2).toString.toDouble) , Some(x(1)._2(5).toString.toDouble - x(0)._2(5).toString.toDouble  ) )
		else
		       (  x(1)._1 , x(1)._2 , None , None  ) 
  }  ).filter( x => x._1 != x._2._1 ).sortBy( _._1 ).
  map( x => ( x._2._2 , x._2._3.getOrElse("None") , x._2._4.getOrElse("None") ) ).
  map( x => (  getString( x._1(0) ) , mySqlDate( x._1(1) ) , getDouble( x._1(2) ) , getDouble( x._2 ) , getDouble( x._3)  )    )




	mappedStartsCumulative.registerTempTable("mappedStartsCumulative")

	val orderedStartsCumulative = sqlContext.sql( "SELECT * FROM mappedStartsCumulative ORDER BY Serial , VIMS " ).zipWithIndex.
			    flatMap( x => { (0 to 1 ).map( i =>  (x._2 - i , ( x._2 , x._1)  ) )  } ).
			    combineByKey( value => List[(Long , org.apache.spark.sql.Row ) ](value ) , ( aggr : List[(Long , org.apache.spark.sql.Row ) ] , value : (Long , org.apache.spark.sql.Row )  ) => aggr ::: List[(Long , org.apache.spark.sql.Row ) ]( value ) , ( aggr1 : List[(Long , org.apache.spark.sql.Row ) ] , aggr2 : List[(Long , org.apache.spark.sql.Row ) ] ) => aggr1 ::: aggr2 ).
mapValues( x => x.sortWith( ( p, q ) => p._1 < q._1 ) ).
mapValues(  x => { 
		if( x.length == 1 )
			( x(0)._1 , x(0)._2 , None , None )
		else if( x(0)._2(0) == x(1)._2(0)  )
			( x(1)._1 , x(1)._2 , Some(x(1)._2(2).toString.toDouble - x(0)._2(2).toString.toDouble) , Some(x(1)._2(5).toString.toDouble - x(0)._2(5).toString.toDouble  ) )
		else
		       (  x(1)._1 , x(1)._2 , None , None  ) 
}  ).filter( x => x._1 != x._2._1 ).sortBy( _._1 ).
	map( x => ( x._2._2 , x._2._3.getOrElse("None") , x._2._4.getOrElse("None") ) ).
        map( x => (  getString( x._1(0) ) , mySqlDate( x._1(1) ) , getDouble( x._1(2) ) , getDouble( x._2 ) , getDouble( x._3)  )    )




	
	val IdleSchema = orderedIdleCumulative.map( x => CumulativeIdle( x._1 , x._2 , x._3 , x._4 , x._5 ) )

	val ESTSchema = orderedEstTotalCumulative.map( x => CumulativeEST( x._1 , x._2 , x._3 , x._4 , x._5 ) )

	val StartsSchema = orderedStartsCumulative.map( x => CumulativeStarts( x._1 , x._2 , x._3 , x._4 , x._5 ) )

	IdleSchema.registerTempTable("IdleSchema")

	ESTSchema.registerTempTable("ESTSchema")

	StartsSchema.registerTempTable("StartsSchema")


	val IdleSummary = sqlContext.sql("SELECT SerialIdle , VIMS_Idle , max(SMU_Idle) , sum(deltaSMUIdle) , sum(DeltaIdleTime ) , sum(deltaSMUIdle) / sum(DeltaIdleTime )  As SMUEngineIdleRatio  FROM IdleSchema GROUP BY SerialIdle , VIMS_Idle ORDER BY SerialIdle , VIMS_Idle ")


	val ESTSummary = sqlContext.sql("SELECT SerialEST , VIMS_EST , max(SMU_EST) , sum(deltaSMUEST) , sum(DeltaESTTime) , sum(DeltaESTTime) / sum(deltaSMUEST)  As LitrePerHour FROM ESTSchema GROUP BY SerialEST , VIMS_EST ORDER BY SerialEST , VIMS_EST ")

	val StartsSummary = sqlContext.sql("SELECT SerialStarts , VIMS_Starts , max(SMU_Starts) , sum(deltaSMUStarts) , sum(DeltaStartsTime) FROM StartsSchema GROUP BY SerialStarts , VIMS_Starts ORDER BY SerialStarts , VIMS_Starts ")	


	
	val lagIdleSummary = IdleSummary.zipWithIndex.flatMap{ x => (0 to 1 ).map(i => ( x._2 - i , (x._2 , x._1 )  ) ) }.
				combineByKey( value => List[(Long , org.apache.spark.sql.Row ) ](value ) , ( aggr : List[(Long , org.apache.spark.sql.Row ) ] , value : (Long , org.apache.spark.sql.Row )  ) => aggr ::: List[(Long , org.apache.spark.sql.Row ) ]( value ) , ( aggr1 : List[(Long , org.apache.spark.sql.Row ) ] , aggr2 : List[(Long , org.apache.spark.sql.Row ) ] ) => aggr1 ::: aggr2 ).
mapValues( x => x.sortWith((p,q) => p._1 < q._1 ) ).
mapValues( x => { 
		if( x.length == 1 ) 
			( x(0)._1 ,x(0)._2 , None )
		else if( x(0)._2(0) == x(1)._2(0) )
			(  x(1)._1,x(1)._2 , dayDiff( x(1)._2(1) , x(0)._2(1) ) )
		else
			( x(1)._1 ,x(1)._2 , None )	

  }).filter( x => x._1 != x._2._1 ).map( x => ( x._2._2 , x._2._3 )).map( x => {

	val Avg_SMU = try {   Some( x._1(3).toString.toDouble / x._2.get )  } 
			catch { case e : java.util.NoSuchElementException => None 
				case e : java.lang.Exception => None }
	val Avg_Value = try { Some( x._1(4).toString.toDouble / x._2.get )  } catch {
						case e : java.util.NoSuchElementException => None
						case e : java.lang.Exception => None }
	val avg_value_ = try {  if( Avg_Value.get < 0  || Avg_Value.get > Avg_SMU.get  ) { Some(0.0)  } else { Avg_Value }   } catch { 
		case e : java.lang.Exception => None 
		case e : java.util.NoSuchElementException => None }
	val bin1 =   try{   Some (( Math.floor(x._1(2).toString.toDouble / 100 ) * 100 ) + 100)    }catch{ 
			case e : java.util.NoSuchElementException => None				
			case e : java.lang.Exception => None }
		
		( x._1 , x._2 , Avg_SMU , Avg_Value , avg_value_  , bin1 )

        } )


	
val lagESTSummary = ESTSummary . zipWithIndex.flatMap{ x => (0 to 1 ).map(i => ( x._2 - i , (x._2 , x._1 )  ) ) }.
				combineByKey( value => List[(Long , org.apache.spark.sql.Row ) ](value ) , ( aggr : List[(Long , org.apache.spark.sql.Row ) ] , value : (Long , org.apache.spark.sql.Row )  ) => aggr ::: List[(Long , org.apache.spark.sql.Row ) ]( value ) , ( aggr1 : List[(Long , org.apache.spark.sql.Row ) ] , aggr2 : List[(Long , org.apache.spark.sql.Row ) ] ) => aggr1 ::: aggr2 ).
mapValues( x => x.sortWith((p,q) => p._1 < q._1 ) ).
mapValues( x => { 
		if( x.length == 1 ) 
			( x(0)._1 ,x(0)._2 , None )
		else if( x(0)._2(0) == x(1)._2(0) )
			(  x(1)._1,x(1)._2 , dayDiff( x(1)._2(1) , x(0)._2(1) ) )
		else
			( x(1)._1 ,x(1)._2 , None )	

  }).filter( x => x._1 != x._2._1 ).
    map( x => ( x._2._2 , x._2._3 ) ).
    map( x => { 			   
		val Avg_SMU = try { Some(x._1(3).toString.toDouble / x._2.get) } catch { case e : java.lang.Exception => None  }
		val Avg_Value = try { 
					if( (x._1(5).toString.toDouble / x._2.get) < 0 ||  ( x._1(5).toString.toDouble / x._2.get) > 100  )
						Some(0.0)
					else 
						 Some(x._1(5).toString.toDouble / x._2.get)
				    } catch { case e : java.lang.Exception => None }
		val bin2 = try{  Some ((Math.floor( x._1(2).toString.toDouble /100 )* 100 ) + 100 )   }
			   catch { case e : java.lang.Exception => None }
		( x._1 , x._2 , Avg_SMU , Avg_Value , bin2 )
	} )



	val lagStartsSummary = StartsSummary.zipWithIndex.flatMap{ x => (0 to 1 ).map(i => ( x._2 - i , (x._2 , x._1 )  ) ) }.
				combineByKey( value => List[(Long , org.apache.spark.sql.Row ) ](value ) , ( aggr : List[(Long , org.apache.spark.sql.Row ) ] , value : (Long , org.apache.spark.sql.Row )  ) => aggr ::: List[(Long , org.apache.spark.sql.Row ) ]( value ) , ( aggr1 : List[(Long , org.apache.spark.sql.Row ) ] , aggr2 : List[(Long , org.apache.spark.sql.Row ) ] ) => aggr1 ::: aggr2 ).
mapValues( x => x.sortWith((p,q) => p._1 < q._1 ) ).
mapValues( x => { 
		if( x.length == 1 ) 
			( x(0)._1 ,x(0)._2 , None )
		else if( x(0)._2(0) == x(1)._2(0) )
			(  x(1)._1,x(1)._2 , dayDiff( x(1)._2(1) , x(0)._2(1) ) )
		else
			( x(1)._1 ,x(1)._2 , None )	

  }).filter( x => x._1 != x._2._1 ).sortBy(_._1).map( x => ( x._2._2 , x._2._3 )).
 map( x => {
		val Avg_SMU = try{ Some(x._1(3).toString.toDouble / x._2.get) }catch{ 
		case p : java.util.NoSuchElementException =>  None
		case e : java.lang.Exception => None 
		case p : java.util.NoSuchElementException =>  None
		 }
		
		val Avg_Value = try { Some( (x._1(4).toString.toDouble / x._2.get) ) } catch { 
			
			case e : java.lang.Exception => None }
		val avg_value_ = try { if( Avg_Value == None  )
						None
					else if( Avg_Value.get < 0 ) 
						Some(0.0) 
					else 
						Some( ( Avg_Value.get ) ) 
				     }
				 catch {        case p : java.util.NoSuchElementException =>  None
						case p : java.lang.NullPointerException => None 
						case e : java.lang.Exception => None }
		
		val bin3 = try{  Some ((Math.floor( x._1(2).toString.toDouble /100 )* 100 ) + 100 )   }
			   catch { case e : java.lang.Exception => None
					case p : java.util.NoSuchElementException =>  None				
				 }
		( x._1 , x._2 , Avg_SMU , Avg_Value , avg_value_ , bin3 )
		 
    	  })

	val lagIdleSummarySchema = lagIdleSummary.map( x => IdleSummaryTemp( getString(x._1(0)) , sqlDate(x._1(1)) , getDouble( x._1(2) ) , getDouble( x._1(3) ) , getDouble( x._1(4) ) , getDouble( x._1(5) ) ,  x._2 , x._3 , x._4 , x._5 , x._6  )  ) 	

	
	val lagESTSummarySchema = lagESTSummary.map( x => ESTSummaryTemp( getString(x._1(0)) , sqlDate(x._1(1)) , getDouble( x._1(2) ) , getDouble( x._1(3) ) , getDouble( x._1(4) ) , getDouble( x._1(5) ) ,  x._2 , x._3 , x._4 , x._5 )  )

	
	val lagStartsSummarySchema  = lagStartsSummary.map( x => StartsSummaryTemp( getString(x._1(0)) , sqlDate(x._1(1)) , getDouble( x._1(2) ) , getDouble( x._1(3) ) , getDouble( x._1(4) ) , x._2  , x._3 , x._4 , x._5 , x._6) ) 


	
	val machineHead = machineStartDates.take(1)(0).split(",")

	val indexOfMachine = machineHead.indexOf("MACH_SER_NO")
	val indexOfStartDate = machineHead.indexOf("StartDate")
	val indexOfEndDate = machineHead.indexOf("EndDate")
	
	val machineDatesWithOutHeader = dropHeader(machineStartDates )


	val  dateTable = machineDatesWithOutHeader.map( x => { val w = x.split(",")
			  
	 /* println( w(1) + "    " + w(2) ) */
	val sqlStartDate = java.sql.Date.valueOf( w(indexOfStartDate).toString )
        val sqlEndDate = java.sql.Date.valueOf( w(indexOfEndDate).toString )
	var dateList = List[(java.sql.Date,java.sql.Date,java.sql.Date)]( (sqlStartDate, sqlEndDate , sqlStartDate) )
			  			   
	val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd")
	val c = java.util.Calendar.getInstance()
	c.setTime( sdf.parse(w(indexOfStartDate).toString) )
	c.add( java.util.Calendar.DATE , 1 )
	var dt = sdf.format(c.getTime())
	while( java.sql.Date.valueOf(dt)  before sqlEndDate ) {
				
	dateList = dateList ::: List[(java.sql.Date,java.sql.Date,java.sql.Date)]( (sqlStartDate, sqlEndDate,java.sql.Date.valueOf(dt)) )
				 c.add( java.util.Calendar.DATE , 1 )
				 dt = sdf.format(c.getTime())
			  } 
			  ( w(indexOfMachine) , dateList ::: List[(java.sql.Date,java.sql.Date,java.sql.Date)]((sqlStartDate, sqlEndDate,java.sql.Date.valueOf(dt)) ) )

		        } ) 

	val masterDates = dateTable.flatMapValues( x => x ).map( x => ( x._1 , x._2._1 , x._2._2 , x._2._3 ))	

	println( masterDates.count )
	
	val masterTableSchema = masterDates.map( x => masterTableTemp( x._1 , x._2 , x._3 , x._4 ) )


	masterTableSchema.registerTempTable("masterTable")

	lagIdleSummarySchema.registerTempTable( "lagIdleSummaryTable")

	lagESTSummarySchema.registerTempTable("lagESTSummaryTable")

	lagStartsSummarySchema.registerTempTable("lagStartsSummaryTable")


	val resJoin1 = sqlContext.sql("SELECT Serial, startDate , EndDate , dateValid , dateIdle , SMUIdle As MaxSMU1 , bin1 , avg_value_ As EngIdleTime FROM masterTable LEFT OUTER JOIN lagIdleSummaryTable ON Serial = SerialIdle AND dateValid = dateIdle    ORDER BY Serial,dateValid ")

	resJoin1.registerTempTable("resJoin1")

	val resJoin2 = sqlContext.sql("SELECT Serial, startDate , EndDate , dateValid , dateIdle , MaxSMU1 , bin1 , EngIdleTime , SMUEST As MaxSMU2 , bin2 , Avg_Value As FuelConsumed FROM resJoin1 LEFT OUTER JOIN lagESTSummaryTable ON Serial = SerialEST AND dateIdle = dateEST  ORDER BY Serial, dateValid ")

	resJoin2.registerTempTable("resJoin2")


	val resJoin3 = sqlContext.sql("SELECT  Serial, startDate , EndDate , dateValid , dateIdle , MaxSMU1 , bin1 , EngIdleTime ,MaxSMU2 , bin2 , FuelConsumed , SMUStarts As MaxSMU3 , avg_value_ As StartCount , bin3   FROM resJoin2 LEFT OUTER JOIN lagStartsSummaryTable ON Serial = SerialStarts AND dateValid = dateStarts ORDER BY Serial , dateValid ")

	resJoin3.registerTempTable("resJoin3")


	
	sqlContext.registerFunction( "max_SMU" , max_SMU( _ : Double , _ : Double , _ : Double   ) )


	val res = sqlContext.sql("SELECT  Serial,startDate , EndDate , dateValid , dateIdle , MaxSMU1 , bin1 , EngIdleTime ,MaxSMU2 , bin2 , FuelConsumed ,MaxSMU3 ,StartCount As EngStartCount , bin3, max_SMU( bin1 , bin2 , bin3)  AS bin    FROM resJoin3 ORDER BY Serial , startDate , EndDate , bin ")


/*
val res = sqlContext.sql("SELECT  Serial, startDate , EndDate , dateValid , dateIdle , MaxSMU1 , bin1 , EngIdleTime ,MaxSMU2 , bin2 , FuelConsumed ,MaxSMU3 ,StartCount As EngStartCount , bin3 , IF( MaxSMU1 >= MaxSMU2 , IF( MaxSMU1 > MaxSMU3 , MaxSMU1 , MaxSMU3 ) , IF( MaxSMU2 >= MaxSMU3 , MaxSMU2 , MaxSMU3  )  ) AS SMU , IF(bin1 > bin2 , IF(bin1 > bin3 , bin1 , bin3) , IF( bin2 > bin3 , bin2 , bin3 )   ) AS bin    FROM resJoin3 ORDER BY Serial , startDate , EndDate , bin ")
*/



	res.registerTempTable("res")

	val cumulativeResult = sqlContext.sql("SELECT Serial, startDate , EndDate , bin , sum(EngIdleTime) AS Max_EngineIdleTime  , sum(FuelConsumed) AS Max_FuelConsumed , sum(EngStartCount) Max_EngineStartCount   FROM res GROUP BY Serial, startDate , EndDate , bin ORDER BY Serial , bin " )

	
	cumulativeResult.registerTempTable("cumulativeResult")
	
	
	payload_Exception_Events_Fluid_variables_TBFC.registerTempTable("payload_Exception_Events_Fluid_variables_TBFC")

	val payload_Exception_Events_Fluid_variables_TBFC_Cumulative_Variable = sqlContext.sql("SELECT  MachSer , TID , Bin , minDate ,  maxDate ,  min_SMU ,  max_payload  , Avg_Sum_FCR_Ltrs_per_Kms_Tonnes ,  Avg_Sum_FCR_LtrsperMins_Tonnes ,  Avg_No_of_Trips , CriticalPercent_Trend , CriticalPercent_Event , CriticalPercent_Fluid , CriticalHours , CriticalDays  , SMU_Event ,  Sum_endOvrSpd1 ,  Sum_engPreLubeOvr1 , Sum_grdLvlShut1 ,  Sum_brkRealted1 ,  Sum_hiOilTemp1 , Sum_hiFuelWtr1 ,  Sum_idlStndRely1 ,  Sum_payloadOvr1 , Sum_TotalAbuseEvents , fluidBadSampleCount , fluidSampleCount , fluidSampleSeverity ,sum_fluidAddedQnty, fluidChangedCount, filterChangedCount , flilterChangeRunningCount ,  BadSamplePercentage ,  ActualEngineHours, FluidBadSamplePerSMu  ,  FluidSampleSeverityScore ,  FilterChangePerHours ,  CumulativeFilterChangePerHours ,  Max_EngineIdleTime ,  Max_FuelConsumed , Max_EngineStartCount    FROM    payload_Exception_Events_Fluid_variables_TBFC LEFT OUTER JOIN cumulativeResult ON MachSer = Serial AND Bin = bin  ") 

	payload_Exception_Events_Fluid_variables_TBFC_Cumulative_Variable.registerTempTable("payload_Exception_Events_Fluid_variables_TBFC_Cumulative_Variable")
	
	
	payload_Exception_Events_Fluid_variables_TBFC_Cumulative_Variable
		
	}  
	
	
}
