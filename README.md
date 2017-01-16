CAT Phase III 

    The aim of the project is to back port the work done in alteryx to make it work in spark.
## Environment: ##
* spark version : 1.2.0 
* scala version : 2.10.5
## Input file: ## 
* date.csv file which contains Machine Repair dates
* cumulative.csv file 
* payload.csv file
* events.csv file
## Ouput: ## 


CAT Phase III 


    The aim of the project is to back port the work done in alteryx to make it work in spark.

## Environment: ##

* spark version : 1.2.0

* scala version : 2.10.5

## Input file: ## 

* date.csv file which contains Machine Repair dates

* cumulative.csv file 

* payload.csv file

* events.csv file

## Ouput: ## 

It gives ouputs a csv file in specified path folder [ 5 th argument while running the jar]  , which contains a file with name part-00000 

It has data seperated by ','( comma) for five variables in following order 


MachineID,BinLevel,Sum_1.1.Overload,Sum_1.2.Overload,Avg_PAYLOADWEIGHT,Sum_Pyld.LdTrvSpd,Sum_TripTime 


Input needs to be given as commandline arguments and file paths need to be given above mentioned order of Input file , separated by space


Example Query to run   

pathOfSpark/spark-1.2.0/bin/spark-submit --class "catetl.Main" pathOfJar/catbackport_2.10-0.0.1.jar  file:///path/data/date.csv  file:///path/cumulative.csv file:///path/payload.csv  file:///path/events.csv  file:///path/result