# beamprojecta


## SUMMARY
This project implements a realtime streaming pipeline with apache beam using the dataflow sdk. It is written in java and compiled with maven. The pipeline runs continously, checks every 5 minutes (processing time) for new data and aggregates results in hourly windows (event time).

## INPUT
As input json files are used, which cannot be nested and have to be formatted as one json object per row. Timestamps have to be format as ISO8601 with a "T" character separating date and time, and a "Z" character (for UTC) terminating the 3 digit milliseconds at the end. 

# Example Input file:
{"order_number": "AP-1", "service_type": "GET_PULSA", "driver_id": "driver-123", "customer_id": "customer-123", "service_area_name": "JARKARTA", "payment_type": "CREDITCARD", "status": "COMPLETED", "event_timestamp": "2018-09-22T12:02:05.114Z"}

We read these files row by row as an unbounded dataset, watching the input directory for changes every 10 seconds. In production, this input method can be replaced by reading directly from a message broker such as Kafka or Pub/Sub. 

## MAIN TRANSFORMATION CLASS: ParseJsonFn
After reading the input file row by row, each row is passed to the main transformation class ParseJsonFn. Here, the json encoded rows are parsed and each element is mapped to a member of a java class called GetMessage. Then, the three elements service_area_name, payment_type and status are concatenated and returned as a string. 

In this step we also extract the event timestamp from each json row and add it to each element we output. This enable us to use the event time later for hour window. 

## COUNTING OCCURENCE OF COMPOSITE KEYS
The concatenated strings from the previous step are used as composite keys. In this step, we count the number of occurences for each unique composite key. 
We use a fixed window of 1 hour based on event time and trigger writing of early results every 5 minutes. We allow data to arrive late for a maximum of 10 minutes.

Example output elements:
HCMC,PAYPAL,ACTIVE: 4
BANGKOK,GET_CASH,COMPLETED: 3

## OUTPUT
Next we turn each K/V pair of composite-key/count into a string to prepare for writing to the output file. Aggregate data is written according to the specified time window and trigger, with 1 shard per window. 

In production, the data can be directly written to a database instead of a file, such as BigQuery or Postgres. Because of the fast, low latency integration between DataFlow and BigQuery, and tried-and-tested dataflow connector for BigQuery, BigQuery would be the preferred option but options such as Posgres would certainly work well given the low volume nature of the aggregated data of this use case. When writing to a database, Date and hour would be extracted from the event timestamp and relevant fields in the database table would be populated. Upon firing of a trigger or on arrival of late data, counts for that date and hour of event time would be updated in the database.  

## RUNNING THE APPLICATION
The pipeline can be run locally with 

"mvn compile exec:java -Dexec.mainClass=org.dl_productions.BeamPipeA -Dexec.args="--inputFile=input/*.json --output=counts --streaming" -Pdirect-runner"


## TESTING THE APPLICATION
A unit test exists for the main transformation class ParseJsonFn. It can be run with 
"mvn test"

## NOTES
1. In the present implementation, the pipeline does not remove duplicates per order id. Thus, when two messages arrive for a given order id, one with status 'ACTIVE' and one with 'COMPLETE', both will be present in the aggregated data. As a result, questions such as 'How many orders were cancelled in Bangkok for Get_RIDE from 08:00-09:00' can be answered with the present implementation. However, a count of all orders for a given hourly window will likely exceed the total number of orders because of status changes for given order ids.

In case duplicates need to be removed, in future implementations, the following options would be viable
a) Do not allow duplicate order_id's within time window: Per order_id, always keep only the row with the most recent timestamp within a window. 
b) Do not allow duplicate order_id's across time window: Like a) but also do not allow duplicate order_id's across. 

Both a) and b) would necessitate buffering collection elements for comparison of order_id with elements that arrive later. This could result in increased memory usage of the application but should otherwise be feasible. 

## FILE GENERATOR
FileGenerator.py can be used to create test input files. These can be set in the future without problems. However care needs to be taken that no file has an event timestamp that is smaller than the current processing time when the pipeline is running. (Otherwise skew has to be set greater than 0). For this reason, by default the file generator adds 5 minutes to the timestamp of the first file generated. 






