This is the implementation of MP4. 

When you are using the datasets with Crane System, be sure that the dataset should be in a good format(i.e words are seperated by spaces...)

Example:

When you run crane.py, you can try run this command offline(when processed by one vm):

start calgary_access_log.txt calgary

This will output a file called calgary1_stat.txt and it will contain the number of each file format in calgary_access_log.txt

===START THE CRANE===

Assuming running in MP4/ folder
`python3 crane.py`



===SUPPORTED COMMAND===

* start <file_name> <file_mark>: start processing stream on that file

* join: send request to introducer for join

* lm: list the membership list

* print: print the processing result

* file_result <output_file>: store the processing result on this node to disk

  

===START THE SPARK STREAMING===

Assuming running in MP4/ folder
`python3 spark.py`



===SUPPORTED COMMAND===

- start <file_name>: start processing on that file
- Print: print the processing result
- File_result  <output_file>: store the processing result on this node to disk



Contact us: {zli104, kaiqi3 }@illinois.edu

