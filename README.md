# ADB

This repository consists of codes related to Hadoop Java Framework, MapReduce, Filtering Patterns, Implementation of Combiners And Partitioners.

The repository contains the following folders and subfolders

Assignment2 folder 
 - AssignmentQ2 : The NYSE files in HDFS to find the average price of stock_price_high values for each stock using MapReduce in a Java             
                  application.
 - Assignment2Q3 : MapReduce to find the number of males and females in the movielens dataset
 - Assignment2Q4 : MapReduce to find the number of movies rated by different users
 - Assignment2Q5 : MapReduce to find the number of times this website has been accessed by each IP Address using the attached log file
 
Assignment3 folder
 - Assignment3Q2 : MapReduce to implement PutMerge to merge the NYSE files in HDFS to find the average price of stock-price-high values 
                   for each stock using MapReduce on the single merged-file
 - A3Q4 : to implement a Writable object that stores some fields from the the NYSE dataset to find the date of the max stock_volume, the 
          date of the min stock_volume, the max stock_price_adj_close
 - A3Q5 : MapReduce to find the top 25 rated movies in the movieLens dataset
 
Assignment4 folder:
 - Assignment4Q2 : To determine the average stock_price_adj_close value by the year. An implementation in which a Reducer could be used as  
                   a Combiner
 - Assignment4Q3 : To determine the median and standard deviation of ratings per movie. Iterate through the given set of values and add 
                   each value to an in-memory list. The iteration also calculates a running sum and count.
 - Assignment4Q4 : To determine the median and standard deviation of ratings per movie using Memory-Conscious Median and Standard  
                   Deviation implementation. Combiner is used for optimization.
                   
 Assignment5 folder:
  - A5Q2 : MR Distinct pattern to find the unique IP addresses from the http_access.log file using Combiner optimization
  - A5Q3 : MR Partitioning pattern to move the records into groups by month using the http_access.log file using Combiner optimization
  - A5Q4 : MR Binning pattern to move the records into groups by hour using the http_access.log file using Combiner optimization
  - A5Q5 : MR Inner Join Pattern to join the files from BookCrossing Dataset
  - A5Q6 : the structured to hierarchical pattern to create an XML file in which Movie title would be the parent element, and tag would be 
           the child element
           
