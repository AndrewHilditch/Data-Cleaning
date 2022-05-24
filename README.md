# Data-Cleaning
This is a repository of the process required to clean data obtained from Network Rail's data feeds. This repository covers schedule data as well as berth movements data. The two data sets are then combined using two intermediate data sets so that an appended version of the berth movements can be obtained. This version can track arrivals at station berths allowing for the calculation of delay.

The berth data file supplied to this code should be in 24 hour format. The day in this format starts at 4:00am and continues until 4:00am the next day. This is a 24 hour day on the U.K. railway. If the data you are supplying does not cross the 4:00am boundary this requirement can be ignored.
