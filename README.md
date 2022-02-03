# ETl (Extract, Transform, Load) Scripts for Media Analytics

## Purpose:
These scripts pull raw data from our OLTP databases, transform that into data that can be used to generate insights, and then stored in our data warehouse. The scripts can be scheduled to run at regular intervals as well.

## Code Explanation:
1. utils.py 
   - Contains helper functions used to perform CRUD (Create, Read, Update, Delete) operations on the databases.

   - Contains transformation functions that are used by 2 or more modules.

2. etl_*.py
   - All etl_* files contain the code to run each module's ETL workflow independently. Some modules whose preprocessing is unique have those preprocessing functions defined in this script as well.

3. run.py
   - This file sets up a scheduler to run this pipeline every 5 minutes (default). The workflow consists of pulling data from the OLTP database, moving it to the annotator database, deleting it from the OLTP databases, transforming the pulled data and then storing the transformed data into a data warehouse.

   - These steps are performed for each individual module. 


## Code Configuration Files:
There are two JSON configuration files for the script. They are briefly described below:

1. dbconfig.json:
   - This file contains two broad settings:
     * Database settings: These allow you to set the client address, the names of the OLTP, annotator and Warehouse databases and finally the repetition interval of the scripts (in seconds)

     * Collection settings: These allow you to set the collection names for each individual module, for the OLTP and Annotator databases. The Warehouse collections are created automatically on the basis of channel names.

2. sched_config.json:
   - This file can be used to define scheduler settings, primarily the number of pool process executors and max job store instances.


## How To Run:
Simply execute run.py after making the appropriate changes to the configuration files. 

If you want to do a test run, create the three databases (OLTP, Annotator and DWH) and add any dummy data to the OLTP. Then execute the script (remember to keep time_filter False for this).
