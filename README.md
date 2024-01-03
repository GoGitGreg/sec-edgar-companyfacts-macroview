# sec-edgar-companyfacts-macroview

This code downloads the bulk zip file from the SECs website and aggregates the data into csv files based each data point.

Step 1:  Import Requirements
Step 2:  Download and Extract zip file
Step 3:  Iterate over each json file and compile the data files
Step 4:  Audit check to make sure code executed correctly

I've recognized two distinct data structures that are ideal for building your data model.  This one takes more of a Macro View, as data from each CIK is aggreated together based up on the data point.  So if I want to see Net Income for a specific period for all CIK's, I can do so with very little effort.  You would only need to locate the correct csv file, open in excel, create a pivot table, filter to desired time period or frame, and display the val by CIK.  For those unfamilar with this data source, you can also obtain a complete list of all CIKs and their Name from the same website.  If you are unfamilar with how Net Income is defined or any of the other data points, check out the Labels.csv file which will contain the description for all of them.

The main part I'm happy with is that the code will dynamically create the csv files for each data point it locates within the json file.  As of today there are 17,644 json files to iterate through.  Rougly 14.9 GB uncompressed.  After running the code for all files, it creates 13,096 csv files.  Roughly 8.88 GB uncompressed.

Please be aware that the website requires headers when making the request.  The code will prompt you for the "Individual or Entity Name Making the Request" and an "Email Address".
Please be aware that the code took a little over two hours on my machine to execute in full.  The code contained here limits the number of json files to iterate through to the first 100, which executed for me in 3 minutes
