1. Introduction to folders
code folder for storing the algorithm program and test data.
data folder in the code folder for stroing all test data
outlets folder in the data folder for storing the outlets information of all sub flow direction matrixes 
basin folder in the data folder for storing the SFR (sub first-round) watershed result and their eight adjacent tile information
finalresult folder in the data folder for storing the final result table and final watershed delineation result

2. Description of code files
Our program needs to run in the following order:1.run outlets.py 2.run subbasin.py 3.run unionbasin.py 4.run createresult.py
outlets.py is used to calculated outlets information of all sub flow direction matrixes
subbasin.py is used to calculate SFR watersheds result and SFR watersheds information of their eight adjacent flow direction matrixes 
unionbasin.py is used to calculated the final result table, which will be used for relabeling the final watershed ID
createresult.py is used to relabeling the final watershed ID based on the final result table and create these final watershed delineation reaster files

The program is written in python environment.
Notably, this program need be interpreted by py 3.X and need the arcpy packpage(3.X).
The arcpy packpage(3.X) is a part of arcgis pro product.


