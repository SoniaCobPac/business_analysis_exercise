
## AVROS
# This file includes all Avros' functionalities

## ACCESS

import os 
import sys

# Function to access previous folders. Required to import funcions from the folder 'mining_data'
def route (steps):
    """
    This function appends the route of the file to the sys path
    to be able to import files from previous foders.

    Args: 
        Steps: Number of folders to go up to the required folder as integer
    """
    route = os.path.abspath(__file__)
    
    for i in range(steps):
        route = os.path.dirname(route)
    sys.path.append(route)
    return route


## LIBRARIES
route(2)
import utils.mining_data as md 

# Schemas are specified in the subfolder 'avro_schema' in the folder 'data'

avsc_path = route(3) + os.sep + "data" + os.sep + "avro_schema" + os.sep
save_path = route(3) + os.sep + "reports" + os.sep 
# In this case:
dict_path = save_path


## WRITE DATA FILES

# Question 1: Median and p95 opening time during the week, by postal code, city, and state triplet.
md.write_avro(avsc_path, save_path, dict_path, "q1_opening")

# Question 2: Median and p95 closing time during the week, by postal code, city, and state triplet.
md.write_avro(avsc_path, save_path, dict_path, "q2_closing")

# Question 3: The number of businesses that are open past 21:00, by city and state pair.
md.write_avro(avsc_path, save_path, dict_path, "q3_open_late")

# Question 4: For each postal code, city and state, the business with the highest number of “cool” reviews that aren't open on Sunday.
md.write_avro(avsc_path, save_path, dict_path, "q4_cool_votes")


## READ DATA FILES

# Check to ensure AVRO files are readable:
md.read_avro(save_path, "q3_open_late")