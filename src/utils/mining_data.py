# Import the necessary libraries
import os 
import sys

import json 

from pyspark.sql import functions as f
from pyspark.sql.functions import regexp_replace, col, percentile_approx, lit, explode

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


# Functions
def route (steps):
    """
    This function appends the route of the file to the sys path
    to be able to import files from previous folders.

    Args: 
        Steps(int): Number of folders to go up to the required folder 
    """
    file_path = os.getcwd()

    for i in range(steps):
        file_path = os.path.dirname(file_path)
    sys.path.append(file_path)

    return file_path


def change_time(dataframe, column_range):
    """
    Replace two points in a string for one point, 
    to then be able to convert the string to float.

    Args:
        Dataframe: Dataframe where info is contained
        Column_range: Columns where strings to changed are contained
    """
    for c in column_range:
        try:
            dataframe = dataframe.withColumn(c, regexp_replace(c, ":", "."))
        except Exception as error:
            print("Error:", error)

    return dataframe


def split_hours(dataframe, columns, time_to_check):
    """
        Given the opening and closing hours in the same string, 
        this function splits them into different lists that then are saved in different columns.

        Args:
            Dataframe: Dataframe with hours info
            Columns: Columns of the dataframe where hours info is saved
            Time_to_check: opening/closing string depending on the business hours to check
        """
    opening = []
    closing = []
    for i, day in enumerate(columns):
        try:
            opening = [f.split(day, '-')[0]] + opening
            closing = [f.split(day, '-')[1]] + closing
        except Exception as error:
            print("Error:", error)
        
    # list containing week-days is reversed to match order of items saved in opening and closing
    week = columns
    week.reverse() 
    for i, elem in enumerate(week):
        if time_to_check == "opening":
            dataframe = dataframe.withColumn(elem, opening[i])
        elif time_to_check == "closing":
            dataframe = dataframe.withColumn(elem, closing[i])
        else:
            print("Please enter either 'opening' or 'closing' as 'time_to_check' attribute")
            break

    return dataframe


def convert_to_num(columns, dataframe, type="float"):
    """
    Change the dataframe column data type to float or int type.

    Args:
        Dataframe: Dataframe where info is saved
        Columns: Columns of the dataframe to convert
    """
    for c in columns:
        if type == "float" or type == "int":
            dataframe = dataframe.withColumn(c, col(c).cast(type))
        else:
            print("Please enter either 'float' or 'int' as 'type' attribute")
            break

    return dataframe


def calc_percentile(dataframe, column_name, percentage, new_column_name, *groupingby):
    """
    Calculate the approximate percentile of all days of the week and return it in a new dataframe.

    Args:
        Dataframe: Dataframe where info is saved
        Column_name: List of numeric columns (string) to calculate the percentile
        Percentage: Percentage (int) to calculate
        New_column_name: List of new column names to save the calculated percentile
        *groupingby: Columns name to group by    
    """
    mon = dataframe.groupby(*groupingby).agg(percentile_approx(column_name[0], percentage, lit(1000000)).alias(f"{new_column_name[0][:3]}_p{percentage}"))
    tues = dataframe.groupby(*groupingby).agg(percentile_approx(column_name[1], percentage, lit(1000000)).alias(f"{new_column_name[1][:3]}_p{percentage}"))
    wed = dataframe.groupby(*groupingby).agg(percentile_approx(column_name[2], percentage, lit(1000000)).alias(f"{new_column_name[2][:3]}_p{percentage}"))
    thurs = dataframe.groupby(*groupingby).agg(percentile_approx(column_name[3], percentage, lit(1000000)).alias(f"{new_column_name[3][:3]}_p{percentage}"))
    frid = dataframe.groupby(*groupingby).agg(percentile_approx(column_name[4], percentage, lit(1000000)).alias(f"{new_column_name[4][:3]}_p{percentage}"))
    sat = dataframe.groupby(*groupingby).agg(percentile_approx(column_name[5], percentage, lit(1000000)).alias(f"{new_column_name[5][:3]}_p{percentage}"))
    sun = dataframe.groupby(*groupingby).agg(percentile_approx(column_name[6], percentage, lit(1000000)).alias(f"{new_column_name[6][:3]}_p{percentage}"))

    week = mon.join(tues, *groupingby).join(wed, *groupingby).join(thurs, *groupingby).join(frid, *groupingby)\
    .join(sat, *groupingby).join(sun, *groupingby)

    return week


def explode_column(dataframe, column_name, new_column_name):
    """
    Returns a new row for each element in a given array.

    Args:
        Dataframe: Dataframe where info is saved
        Column_name: Column name of the array to explode
        New_column_name: Name of the new column with the exploded data
    """
    dataframe = dataframe.withColumn(new_column_name, explode(dataframe[column_name]))

    return dataframe


def total_percentile(dataframe, columns, percentage, percentile_name, *groupingby):
    """
    Calculate the approximate percentile of numbers in an array in one column.

    Args:
        Dataframe: Dataframe where info is saved
        Columns: Columns to extract data to calculate the percentile
        Percentage: Percentage (int) to calculate
        Percentile_name: Name of the new column depending on the percentage calculated
        *groupingby: Columns' name to group by    
    """
    res = []
    for elem in columns:
        res = [f.col(elem)] + res

    # Array of total hours is saved in another column
    total_per = dataframe.withColumn("array", f.array(res))

    # The array containing hours is exploded to easily calculate the percentile
    total_per = explode_column(total_per, "array", "Total hours")

    # The total percentile is calculated grouping the business characteristic as requested
    total_per = total_per.groupBy(*groupingby).agg(percentile_approx("Total hours", percentage, lit(1000000)).alias(percentile_name))

    return total_per


def filtering_hours(dataframe, week, hours=[0.00, 10.00]):
    """
    Filter businesses if they are opened between the specified hour range at least one day per week.

    Args:
        Dataframe: Dataframe where info is saved
        Week: List of week-days
        Hours: Specific hour range to filter businesses
    """
    df = dataframe.filter((dataframe[week[0]] > hours[0]) & (dataframe[week[0]] < hours[1])\
                                | (dataframe[week[1]] > hours[0]) & (dataframe[week[1]] < hours[1])\
                                | (dataframe[week[2]] > hours[0]) & (dataframe[week[2]] < hours[1])\
                                | (dataframe[week[3]] > hours[0]) & (dataframe[week[3]] < hours[1])\
                                | (dataframe[week[4]] > hours[0]) & (dataframe[week[4]] < hours[1])\
                                | (dataframe[week[5]] > hours[0]) & (dataframe[week[5]] < hours[1])\
                                | (dataframe[week[6]] > hours[0]) & (dataframe[week[6]] < hours[1]))
    return df


def df_to_csv(dataframe, path_file, filename):
    """
    Convert a dataframe to CSV file. 

    Args:
        Dataframe: Dataframe to convert to csv
        Path_file: Path to save the csv file
        Filename: Name (string) of the created csv file
    """
    return dataframe.toPandas().to_csv(path_file + f"{filename}.csv", index=False)


def df_to_json(dataframe, path_file, filename):
    """
    Convert a dataframe to json file. 

    Args:
        Dataframe: Dataframe to convert to json
        Path_file: Path to save the json file
        Filename: Name (string) of the created json file
    """
    return dataframe.toPandas().to_json(path_file + f"{filename}.json", orient="records")


def read_json(path, filename):
    """
    Read a json and return a object created from it.
    
    Args:
        Path: Path where the json file is saved 
        Filename: Name (string) of the created json file
    """
    try:
        with open(path + f"{filename}.json", "r+", encoding="utf8") as outfile:
            json_readed = json.load(outfile)
        return json_readed
    except Exception as error:
        raise ValueError(error)


def write_avro(avsc_path, save_path, dict_path, filename):
    """
    This funcion serializes the given information to a data file on disk.

    Args:
        Avsc_path: Path where the AVSC file is saved 
        Save_path: Path to save the AVRO file
        Dict_path: Path where the json file is saved 
        Filename: Name (string) of the created json file
    """
    # Create Avro schema
    schema = avro.schema.parse(open(avsc_path + f"{filename}.avsc", "rb").read())

    # Write json to avro file
    writer = DataFileWriter(open(save_path + f"{filename}.avro", "wb"), DatumWriter(), schema)

    dictionary = read_json(dict_path, filename)

    for line in dictionary:
        try:
            writer.append(line)
        except Exception as error:
            raise ValueError(error)  
              
    writer.close()


def read_avro(save_path, filename):
    """
    This funcion read the serialized data file on disk and
    returns dicts corresponding to the items.

    Args:
        Save_path: Path where the AVRO file is saved
        Filename: Name (string) of the created AVRO file  
    """
    reader = DataFileReader(open(save_path + f"{filename}.avro", "rb"), DatumReader())
    for user in reader:
        try:
            print(user)
        except Exception as error:
            raise ValueError(error)  

    reader.close()

