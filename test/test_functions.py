# Import the necessary libraries
import os 
import sys

import json 

from pyspark.sql import functions as f
from pyspark.sql.functions import regexp_replace, col, percentile_approx, lit, explode


def split_hours(dataframe, columns, new_column_name1, new_column_name2):
    """
    Given the opening and closing hours in the same string, 
    this function splits them into different list that then are saved in different columns.

    Args:
        Dataframe: Dataframe with hours info
        Columns: Columns of the dataframe where hours info is saved
        New_column_name1: Name of the new column with the first part of the split data
        New_column_name2: Name of the new column with the second part of the split data
    """
    opening = []
    closing = []
    for day in columns:
        try:
            opening = [f.split(day, '-')[0]] + opening
            closing = [f.split(day, '-')[1]] + closing
        except Exception as error:
            print("Error:", error)
        
    dataframe = dataframe.withColumn(new_column_name1, f.array(opening))
    dataframe = dataframe.withColumn(new_column_name2, f.array(closing))
    
    return dataframe
    

def convert_to_float (columns, dataframe):
    """
    Change the dataframe column data type to float type.

    Args:
        Dataframe: Dataframe where info is saved
        Columns: Columns of the dataframe to convert
    """
    for c in columns:
        dataframe = dataframe.withColumn(c, col(c).cast("float"))

    return dataframe


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


def extract_data(dataframe, existing_column_name, elem_to_extract):
    """
    Extract elements from an array into new independent columns.

    Args:
        Dataframe: Dataframe where info is saved
        Existing_column_name: Name of the column containing the array of elements to extract
        Elem_to_extract: Number (int) of elements to extract
    """
    
    # As days were disorganised they are called 'Day1' etc rather than by they week-name to avoid mistakes
    for i in range(elem_to_extract):
        dataframe = dataframe.withColumn(f"Day{i}", dataframe[existing_column_name].getItem(i))

    return dataframe


def compare_hours(dataframe, columns, column_name, common_columns):
    """
    This function splits opening and closing hours of a day to then be able to compare them
    Args:
        Dataframe: Dataframe with hours info
        Columns: Columns of the dataframe where hours info is saved 
    """

    open_hours = split_hours(dataframe, columns, column_name[0]).withColumnRenamed(columns, f"{columns[:3]}_{column_name[0]}")
    close_hours = split_hours(dataframe, columns, column_name[1]).withColumnRenamed(columns,f"{columns[:3]}_{column_name[1]}")

    df = open_hours.join(close_hours, [common_columns[0], common_columns[1], common_columns[2]])
    df = df.select(col(common_columns[0]), col(common_columns[1]), col(common_columns[2]), col(f"{columns[:3]}_{column_name[0]}"), col(f"{columns[:3]}_{column_name[1]}"))
    
    return df


def past_midnight(dataframe, week, hours=[0.00, 10.00]):
    """
    Filter businesses if they are open between the specified hour range at least one day per week.
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


def past_nine(dataframe, week, hour=21.00):
    """
    Filter businesses if they are open after the specified hour at least one day per week.
    Args:
        Dataframe: Dataframe where info is saved
        Week: List of week-days
        Hour: Specific hour to filter businesses
    """
    df = dataframe.filter((dataframe[week[0]] > hour) | (dataframe[week[1]] > hour) | (dataframe[week[2]] > hour) | \
    (dataframe[week[3]] > hour) | (dataframe[week[4]] > hour) | (dataframe[week[5]] > hour) | (dataframe[week[6]] > hour))
    
    return df


