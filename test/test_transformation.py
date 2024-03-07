# Below code perform unit testing on movie analysis job
import pandas as pd

from pyspark.sql import functions as F
from transform.transform_runner import user_choice, find_rating



def test_user_choice(spark):
    '''This function is responsible for testing user_choice function in transform module.
            parametes:
                spark: Spark session.
    '''
    #Movies dataframe for testing
    movies_raw_test = spark.createDataFrame(
        [(1, "Toy Story (1995)", "Adventure"),
         (2, "Jumanji (1995)", "Adventure"),
         (3, "Grumpier Old Men (1995)","Comedy"),
         (4, "Waiting to Exhale (1995)", "Comedy"),
         (5, "Heat (1995)", "Action|Crime")],
        ['MovieID', 'Title', 'Genres']
    )
    
    #Ratings dataframe for testing
    ratings_raw_test = spark.createDataFrame(
        [(1, 1, 5, 978294260),
         (1, 2, 5, 978294261),
         (1, 3, 5, 978294262),
         (1, 4, 4, 978294263),
         (1, 5, 3, 978294264),
         (2, 1, 2, 978294265),
         (2, 2, 3, 978294266),
         (2, 3, 5, 978294267),
         (2, 4, 5, 978294268),
         (2, 5, 4, 978294269)],
        [
            'UserID', 
            'MovieID', 
            'Rating', 
            'Timestamp']
    )

    # Dataframe containing expectedresult
    expected_result = spark.createDataFrame(
        [(1, "Toy Story (1995)"),
         (1, "Jumanji (1995)"),
         (1, "Grumpier Old Men (1995)"),
         (2, "Grumpier Old Men (1995)"),
         (2, "Waiting to Exhale (1995)"),
         (2, "Heat (1995)")],
        [
            'UserID', 
            'Title']
    ).sort(F.col('UserID'), F.col('Title')).toPandas()

    #Actual dataframe returned by transform module
    actual_result = user_choice(
        movies_raw_test, 
        ratings_raw_test
        ).sort(
            F.col('UserID'),F.col('Title')
            ).toPandas()

    #Assert if actual result is equal to expected result
    pd.testing.assert_frame_equal(
        actual_result.reset_index(drop=True),
        expected_result.reset_index(drop=True)
        )
    
def test_find_rating(spark):
    '''This function is responsible for testing find_rating function in transform module.
            parametes:
                spark: Spark session.
    '''

    #Movies dataframe for testing
    movies_raw_test = spark.createDataFrame(
        [(1, "Toy Story (1995)", "Adventure"),
         (2, "Jumanji (1995)", "Adventure"),
         (3, "Grumpier Old Men (1995)","Comedy"),
         (4, "Waiting to Exhale (1995)", "Comedy"),
         (5, "Heat (1995)", "Action|Crime")],
        [
            'MovieID', 
            'Title', 
            'Genres']
    )

    #Ratings dataframe for testing
    ratings_raw_test = spark.createDataFrame(
        [(1, 1, 1, 978294260),
         (1, 2, 2, 978294261),
         (1, 3, 5, 978294262),
         (1, 4, 4, 978294263),
         (1, 5, 3, 978294264),
         (2, 1, 2, 978294265),
         (2, 2, 3, 978294266),
         (2, 3, 1, 978294267),
         (2, 4, 5, 978294268),
         (2, 5, 4, 978294269)],
        [
            'UserID', 
            'MovieID', 
            'Rating', 
            'Timestamp']
    )
    # Dataframe containing expectedresult
    expected_result = spark.createDataFrame(
        [(1, "Toy Story (1995)", "Adventure",2.0,1,2),
         (2, "Jumanji (1995)","Adventure",3.0,2,3),
         (3, "Grumpier Old Men (1995)", "Comedy",3.0,1,5),
         (4, "Waiting to Exhale (1995)", "Comedy",5.0,4,5),
         (5, "Heat (1995)", "Action|Crime",4.0,3,4)],
        ['MovieID', 'Title', 'Genres', 'avg', 'min', 'max']
    ).sort(
        F.col('MovieID'), 
        F.col('Title')).toPandas()

    #Actual dataframe returned by transform module 
    actual_result = find_rating(
        movies_raw_test, ratings_raw_test).sort(
            F.col('MovieID'),
            F.col('Title')).toPandas()
    
    #Assert if actual result is equal to expected result
    pd.testing.assert_frame_equal(
        actual_result.reset_index(drop=True),
        expected_result.reset_index(drop=True)
        )