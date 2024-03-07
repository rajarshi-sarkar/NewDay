from pyspark.sql import functions as F
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
import logging

logger = logging.getLogger(__name__)

def find_rating(movies_raw, ratings_raw):
    '''This function is responsible for following transformation of raw data:
        Generating dataframe containing rating overview for each movie
        parametes:
            movies_raw: Raw movies dataframe.
            ratings_raw: Raw ratings dataframe.
        return:
            Dataframe
    '''
    try:
        # Get average, min and max rating for each movie id from ratings dataframe
        agg_ratings = ratings_raw.groupBy(
            'MovieID').agg(
                F.avg(
                    F.col('Rating')),
                    F.max(F.col('Rating')),
                    F.min(F.col('Rating'))).withColumnRenamed(
                        "avg(Rating)", 
                        "avg").withColumnRenamed(
                            "max(Rating)", 
                            "max").withColumnRenamed(
                                "min(Rating)", 
                                "min"
                                )
        
        # join movies dataframe to agg_ratings keeping all rows from movies df 
        movie_ratings_raw = movies_raw.join(
            agg_ratings, 
            movies_raw.MovieID == agg_ratings.MovieID,
            'left').drop(
                agg_ratings.MovieID
                )
        
         # fromat movie_ratings_tmp dataframe, round off average rating column
        movie_ratings = movie_ratings_raw.withColumn(
            'avg', F.round(
                movie_ratings_raw['avg'], 
                0)).select(
                    'MovieID', 
                    'Title', 
                    'Genres',
                    'avg', 
                    'min', 
                    'max'
                    )
        
    except Exception as e:
        logger.error(f"Error encountered::{str(e)}")
    
    return (movie_ratings)



def user_choice(movies_raw, ratings_raw):
    '''This function is responsible for following transformation of raw data:
        generating dataframe containing users choice(top 3 selection for each user).
        parametes:
            movies_raw: Raw movies dataframe.
            ratings_raw: Raw ratings dataframe.
        return:
            Dataframe
    '''

    try:
        # Choosing partition and order columns for implementing windows function
        windowSpec = Window.partitionBy('UserID').orderBy(F.col('Rating').desc())

        # Implement windows function to get top 3 movie selection for each user
        users_choice_temp = ratings_raw.withColumn(
            'row_number',
            row_number().over(windowSpec)).filter(
                F.col('row_number') <= 3).drop(
                    'Rating', 
                    'Timestamp', 
                    'row_number'
                    )

        # Join with movies data and fetch movie names
        users_choice = users_choice_temp.join(
            movies_raw,
            users_choice_temp.MovieID == movies_raw.MovieID,
            'left'
            ).select(
                users_choice_temp['UserID'],
                movies_raw['Title']
                ).orderBy(
                    F.col('UserID').asc()
                    )
        
    except Exception as e:
        logger.error(f"Error encountered::{str(e)}")

    return(users_choice)



def transform_runner(movies_raw, ratings_raw):
    '''This function is responsible for following transformation of raw data:
        Generating dataframe containing rating overview for each movie,
        generating dataframe containing users choice(top 3 selection for each user).
        parametes:
            movies_raw: Raw movies dataframe.
            ratings_raw: Raw ratings dataframe.
        return:
            Dictionary containing movie ratings and user top ratedmovies dataframes
    '''
    logger.info('----Starting transformation----')
    # join movies dataframe to agg_ratings keeping all rows from movies df 
    movie_ratings = find_rating(movies_raw, ratings_raw)

    rating_based_selection = user_choice(movies_raw, ratings_raw)



    logger.info('----Finishing transformation----')

    return(
            {
                "movie_ratings": movie_ratings,
                "rating_based_selection": rating_based_selection
            }
        )


