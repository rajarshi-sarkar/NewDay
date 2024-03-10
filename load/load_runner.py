
import logging

def load_runner(spark , config, movie_ratings, rating_based_selection, movies_raw, ratings_raw):
    '''This function is responsible for loading movie_ratings, rating_based_selection to output directory.
        parametes:
            spark: Spark session.
            config: Configuration dictionary.
            movie_ratings: Transformed dataframe containing movies rating
            rating_based_selection: Transformed dataframe containing user top ratings
            movies_raw: Raw data from movies input file
            ratings_raw: Raw data from ratings input file
    '''

    logger = logging.getLogger(__name__)
    logger.info('----Starting Load----')

    try:

        rating_based_selection.show()
        movie_ratings.show()
        movies_raw.show()
        ratings_raw.show()

        

        #Write movie_raw dataframe to output directory(overwrites old file) in parquet format
        movies_raw.write.mode('overwrite').parquet(
            f"{config.get('destination_dir')}/movie_raw/movie_raw.parquet"
            )
        
        #Write ratings_raw dataframe to output directory(overwrites old file) in parquet format
        ratings_raw.write.mode('overwrite').parquet(
            f"{config.get('destination_dir')}/ratings_raw/ratings_raw.parquet"
            )

        #Write movie_ratings dataframe to output directory(overwrites old file) in parquet format
        movie_ratings.write.mode('overwrite').parquet(
            f"{config.get('destination_dir')}/movie_ratings/movie_ratings.parquet"
            )

        #Write rating_based_selection dataframe to output directory(overwrites old file) in parquet format
        rating_based_selection.write.mode('overwrite').parquet(
            f"{config.get('destination_dir')}/rating_based_selection/rating_based_selection.parquet"
            )
        
        logger.info("Successfully loaded data.")

    except Exception as e:
        logger.error(f"Error encountered::{str(e)}")

    logger.info('----Finishing load----')
   