
import logging

def extract_runner(spark, config) -> dict:
    '''This function is responsible for extracting raw data from input files.
        parametes:
            spark: Spark session.
            config: Configuration dictionary.
        return:
            Dictionary containing movies and ratings raw dataset
    '''

    logger = logging.getLogger(__name__)
    logger.info('----Starting Extract----')

    sep = "::"
    movies_schema = 'MovieID int,Title String,Genres String'
    ratings_schema = 'UserID int,MovieID int,Rating int,Timestamp long'

    try:
        # Read movies data from input file
        movies_raw = spark.read.csv(
            f"{config.get('source_dir')}/movies.dat",
            sep= sep,
            schema = movies_schema
            )
        logger.info("Successfully extracted movies file.")
    
        # Read ratings data from input file
        ratings_raw = spark.read.csv(
            f"{config.get('source_dir')}/ratings.dat",
            sep= sep,
            schema = ratings_schema
            )
        
        logger.info("Successfully extracted ratings file.")

    except Exception as e:
        logger.error(f"Error encountered::{str(e)}")

    logger.info('----Finishing Extract----')
    return(
            {
                "movies_raw": movies_raw,
                "ratings_raw": ratings_raw
            }
        )
