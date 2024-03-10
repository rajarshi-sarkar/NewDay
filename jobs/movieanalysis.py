from extract.extract_runner import extract_runner
from transform.transform_runner import transform_runner
from load.load_runner import load_runner


def run_job(spark, config):
    '''This function is responsible for executing ETL spark job.
        parametes:
            spark: Spark session.
            config: Configuration dictionary.
    '''

    # Run extract module
    raw_input = extract_runner(spark, config)

    movies_raw, ratings_raw = raw_input.get('movies_raw'), raw_input.get('ratings_raw')

    # Run transform module
    transformed_data = transform_runner(movies_raw, ratings_raw)

    movie_ratings, rating_based_selection = transformed_data.get('movie_ratings'),transformed_data.get('rating_based_selection')

    # Run load module
    load_runner(
        spark,
        config,
        movie_ratings,
        rating_based_selection,
        movies_raw,
        ratings_raw
        )