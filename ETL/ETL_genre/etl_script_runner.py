import logging
from datetime import datetime, timezone

from pkg.psql_extractor import PsqlExtractor
from pkg.state_saver import StateSaver
from pkg.es_loader import ESLoader

logging.getLogger().addHandler(logging.StreamHandler())
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


class ETLProcessRunner:

    @classmethod
    def run(cls):
        logger.info('Genre-ETL process has been started.')

        state_saver = StateSaver()

        process_status = state_saver.get_working_process_status()

        # if process is already running do not nothing
        if process_status:
            logger.info(f'Found already running genre-etl process.')
            return
        else:
            state_saver.update_working_process_status(1)

        # getting date of last update for genres
        current_state = state_saver.get_state()

        logger.info(f'Last updating date: {current_state}')

        psql_ex = PsqlExtractor(current_state)
        es_loader = ESLoader()

        es_loader.check_index()

        updated_genres_count = 0
        for genres_batch, last_time in psql_ex.get_updated_genres():
            es_loader.load_genres_batch(genres_batch)
            state_saver.save_state(last_time)
            updated_genres_count += len(genres_batch)

        if updated_genres_count:
            logger.info(
                f"{updated_genres_count} genre(s) has(have) been inserted into ES."
            )

        state_saver.save_state(datetime.utcnow().replace(tzinfo=timezone.utc))
        psql_ex.close_psql_con()
        state_saver.update_working_process_status(0)

        logger.info('genre-ETL process finished.')


if __name__ == "__main__":
    ETLProcessRunner.run()
