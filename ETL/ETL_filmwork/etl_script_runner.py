import logging
import os
from datetime import datetime, timezone

from pkg.psql_extractor import PsqlExtractor
from pkg.state_saver import (
    GENRE_STATE_NAME,
    PERSONS_STATE_NAME,
    FILMWORK_STATE_NAME,
    StateSaver
)
from pkg.es_loader import ESLoader

logging.getLogger().addHandler(logging.StreamHandler())
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

MODEL_TO_CHECK = os.environ.get('MODEL_TO_CHECK')


class ETLProcessRunner:

    @classmethod
    def run(cls):
        logger.info('ETL process has been started.')

        state_saver = StateSaver()

        process_status = state_saver.get_working_process_status(MODEL_TO_CHECK)

        # if process is already running do not nothing
        if process_status:
            logger.info(f'Found already running process for {MODEL_TO_CHECK}')
            return

        # getting date of last update for model
        current_states = state_saver.get_states()
        if not any(current_states.values()):
            current_state = datetime.min.replace(tzinfo=timezone.utc)
            if MODEL_TO_CHECK in (GENRE_STATE_NAME, PERSONS_STATE_NAME,):
                # at first start update es only from fw table
                logger.info('Waiting for update from film_work table.')
                return
            else:
                # update state for all models if this is the first run with 'film_work' model
                states_to_update = (
                    GENRE_STATE_NAME, PERSONS_STATE_NAME, FILMWORK_STATE_NAME,
                )
        else:
            states_to_update = (MODEL_TO_CHECK, )
            try:
                current_state = datetime.strptime(
                    current_states[MODEL_TO_CHECK], "%Y-%m-%d %H:%M:%S.%f %z")
            except (ValueError, TypeError) as e:
                logger.error(f"Can't parse current datetime state.\n{e}")
                return

        state_saver.update_working_process_status(MODEL_TO_CHECK, 1)

        logger.info(f'Last updating date: {current_state}')

        psql_ex = PsqlExtractor(MODEL_TO_CHECK, current_state)
        es_loader = ESLoader()
        updated_fw_count = 0
        for fw_batch, last_time in psql_ex.get_updated_filmworks():
            es_loader.load_filmworks_batch(fw_batch)
            state_saver.save_states(states_to_update, last_time)
            updated_fw_count += len(fw_batch)

        if updated_fw_count:
            logger.info(
                f"{updated_fw_count} film_work(s) has(have) been inserted into ES."
            )

        state_saver.save_states(
            states_to_update, datetime.utcnow().replace(tzinfo=timezone.utc))
        psql_ex.close_psql_con()
        state_saver.update_working_process_status(MODEL_TO_CHECK, 0)

        logger.info('ETL process finished.')


if __name__ == "__main__":
    ETLProcessRunner.run()
