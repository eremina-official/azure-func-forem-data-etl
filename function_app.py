import logging
import azure.functions as func
import os
from fetch_articles import main
from fetch_articles_backfill import main_fetch_backfill

schedule = os.getenv("FETCH_TIMER_SCHEDULE", "0 0 0 * * *")


app = func.FunctionApp()

@app.timer_trigger(schedule=schedule, arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    
    backfill_timestamp = os.getenv("BACKFILL_MODE", "")

    if backfill_timestamp:
        logging.info(f"Triggering backfill for timestamp {backfill_timestamp}")
        main_fetch_backfill(backfill_timestamp)
        logging.info("Backfill completed. Consider clearing BACKFILL_MODE env variable.")
    else:
        logging.info("Triggering normal fetch.")
        main()

    logging.info('Python timer trigger function executed.')