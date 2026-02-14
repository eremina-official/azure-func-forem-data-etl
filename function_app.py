import logging
import azure.functions as func
import os
from fetch_articles import main


schedule = os.getenv("FETCH_TIMER_SCHEDULE", "0 0 0 * * *")
backfill_timestamp = os.getenv("BACKFILL_MODE", "")


app = func.FunctionApp()


@app.timer_trigger(
    schedule=schedule, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("The timer is past due!")

    logging.info("Triggering normal fetch.")

    main()

    logging.info("Python timer trigger function executed.")
