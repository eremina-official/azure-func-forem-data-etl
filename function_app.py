import logging
import azure.functions as func
import os
from fetch_articles import main

schedule = os.getenv("FETCH_TIMER_SCHEDULE", "0 0 0 * * *")


app = func.FunctionApp()

@app.timer_trigger(schedule=schedule, arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    
    main()

    logging.info('Python timer trigger function executed.')