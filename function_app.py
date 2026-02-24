import logging
import os

import azure.functions as func

from fetch_articles import main as fetch_articles_main


app = func.FunctionApp()


@app.route(
    route="http_trigger", auth_level=func.AuthLevel.FUNCTION, methods=["GET", "POST"]
)
def http_trigger(req):
    logging.info("HTTP trigger function received a request.")

    try:
        fetch_articles_main()
    except Exception as e:
        logging.error(f"Processing failed: {e}")
        return func.HttpResponse(f"Function execution failed: {e}", status_code=500)

    logging.info("HTTP trigger function executed successfully.")

    return func.HttpResponse("Function executed successfully.", status_code=200)
