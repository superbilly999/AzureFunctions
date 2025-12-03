import logging
import os

import azure.functions as func
from azure.functions import TimerRequest
from azure.storage.blob import BlobServiceClient

# Create the FunctionApp object (new Python model)
app = func.FunctionApp()

# Timer trigger – runs at 03:00 every day (change schedule if you like)
@app.schedule(
    schedule="0 0 3 * * *",   # CRON: sec min hour day month day-of-week
    arg_name="mytimer",
    run_on_startup=False,
    use_monitor=True,
)
def copy_prod_to_dev(mytimer: TimerRequest) -> None:
    logging.info("CopyProdToDev timer function started")

    conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    src_container_name = os.environ["PROD_CONTAINER"]
    dst_container_name = os.environ["DEV_CONTAINER"]

    service = BlobServiceClient.from_connection_string(conn_str)
    src_container = service.get_container_client(src_container_name)
    dst_container = service.get_container_client(dst_container_name)

    # Make sure destination container exists
    try:
        dst_container.create_container()
        logging.info("Destination container created")
    except Exception:
        # already exists
        pass

    copied = 0
    skipped = 0

    for blob in src_container.list_blobs():
        src_blob = src_container.get_blob_client(blob.name)
        dst_blob = dst_container.get_blob_client(blob.name)

        # Skip if already exists in dev
        try:
            dst_blob.get_blob_properties()
            skipped += 1
            continue
        except Exception:
            pass

        logging.info(f"Copying blob: {blob.name}")
        dst_blob.start_copy_from_url(src_blob.url)
        copied += 1

    logging.info(f"CopyProdToDev completed. Copied: {copied}, skipped: {skipped}")