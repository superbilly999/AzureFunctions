import logging
import os

import azure.functions as func
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()


@app.function_name(name="CopyProdToDev")
@app.schedule(
    schedule="0 0 3 * * *",
    arg_name="mytimer",
    run_on_startup=False,
    use_monitor=True,
)
def copy_prod_to_dev(mytimer: func.TimerRequest) -> None:
    logging.info("CopyProdToDev timer function started")
    try:
        # Step 1: read env vars
        logging.info("Step 1: reading environment variables")
        conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        src_container_name = os.environ["PROD_CONTAINER"]
        dst_container_name = os.environ["DEV_CONTAINER"]
        logging.info(
            "Using src_container=%s, dst_container=%s",
            src_container_name,
            dst_container_name,
        )

        # Step 2: create clients
        logging.info("Step 2: creating BlobServiceClient")
        service = BlobServiceClient.from_connection_string(conn_str)
        src_container = service.get_container_client(src_container_name)
        dst_container = service.get_container_client(dst_container_name)

        # Step 3: ensure destination container
        logging.info("Step 3: ensuring destination container exists")
        try:
            dst_container.create_container()
            logging.info("Destination container created")
        except Exception:
            logging.info("Destination container already exists")

        # Step 4: copy blobs
        logging.info("Step 4: starting copy loop")
        copied = 0
        updated = 0
        skipped = 0

        for blob in src_container.list_blobs():
            src_blob = src_container.get_blob_client(blob.name)
            dst_blob = dst_container.get_blob_client(blob.name)

            try:
                src_props = src_blob.get_blob_properties()
                dst_props = dst_blob.get_blob_properties()

                if dst_props.etag == src_props.etag:
                    skipped += 1
                    continue

                logging.info("Updating blob (content differs): %s", blob.name)
                try:
                    dst_blob.delete_blob()
                except Exception:
                    logging.info("Destination blob existed but could not delete before copy; retrying copy anyway")

                dst_blob.start_copy_from_url(src_blob.url)
                updated += 1
            except Exception:
                logging.info("Copying new blob: %s", blob.name)
                dst_blob.start_copy_from_url(src_blob.url)
                copied += 1

        logging.info(
            "CopyProdToDev completed. Copied: %d, updated: %d, skipped: %d",
            copied,
            updated,
            skipped,
        )

    except Exception:
        # THIS will print the real error into Log Stream
        logging.exception("CopyProdToDev failed with an unexpected error")
        raise
