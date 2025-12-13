import logging
import os

import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

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
        # --- Step 1: env vars ---
        conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        src_container_name = os.environ["PROD_CONTAINER"]
        dst_container_name = os.environ["DEV_CONTAINER"]

        # Optional: only sync a "folder"/prefix inside the containers (leave unset to sync all)
        sync_prefix = os.environ.get("SYNC_PREFIX", "").strip() or None

        logging.info(
            "Syncing src_container=%s to dst_container=%s (prefix=%s)",
            src_container_name,
            dst_container_name,
            sync_prefix or "<ALL>",
        )

        # --- Step 2: clients ---
        service = BlobServiceClient.from_connection_string(conn_str)
        src_container = service.get_container_client(src_container_name)
        dst_container = service.get_container_client(dst_container_name)

        # --- Step 3: ensure destination container exists ---
        try:
            dst_container.create_container()
            logging.info("Destination container created")
        except Exception:
            logging.info("Destination container already exists")

        copied = 0
        updated = 0
        skipped = 0
        deleted = 0

        # Track all source blob names so we can delete orphans from destination
        src_names: set[str] = set()

        # --- Step 4: copy/update/skip loop ---
        # NOTE: list_blobs() typically includes etag and size on each item, so we can avoid
        # an extra HEAD call to source for each blob.
        for src_item in src_container.list_blobs(name_starts_with=sync_prefix):
            name = src_item.name
            src_names.add(name)

            src_blob = src_container.get_blob_client(name)
            dst_blob = dst_container.get_blob_client(name)

            # Source fingerprint we will store on destination metadata
            # (ETag as returned by list_blobs is fine; size is another useful check)
            src_etag = src_item.etag
            src_len = str(src_item.size)

            try:
                dst_props = dst_blob.get_blob_properties()
                dst_meta = dst_props.metadata or {}

                # We compare against the metadata *we previously wrote*,
                # not destination ETag (destination ETag is a different resource/version tag).
                if dst_meta.get("src_etag") == src_etag and dst_meta.get("src_len") == src_len:
                    skipped += 1
                    continue

                logging.info("Updating blob (source changed): %s", name)

                # Best-effort delete first (helps avoid conflicts with some existing states/leases)
                try:
                    dst_blob.delete_blob()
                except Exception:
                    logging.info("Could not delete before copy; attempting copy anyway: %s", name)

                dst_blob.start_copy_from_url(
                    src_blob.url,
                    metadata={"src_etag": src_etag, "src_len": src_len},
                )
                updated += 1

            except ResourceNotFoundError:
                logging.info("Copying new blob: %s", name)
                dst_blob.start_copy_from_url(
                    src_blob.url,
                    metadata={"src_etag": src_etag, "src_len": src_len},
                )
                copied += 1

        # --- Step 5: delete blobs in destination that are not in source (mirror deletions) ---
        for dst_item in dst_container.list_blobs(name_starts_with=sync_prefix):
            if dst_item.name not in src_names:
                logging.info("Deleting orphan blob removed from source: %s", dst_item.name)
                try:
                    dst_container.delete_blob(dst_item.name)
                    deleted += 1
                except Exception:
                    logging.exception("Failed deleting destination blob: %s", dst_item.name)

        logging.info(
            "CopyProdToDev completed. Copied: %d, updated: %d, skipped: %d, deleted: %d",
            copied,
            updated,
            skipped,
            deleted,
        )

    except Exception:
        logging.exception("CopyProdToDev failed with an unexpected error")
        raise