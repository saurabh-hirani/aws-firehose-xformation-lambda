from __future__ import print_function

import os
import sys
import base64
import json
import importlib

import logger_utils

logger = logger_utils.setup_logging(name="xformation_lambda_logger", log_level=os.environ.get("LOG_LEVEL", "DEBUG"))


def load_input_params(event):
    """ Load input params """
    xformation_func_strings = os.environ.get("XFORMATION_FUNCS", event.get("XFORMATION_FUNCS", ""))
    timestamp_key = os.environ.get("TIMESTAMP_KEY", event.get("TIMESTAMP_KEY", "log_timestamp"))
    extract_index_date_func_string = os.environ.get(
        "EXTRACT_INDEX_DATE_FUNC", event.get("EXTRACT_INDEX_DATE_FUNC", "extract_index_date")
    )

    xformation_funcs = []

    for func in xformation_func_strings.split(","):
        mod_name, func_name = func.split(".", 1)
        mod = importlib.import_module(mod_name)
        xformation_funcs.append(getattr(mod, func_name))

    mod_name, func_name = extract_index_date_func_string.split(".", 1)
    mod = importlib.import_module(mod_name)
    extract_index_date_func = getattr(mod, func_name)

    return {
        "XFORMATION_FUNCS": xformation_funcs,
        "EXTRACT_INDEX_DATE_FUNC": extract_index_date_func,
        "TIMESTAMP_KEY": timestamp_key,
    }


def lambda_handler(event, context):
    """ Lambda handler """
    # Get env vars
    input_params = load_input_params(event)
    output = []
    stats = {
        "firehose_name": str(event["deliveryStreamArn"]),
        "total_records": 0,
        "total_processed": 0,
        "total_failed": 0,
        "total_failed_max_size_exceeded": 0,
        "total_failed_b64_decode": 0,
        "total_failed_json_load": 0,
        "total_failed_xformation": 0,
        "total_event_record_size_bytes": 0,
        "max_event_record_size_bytes": 0,
        "min_event_record_size_bytes": 0,
        "all_records_processed": True,
        "index_dates": "",
    }

    stats["total_records"] = len(event["records"])
    min_size = max_size = None
    total_size = 0
    uniq_dates = []

    for record_num, record in enumerate(event["records"]):

        status = {"recordId": record["recordId"], "result": "Ok"}

        record_size = len(record["data"])

        total_size += record_size

        if max_size is None:
            max_size = record_size
        elif record_size > max_size:
            max_size = record_size

        if min_size is None:
            min_size = record_size
        elif record_size < min_size:
            min_size = record_size

        try:
            b64_decoded_record_data = base64.b64decode(record["data"])
        except Exception as ex:
            logger.debug("dumping_error", extra={"stage": "base64_decoding", "error": str(ex)})
            stats["total_failed"] += 1
            stats["total_failed_b64_decode"] += 1
            status["data"] = record["data"]
            status["result"] = "ProcessingFailed"
            output.append(status)
            continue

        try:
            loaded_record_data = json.loads(b64_decoded_record_data)
        except Exception as ex:
            logger.debug("dumping_error", extra={"stage": "json_loading", "error": str(ex)})
            stats["total_failed"] += 1
            stats["total_failed_json_load"] += 1
            status["data"] = record["data"]
            status["result"] = "ProcessingFailed"
            output.append(status)
            continue

        logger.debug("decoded_record_data", extra=loaded_record_data)

        updated_record_data = loaded_record_data

        for func in input_params["XFORMATION_FUNCS"]:
            logger.debug("running_xformation_func", extra={"function": str(func)})
            try:
                updated_record_data = func(updated_record_data, input_params["TIMESTAMP_KEY"])
            except Exception as ex:
                logger.debug("dumping_error", extra={"stage": "xformation", "function": str(func), "error": str(ex)})
                stats["total_failed"] += 1
                stats["total_failed_xformation"] += 1
                status["data"] = record["data"]
                status["result"] = "ProcessingFailed"
                output.append(status)
                continue

        logger.debug("extracting_timestamp_date")
        try:
            curr_date = input_params["EXTRACT_INDEX_DATE_FUNC"](updated_record_data, input_params["TIMESTAMP_KEY"])
            if curr_date not in uniq_dates:
                uniq_dates.append(curr_date)

            if len(uniq_dates) > 1:
                uniq_dates.sort()

            stats["index_dates"] = ",".join(uniq_dates)

        except Exception as ex:
            logger.debug("dumping_error", extra={"stage": "extract_timestamp_date", "error": str(ex)})
            stats["total_failed"] += 1
            stats["total_failed_xformation"] += 1
            status["data"] = record["data"]
            status["result"] = "ProcessingFailed"
            output.append(status)
            continue

        logger.debug("dumping_transformed_payload", extra=updated_record_data)

        status["data"] = base64.b64encode(json.dumps(updated_record_data).encode("utf-8")).decode("utf-8")
        logger.debug("dumping_lambda_status", extra=status)

        output.append(status)
        stats["total_processed"] += 1

    if stats["total_records"] != stats["total_processed"]:
        stats["all_records_processed"] = False

    stats["min_event_record_size_bytes"] = min_size
    stats["max_event_record_size_bytes"] = max_size
    stats["total_event_record_size_bytes"] = total_size

    logger.info("xformation_stats", extra=stats)

    return {"records": output}


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("ERROR: Provide payload file as the first arg.")
        sys.exit(1)

    event = json.loads(open(sys.argv[1]).read())

    logger.info("lambda_output", extra={"records": lambda_handler(event, None)})
