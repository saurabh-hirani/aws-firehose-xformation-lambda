import time


def convert_to_underscore(record, timestamp_key="timestamp"):
    """ Convert - to _ """
    underscore_record = {}
    for key, value in record.items():
        underscore_record[key.replace("-", "_")] = value
    return underscore_record


def convert_epoch_sec_to_iso8601(record, timestamp_key="timestamp"):
    """ Convert epoch sec time to iso8601 format """
    record[timestamp_key] = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(int(record[timestamp_key])))
    return record


def convert_epoch_ms_to_iso8601(record, timestamp_key="timestamp"):
    """ Convert epoch time ms to iso8601 format """
    s, ms = divmod(int(record[timestamp_key]), 1000)
    record[timestamp_key] = "%s.%03d" % (time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(s)), ms)
    return record


def extract_date_from_iso8601(record, timestamp_key="timestamp"):
    """ Extract date from iso8601 timestamp """
    return record[timestamp_key].split("T")[0]
