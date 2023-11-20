import argparse
import json
import pandas as pd

def _read_log_file(logFile):
    print(f"Reading log file {logFile}...")
    fLog = open(logFile, "r")
    logData = fLog.read()
    fLog.close()
    print("Log file read.")
    return logData

def _get_failed_tables(logData):
    print("Generating failed tables...")
    allFailedTables = []
    failedReasons = []
    try:
        for log in logData.split("\n"):
            logJSON = json.loads(log)
            failedTable = logJSON['table']
            failedReason = logJSON['summary'][:200]
            print(failedTable)
            allFailedTables.append(failedTable)
            failedReasons.append(failedReason)
    except json.decoder.JSONDecodeError:
        pass
    return allFailedTables, failedReasons

def _report(allFailedTables, failedReasons):
    print(f"Failed tables: {len(allFailedTables)}")
    print(f"Failed reasons: {len(failedReasons)}")
    print("Saving in log file failed_tables.csv ...")
    pd.DataFrame.from_dict(
        dict({
            "failed_table": allFailedTables,
            "failed_reason": failedReasons
    })
    ).to_csv("failed_tables.csv")
    print("Saved.")

def main(LOGFILE):
    logData = _read_log_file(LOGFILE)
    allFailedTables, failedReasons = _get_failed_tables(logData)
    _report(allFailedTables, failedReasons)

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Generate failed tables list.")
    parser.add_argument("--LOGFILE", "--LOG", dest="LOGFILE", help="Path to failed log metastore file.")
    parser = parser.parse_args()
    main(parser.LOGFILE)