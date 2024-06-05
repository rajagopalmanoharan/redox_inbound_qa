#Python/Dagster Script/Job/Schedule to
# download, database and run data quality checks
# against Admit, Discharge and Transfer (ADT) data
# being received from Health Information Exchanges (HIEs)
# via Redox real-time messages arriving as HTTP posts
#
import sys
sys.path.append("..")
from util import DpLogger, get_run_config, email, sub_var, get_logger
from core import auth_bq, get_secret, google_cloud_logging_logger, log_run
import json
import re
import mmap
import os
import logging
from datetime import date, datetime, timedelta
from dagster import op, job, schedule, repository, RunRequest, RunConfig
from google.cloud import bigquery
from google.cloud import storage
from pathlib import Path


class Parms:
    def __init__(self,context):

        #
        # Initialization
        #
        code_environment = "DEV"  # LOCAL, DEV,TEST or PROD
        data_environment = "STAGING"  # STAGING or PROD
        self.debug_mode = False

        # Set credentials based upon environment
        if code_environment == "LOCAL":
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = \
                 "/Users/arvydas.sepetys/.config/gcloud/" \
                 "batch_service_account_key.json"
            #os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = \
            #     "/Users/arvydas.sepetys/.config/gcloud/" \
            #     "application_default_credentials.json"
        elif code_environment == "DEV":
            context.log.info("Bypassing credential setting in Dev environment, using preset credentials.")
        elif code_environment == "TEST":
            context.log.info("Bypassing credential setting in Test environment, using present credentials.")
        elif code_environment == "PROD":
            context.log.info("Bypassing credential setting in Production environment, using present credentials.")
        else:
            context.log.error("Unknown code environment.  Exiting...")
            exit()

        # Set data locations based upon environment
        if data_environment == "DEV":
            self.source_bucket_and_folder = \
                "staging-cityblock-data-raw-inputs/redox/raw/dev"
            self.bundles_folder = "tempBundlesStaging"
            self.target_table = "cbh-data-platform-dev.cbh_validation.redox_inbound_bundles"
            self.test_config_file = \
                str(Path(__file__)
                    .with_name("redox_staging_inbound_qa_config_tests.txt"))
        elif data_environment == "STAGING":
            self.source_bucket_and_folder = \
                "staging-cityblock-data-raw-inputs/redox/raw/dev"
            self.bundles_folder = "tempBundlesStaging"
            self.target_table = "cbh-data-platform-staging.cbh_validation.redox_inbound_bundles"
            self.test_config_file = \
                str(Path(__file__)
                    .with_name("redox_staging_inbound_qa_config_tests.txt"))
        elif data_environment == "PROD":
            self.source_bucket_and_folder = \
                "cityblock-data-raw-inputs/redox/raw/prod"
            self.bundles_folder = "tempBundles"
            self.target_table = "cbh-data-platform-production.cbh_validation.redox_inbound_bundles"
            self.test_config_file = \
                str(Path(__file__)
                    .with_name("redox_inbound_qa_config_tests.txt"))
        else:
            context.log.error("Unknown data environment.  Exiting...")
            exit()

        # Display run-time parameters
        context.log.info("Running with parameters as follows:")
        #
        # print("GOOGLE_APPLICATION_CREDENTIALS:" +
        #      (os.environ.get('GOOGLE_APPLICATION_CREDENTIALS2')
        #       if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS2')
        #       else "Not Found"))
        #
        context.log.info("source_bucket_and_folder:" + self.source_bucket_and_folder)
        context.log.info("bundles_folder:" + self.bundles_folder)
        context.log.info("target_table:" + self.target_table)
        context.log.info("test_config_file:" + self.test_config_file)


#
# Operation/function to get all bundles received from Redox
# up to n days ago (currently set at 1 day ago)
#
@op
def get_bundles_op(context):

    parms = Parms(context)

    try:
        #
        # Start up housekeeping
        #
        print('Removing any temp directories, files from previous run...')
        context.log.info(
            'Removing any temp directories, files from previous run...')

        os.system("rm -rf " + parms.bundles_folder)
        os.system("mkdir " + parms.bundles_folder)

        #
        # Download today's and up to n-days-ago bundles
        #
        runDate = date.today().strftime("%Y%m%d")
        fileDate = date.today().strftime("%Y-%m-%d")
        start_days_ago = 0
        end_days_ago = 7

        this_days_ago = start_days_ago
        context.log.info('Start days ago looping')

        while this_days_ago <= end_days_ago:
            this_file_date = (
                datetime.today() -
                timedelta(
                    days=this_days_ago)).strftime("%Y-%m-%d")
            print("this_file_date:" + str(this_file_date))
            context.log.info("this_file_date:" + str(this_file_date))

            print(
                "[redox_inbound_qa " +
                datetime.now().strftime("%Y-%m-%d %H:%M:%S") +
                "] "
                "Retrieving all message data bundles for " +
                this_file_date +
                " from Google Cloud Storage...")
            context.log.info(
                "[redox_inbound_qa " +
                datetime.now().strftime("%Y-%m-%d %H:%M:%S") +
                "] "
                "Retrieving all message data bundles for " +
                this_file_date +
                " from Google Cloud Storage...")

            os_command = "gsutil ls -l " \
                "gs://" + parms.source_bucket_and_folder + "/** | " \
                "grep -i \"" + this_file_date + "\" | " \
                "grep -io \"gs.*$\" | " \
                "gsutil -m cp -n -I " + parms.bundles_folder
            os.system(os_command)

            this_days_ago += 1

        return "success"

    except Exception as e:
        print("get_bundle_op failed with error:" + e)
        context.log.error("get_bundle_op failed with error:" + e)

#
# Function to extract select data elments from
# specified bundle/json file
#


def process_file(context,filename, load_file, logger=get_logger()):

    parms = Parms(context)

    if parms.debug_mode: context.log.info("Starting processing of file:" + filename)

    #
    # Read file into memory
    #
    filecontents = open(filename).read()
    #
    # Alternate way to read file into memory (may or may not be faster)
    #
    # with open(filename,'r+') as f:
    #    filecontents = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    #        text = mmap_obj.read()
    #        print(text)

    #
    # Find bundle number
    #
    match = re.search("bundle-transaction-(.*?)\"", filecontents)
    Bundle = match.group(1) if match else "Not Found"
    if parms.debug_mode: context.log.info("Bundle:" + Bundle)

    #
    # Find Source
    #
    match = re.search("\"source\":\"(.*?)\"", filecontents)
    Source = match.group(1) if match else "Not Found"
    if parms.debug_mode: context.log.info("Source:" + Source)

    #
    # Find MessageDateTime
    #
    match = re.search("lastUpdated...(.*?)\"", filecontents)
    MessageDateTime = match.group(1) if match else "Not Found"
    if parms.debug_mode: context.log.info("MessageDateTime:" + MessageDateTime)

    #
    # Find FirstName
    #
    match = re.search("POST...url...Patient.*?given....(.*?)\"", filecontents)
    FirstName = match.group(1) if match else "Not Found"
    if parms.debug_mode: context.log.info("FirstName:" + FirstName)

    #
    # Find LastName
    #
    match = re.search("POST...url...Patient.*?family...(.*?)\"", filecontents)
    LastName = match.group(1) if match else "Not Found"
    if parms.debug_mode: context.log.info("LastName:" + LastName)

    #
    # Find Birthdate
    #
    match = re.search(
        "POST...url...Patient.*?birthDate...(.*?)\"",
        filecontents)
    Birthdate = match.group(1) if match else "Not Found"
    if parms.debug_mode: context.log.info("Birthdate:" + Birthdate)

    #
    # Find PatientId
    #
    match = re.search("POST...url...Patient.*?"
                      "identifier.*?value...(.{36}).{30,60}:CITYBLOCK.*?",
                      filecontents, re.IGNORECASE)
    PatientId = match.group(1) if match else "Not Found"
    if parms.debug_mode: context.log.info("PatientId:" + PatientId)

    #
    # Find AdmitDateTime
    #
    match = re.search("POST...url...Encounter.*?"
                      "period.*?start...(.*?)\"",
                      filecontents, re.IGNORECASE)
    AdmitDateTime = match.group(1) if match else "Not Found"
    if parms.debug_mode: context.log.info("AdmitDateTime:" + AdmitDateTime)

    #
    # Find DischargeDateTime
    #
    matchEncounter = re.search("POST...url...(Encounter.*?)\"POST\"",
                               filecontents, re.IGNORECASE)
    Encounter = matchEncounter.group(1) if matchEncounter else ""
    if parms.debug_mode: context.log.info("Encounter:" + Encounter)

    match = re.search("period....start.{1,30}\"end\":\"(.{1,30}?)\"",
                      Encounter, re.IGNORECASE)
    DischargeDateTime = match.group(1) if match else ""
    if parms.debug_mode: context.log.info("DischargeDateTime:" + DischargeDateTime)

    #match = re.search("POST...url...Encounter.*?"
    #                  "period....start.{1,30}\"end\":\"(.{1,30}?)\"",
    #                  filecontents, re.IGNORECASE)
    #DischargeDateTime = match.group(1) if match else ""
    #if parms.debug_mode: context.log.info("DischargeDateTime:" + DischargeDateTime)

    #
    # Find AdmitType
    #
    match = re.search("POST...url...Encounter.*?"
                      "class....code...(.*?)\"",
                      filecontents, re.IGNORECASE)
    AdmitType = match.group(1) if match else "Not Found"
    if parms.debug_mode: context.log.info("AdmitType:" + AdmitType)

    #
    # Write bundle extract into pipe-delimited load file
    #
    loadData = Bundle + "|" + \
        Source + "|" + \
        MessageDateTime + "|" + \
        PatientId + "|" + \
        FirstName + "|" + \
        LastName + "|" + \
        Birthdate + "|" + \
        AdmitDateTime + "|" + \
        DischargeDateTime + "|" + \
        AdmitType + "\n"
    if parms.debug_mode: context.log.info("loadData:" + loadData)
    load_file.write(loadData)


#
# Operation/Function to cycle through each downloaded
# bundle/file, calling data extraction function above
# to extract selected data elements
#
@op
def extract_data_op(context,get_bundles_op_result, logger=get_logger()):

    parms = Parms(context)

    try:
        #
        # Open and write headings of file to be used
        # for loading new data into BigQuery
        #
        load_file = open(parms.bundles_folder
                         + "/tempBundlesLoad.txt", "w")
        load_file.write("Bundle|Source|MessageDateTime|"
                        "PatientId|FirstName|LastName|Birthdate|"
                        "AdmitDateTime|DischargeDateTime|AdmitType\n")
        load_file.close()

        #
        # Reopen output file in append mode and write data records,
        # one per bundle file read in
        #
        load_file = open(
            parms.bundles_folder +
            "/tempBundlesLoad.txt",
            "a")  # reopen in append mode

        directory = parms.bundles_folder
        for this_file in os.listdir(directory):
            if this_file.endswith(".json"):
                process_file(context,directory + "/" + this_file, load_file)
        load_file.close()

        return "success"

    except Exception as e:
        context.log.error("extract_data_op failed with error:" + e)


#
# Operation/function to load extracted data into BigQuery
#
@op
def load_data_op(context,get_extract_data_op_result, logger=get_logger()):

    parms = Parms(context)

    try:
        client = bigquery.Client()
        table_id = parms.target_table + "temp"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter='|',
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("Bundle", "INTEGER"),
                bigquery.SchemaField("Source", "STRING"),
                bigquery.SchemaField("MessageDateTime", "TIMESTAMP"),
                bigquery.SchemaField("PatientId", "STRING"),
                bigquery.SchemaField("FirstName", "STRING"),
                bigquery.SchemaField("LastName", "STRING"),
                bigquery.SchemaField("Birthdate", "STRING"),
                bigquery.SchemaField("AdmitDateTime", "STRING"),
                bigquery.SchemaField("DischargeDateTime", "STRING"),
                bigquery.SchemaField("AdmitType", "STRING")
            ],
            skip_leading_rows=1,
        )

        context.log.info("Load file:" + parms.bundles_folder + "/tempBundlesLoad.txt")
        load_file = open(
            parms.bundles_folder +
            "/tempBundlesLoad.txt",
            "rb")  # reopen in binary mode
        job = client.load_table_from_file(
            load_file, table_id, job_config=job_config)
        job.result()  # Waits for the job to complete.
        table = client.get_table(table_id)
        rows = str(table.num_rows)
        context.log.info("Loaded " + rows + " rows to " + table_id + " table.")

        return "success"

    except Exception as e:
        context.log.error("load_data_op failed with error:" + str(e))


#
# Operation/function to merge newly downloaded, extracted data into
# cumulative bundles table
#
@op
def merge_data_op(context,load_data_op_result, logger=get_logger()):

    parms = Parms(context)

    try:
        client = bigquery.Client()
        sql = "merge " + parms.target_table + " o " \
            "using " + parms.target_table + "temp n " \
            "  on n.Bundle = o.Bundle " \
            "when not matched then " \
            "  insert(Bundle,Source,MessageDateTime," \
            "    PatientId,FirstName,LastName,Birthdate," \
            "    AdmitDateTime,DischargeDateTime,AdmitType) " \
            "  values(Bundle,Source,MessageDateTime," \
            "    PatientId,FirstName,LastName,Birthdate," \
            "    AdmitDateTime,DischargeDateTime,AdmitType)"

        query_job = client.query(sql)  # API request
        rows = query_job.result()  # Waits for query to finish

        # to-do:  determine what statistic useful

        return "success"

    except Exception as e:
        context.log.error("merge_data_op failed while executing sql:" + sql)
        context.log.error("merge_data_op failed with error:" + e)


#
# Function to run data quality checks
#
@op
def execute_tests_op(context,
                     merge_data_op_result,
                     config_schema={"cfg": dict}):

    parms = Parms(context)

    #
    # Instantiate DpLogger with extra dq specific fields
    #
    try:
        logger = DpLogger(context, extra_labels={
            'test_name': 'tbd',
            'test_result': 'tbd',
            'test_threshold': 'tbd',
            'test_outcome': 'tbd',
            'test_sql': 'tbd',
            'data_source': parms.source_bucket_and_folder})

    except Exception as e:
        context.log.error("DpLogger instantiation failed with error:" + e)

    #
    # Execute tests as per tests config file
    #
    try:
        client = bigquery.Client()

        tests_file = open(parms.test_config_file, "r")
        lines = tests_file.readlines()

        for line in lines:
            if len(line.split('|')) > 1:  # skip blank/comment lines
                test = line.split('|')[0]
                threshold = float(line.split('|')[1])

                sql = "select (sum(case when " \
                      + test + " then 1 else 0 end) * 100) " \
                      " / count(*) as result " \
                      "from " + parms.target_table + "temp"

                context.log.info("sql:" + sql)

                query_job = client.query(sql)  # API request
                rows = query_job.result()  # Waits for query to finish

                for row in rows:
                    context.log.info(str(row.result) + "%  test:" + test + ")")

                    #
                    # test fail output
                    #
                    if row.result > threshold:
                        logger.error(
                            "Test failed",
                            stack_info=False,
                            extras={
                                'test_name': test,
                                'test_result': str(row.result),
                                'test_threshold': str(threshold),
                                'test_outcome': 'fail',
                                'test_sql': sql,
                                'data_source':
                                    parms.source_bucket_and_folder
                            }
                        )

                        context.log.error("warning: threshold of "
                              + str(threshold) + " exceeded for "
                              " test '" + test + "'.")
                        context.log.error("sql:" + sql)

                    #
                    # test pass output
                    #
                    else:
                        logger.info(
                            "Test passed",
                            extras={
                                'test_name': test,
                                'test_result': str(row.result),
                                'test_threshold': str(threshold),
                                'test_outcome': 'pass',
                                'test_sql': sql,
                                'data_source':
                                    parms.source_bucket_and_folder
                            }
                        )

        tests_file.close()

        return "success"

    except Exception as e:
        context.log.error("execute_tests_op failed with error:" + str(e))
        context.log.error("sql:" + sql)


@job(config={
    "loggers": {
        "google_cloud_logger": {
            "config": {
                "log_level": "INFO"}}}},
     logger_defs={
         "google_cloud_logger":
         google_cloud_logging_logger(log_level="INFO")},
     tags={
         "dagster-k8s/config": {
             "pod_spec_config": {
                 "serviceAccountName": "di"}}})
def redox_inbound_qa_job():
    execute_tests_op(
        merge_data_op(
            load_data_op(
                extract_data_op(
                    get_bundles_op()))))


@schedule(job=redox_inbound_qa_job,
          description="Redox Inbound Data QA Job",
          cron_schedule="0 6 * * *")
def redox_inbound_qa_schedule(context):

    parms = Parms(context)
    source_bucket_and_folder = "staging-cityblock-data-raw-inputs/redox/raw/dev"

    #
    # Define config using DI standard logging fields
    #
    this_run_config = {
        "ops": {
            "execute_tests_op": {
                "config": {
                    "cfg": {
                        "file_name": source_bucket_and_folder
                    }
                }
            }
        }
    }
    return RunRequest(
        run_key=None,
        run_config=this_run_config
    )


@repository
def redox_inbound_qa_repository():
    return [
        redox_inbound_qa_schedule,
    ]
