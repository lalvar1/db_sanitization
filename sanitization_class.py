import os
from google.cloud import bigquery
import json

EVENTS_TABLE_DEV = os.environ["EVENTS_TABLE_DEV"]
EVENTS_TABLE_PROD = os.environ["EVENTS_TABLE_PROD"]
FIRESTORE_TABLE_CHANGELOG = os.environ["FIRESTORE_TABLE_CHANGELOG"]
FIRESTORE_LATEST = os.environ["FIRESTORE_LATEST"]
SVC_ACCOUNT_FIRESTORE = os.environ["SVC_ACCOUNT_FIRESTORE"]
SVC_ACCOUNT_EVENTS = os.environ["SVC_ACCOUNT_EVENTS"]
JSON_FILE_PATH = os.environ["JSON_FILE_PATH"]
DEST_PROJECT_ID = os.environ["DEST_PROJECT_ID"]
DEST_DATASET_ID = os.environ["DEST_DATASET_ID"]
DEST_TABLE_ID = os.environ["DEST_TABLE_ID"]
DEST_TABLE_SCHEMA = {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'}, \
                    {'name': 'post_id', 'type': 'INTEGER', 'mode': 'NULLABLE'}, \
                    {'name': 'post_slug', 'type': 'STRING', 'mode': 'NULLABLE'}, \
                    {'name': 'post_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}, \
                    {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}, \
                    {'name': 'event', 'type': 'STRING', 'mode': 'NULLABLE'}, \
                    {'name': 'useragent', 'type': 'STRING', 'mode': 'NULLABLE'}, \
                    {'name': 'subject', 'type': 'STRING', 'mode': 'NULLABLE'}, \
                    {'name': 'ip', 'type': 'STRING', 'mode': 'NULLABLE'}, \
                    {'name': 'channel', 'type': 'STRING', 'mode': 'NULLABLE'}, \
                    {'name': 'org', 'type': 'STRING', 'mode': 'NULLABLE'}, \
                    {'name': 'org_sync_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}


class BigQueryProcessor:
    def __init__(self, svc_account, project_id, dataset_id, dest_table, dest_table_schema,
                 orgs_dataset):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.dest_table = dest_table
        self.user_orgs = self.get_user_historical_orgs(orgs_dataset)
        self.svc_account = self.set_svc_account(svc_account)
        self.client = bigquery.Client()
        self.dest_table_schema = self.format_schema(dest_table_schema)

    def run_query(self, query):
        """
        Perform SQL to GCP BigQuery table
        :param query: SQL query
        :return: list of dicts records
        """
        try:
            print(f"Running query: {query}")
            query_job = self.client.query(query)
            records = [dict(row) for row in query_job]
            return records
        except Exception as e:
            print(f"Error processing query: {query}. Error was: {e}")

    def load_data_from_file(self, json_file):
        """
        Loads json newline delimited file to BigQuery table
        :param json_file: json file path
        :return: None
        """
        try:
            print("Loading data to BigQuery...")
            dataset = self.client.dataset(DEST_DATASET_ID)
            table = dataset.table(DEST_TABLE_ID)
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job_config.schema = self.dest_table_schema
            job_config.autodetect = True
            with open(json_file, "rb") as source_file:
                job = self.client.load_table_from_file(source_file, table, job_config=job_config)
            print(job.result())
            print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset, table))
        except Exception as e:
            print(f"Error loading data to BigQuery. Error was: {e}")

    def set_svc_account(self, svc_account_path):
        """
        Redefine GCP Client, needed when changing svc accounts accross different GCP projects
        :param svc_account_path:
        :return:
        """
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = svc_account_path
        self.client = bigquery.Client()
        return svc_account_path

    @staticmethod
    def nice_query_stdout(query_job):
        """
        Return beautified stdout from query job object
        :param query_job: query_job type object
        :return: None
        """
        records = [dict(row) for row in query_job]
        print(*records, sep='\n')

    @staticmethod
    def create_json_newline_delimited_file(file_path, json_object):
        """
        Create a json delimited file from a json object, required format
        to load data from file to BigQuery table
        :param file_path: json file path
        :param json_object: list/tuple iterable json object
        :return: None
        """
        print("Generating JSON file...")
        with open(file_path, 'w') as f:
            for d in json_object:
                json.dump(d, f, default=str)
                f.write('\n')
        print(f"JSON file created at path: {file_path}")

    def get_user_historical_orgs(self, table):
        """
        Get all distinct org changes per user on input table
        :param table: BigQuery table name
        :return: dict of dicts (users hash_map) object
        """
        query_firebase_user_orgs = f"""
        SELECT
               MIN(timestamp) org_change,
               LOWER(JSON_EXTRACT_SCALAR(data , "$.user_email")) user,
               JSON_EXTRACT_SCALAR(data , "$.user_subscription_notification[0]") AS org,
        FROM {table}
        WHERE 
               JSON_EXTRACT_SCALAR(data , "$.user_email") NOT LIKE '%fenix%' AND
               JSON_EXTRACT_SCALAR(data , "$.user_subscription_notification[0]") IS NOT NULL 
        GROUP BY 2,3
        ORDER BY org_change
        """
        query_job_firestore = self.run_query(query_firebase_user_orgs)
        users_hash_map = {row['user']: {} for row in query_job_firestore}
        for row in query_job_firestore:
            users_hash_map[row['user']].update({row['org']: str(row['org_change'])})
        return users_hash_map

    def get_fixed_org(self, event_date, user):
        """
        Get nearest org_change and its corresponding org
        :param event_date: str event timestamp
        :param user: user ID
        :return: tuple: (org, org_change)
        """
        if user not in self.user_orgs:
            return None, None
        ordered_orgs = sorted(self.user_orgs[user].items(), key=lambda x: x[1], reverse=True)
        for org, org_change in ordered_orgs:
            if str(event_date) > org_change:
                return org, org_change
        return ordered_orgs[-1][0], ordered_orgs[-1][1]

    def update_events_columns(self, events):
        """
        Gets and update org and org_sync_date columns
        :return:  list of dicts fixed records
        """
        print("Updating events columns...")
        for index, row in enumerate(events):
            events[index]['org'], events[index]['org_sync_date'] = self.get_fixed_org(row['post_date'],
                                                                                      row['email'])
        return events

    @staticmethod
    def format_schema(schema):
        """
        Format table Schema
        :param schema: BigQuery table schema
        :return: Formatted Schema
        """
        formatted_schema = []
        for row in schema:
            formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
        return formatted_schema


if __name__ == "__main__":
    QUERY_EVENTS = f"""
    SELECT  *
            FROM {EVENTS_TABLE_PROD}
      WHERE 
            email NOT LIKE '%fenix%' AND email NOT LIKE '%dbala%'
            AND event IN ('open', 'delivered', 'click', 'bounce')
            AND channel IN ('celltelligence')
    """
    processor_job = BigQueryProcessor(SVC_ACCOUNT_FIRESTORE, DEST_PROJECT_ID, DEST_DATASET_ID,
                                      DEST_TABLE_ID, DEST_TABLE_SCHEMA, FIRESTORE_TABLE_CHANGELOG)
    processor_job.set_svc_account(SVC_ACCOUNT_EVENTS)
    events_records = processor_job.run_query(QUERY_EVENTS)
    # processor_job.nice_query_stdout(query_job)
    fixed_event_records = processor_job.update_events_columns(events_records)
    processor_job.create_json_newline_delimited_file(JSON_FILE_PATH, fixed_event_records)
    processor_job.load_data_from_file(JSON_FILE_PATH)
