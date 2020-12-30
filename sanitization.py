import os
from google.cloud import bigquery
import json
from dotenv import load_dotenv
from datetime import date

load_dotenv()
EVENTS_TABLE_DEV = os.environ["EVENTS_TABLE_DEV"]
EVENTS_TABLE_PROD = os.environ["EVENTS_TABLE_PROD"]
FIRESTORE_LATEST = os.environ["FIRESTORE_LATEST"]
SVC_ACCOUNT_FIRESTORE = os.environ["SVC_ACCOUNT_FIRESTORE"]
SVC_ACCOUNT_EVENTS = os.environ["SVC_ACCOUNT_EVENTS"]
CHANNEL = os.environ["CHANNEL"]
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
    def __init__(self, svc_account, project_id, dataset_id, dest_table, dest_table_schema, channel):
        self.table_id = f'{project_id}.{dataset_id}.{dest_table}'
        self.channel = channel
        self.svc_account = self.set_svc_account(svc_account)
        self.client = bigquery.Client()
        self.user_orgs = self.get_orgs()
        self.dest_table_schema = self.format_schema(dest_table_schema)
        self.dest_json_file = f'./Sanitized_Data_{channel}_{date.today()}.json'
        self.channel_hashmap = {
         "Digital Dashboard Blast": "otsuka",
         "CNS Dashboard Blast": "otsuka",
         "Neph Dashboard Blast": "otsuka",
         "neph_map_viewer": "otsuka",
         "Marketplace Blast": "marketplace",
         "marketplace_profiles_viewer": "marketplace",
         "marketplace_post_viewer": "marketplace",
         "marketplace_map_viewer": "marketplace",
         "celltelligence": "celltelligence"
          }

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

    def load_data_from_file(self):
        """
        Loads json newline delimited file to BigQuery table
        :return: None
        """
        try:
            print("Loading data to BigQuery...")
            job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                                                schema=self.dest_table_schema)
            # job_config.autodetect = True
            with open(self.dest_json_file, "rb") as source_file:
                job = self.client.load_table_from_file(source_file, self.table_id, job_config=job_config)
            print(job.result())
            print("Loaded {} rows into {}.".format(job.output_rows, self.table_id))
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

    def create_json_newline_delimited_file(self, json_object):
        """
        Create a json delimited file from a json object, required format
        to load data from file to BigQuery table
        :param json_object: list/tuple iterable json object
        :return: None
        """
        print("Generating JSON file...")
        with open(self.dest_json_file, 'w') as f:
            for d in json_object:
                json.dump(d, f, default=str)
                f.write('\n')
        print(f"JSON file created at path: {self.dest_json_file}")

    def get_orgs(self):
        query_firebase_user_orgs = f"""
        SELECT * 
        FROM fenix-marketplace-data.user_data.latest_orgs
        ORDER BY email
        """
        query_job_firestore = self.run_query(query_firebase_user_orgs)
        users_hash_map = {row['email']: {} for row in query_job_firestore}
        for row in query_job_firestore:
            users_hash_map[row['email']].update({row['org']: str(row['org_changed'])})
        return users_hash_map

    def get_channel_org(self, user):
        if user not in self.user_orgs:
            return None, None
        channel_org = self.channel_hashmap[self.channel]
        for org, org_change in self.user_orgs[user].items():
            if org.split('-')[0].lower() == channel_org:
                return org, org_change
        return None, None

    def update_events_columns(self, events):
        """
        Gets and update org and org_sync_date columns
        :return:  list of dicts fixed records
        """
        print("Updating events columns...")
        for index, row in enumerate(events):
            events[index]['org'], events[index]['org_sync_date'] = self.get_channel_org(row['email'].lower())
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
    start_date = '20200101'
    end_date = '20201212'
    QUERY_EVENTS = f"""
    SELECT  *
            FROM {EVENTS_TABLE_PROD}
      WHERE 
            email NOT LIKE '%fenix%' AND email NOT LIKE '%dbala%'
            AND event IN ('open', 'delivered', 'click', 'bounce')
            AND EXTRACT(DATE FROM post_date) BETWEEN PARSE_DATE('%Y%m%d', {start_date}) AND PARSE_DATE('%Y%m%d', {end_date})
            AND channel IN ('{CHANNEL}')
      ORDER BY post_date desc
    """
    processor_job = BigQueryProcessor(SVC_ACCOUNT_FIRESTORE, DEST_PROJECT_ID, DEST_DATASET_ID,
                                      DEST_TABLE_ID, DEST_TABLE_SCHEMA, CHANNEL)
    processor_job.set_svc_account(SVC_ACCOUNT_EVENTS)
    events_records = processor_job.run_query(QUERY_EVENTS)
    fixed_event_records = processor_job.update_events_columns(events_records)
    processor_job.create_json_newline_delimited_file(fixed_event_records)
    processor_job.load_data_from_file()
