from __future__ import annotations
from enum import Enum
import psycopg2
import sqlite3
import pandas as pd
import datetime
import pm4py
from pm4py.objects.log.obj import EventLog, Trace, Event, EventStream
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.exporter.xes import exporter as xes_exporter

class AnnotationType(Enum):
    ONE_TO_ONE = 1
    ONE_TO_MANY = 2
    MANY_TO_MANY = 3

class Annotation:
    def __init__(self, table1, table2, att1, att2, annot_type):
        self.table1 = table1
        self.table2 = table2
        self.att1 = att1
        self.att2 = att2
        self.annot_type = annot_type

class RelationalToXESConverter:
    DEFAULT_CASE_ID = "@@DEFAULT_CASE@@"
    DEFAULT_ACTIVITY = "@@DEFAULT_ACTIVITY@@"
    DEFAULT_TIMESTAMP = datetime.datetime(1970,1,1)

    def __init__(self, event_table, activity_key, timestamp_key, case_id_key, annotations):
        '''
        Init the converter with needed parameters for an event log
        '''
        self.event_table = event_table
        self.activity_key = activity_key
        self.timestamp_key = timestamp_key
        self.case_id_key = case_id_key
        self.annotations = annotations.copy() # List of Annotation, NO CYCLE
        self.database_adapter = psycopg2 # Default to postgresql

    def set_db_access_params(self, db_access_params):
        '''
        Set parameters to access a database
        '''
        self.db_access_params = db_access_params.copy()

    def set_database_adapter(self, database_adapter):
        '''
        Choose database adapter. Many adapters share Python DB API 2.0, therefore code can be reused for different adapters
        Currently support sqlite and postgresql
        '''
        if database_adapter in ['psycopg2', 'postgresql', psycopg2]:
            self.database_adapter = psycopg2
        if database_adapter in ['sqlite', 'sqlite3', sqlite3]:
            self.database_adapter = sqlite3

    def retrieve_table(self, table_name):
        '''
        Retrieve column names and data of table from the database and return its column names and rows
        '''
        query = f"SELECT * FROM {table_name};"
        conn = self.database_adapter.connect(**self.db_access_params)
        cur = conn.cursor()
        cur.execute(query)
        data = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        conn.close()
        return colnames, data

    def retrieve_join_table(self, annot: Annotation):
        '''
        Retrieve column names and data of the join of two table based on an annotation
        '''
        join_type = None
        if annot.annot_type in [AnnotationType.ONE_TO_ONE, AnnotationType.ONE_TO_MANY]:
            join_type = 'JOIN'
        elif annot.annot_type == AnnotationType.ONE_TO_MANY:
            join_type = 'RIGHT JOIN'
        query = f"SELECT * FROM {annot.table1} {join_type} {annot.table2}\
             ON {annot.table1}.{annot.att1} = {annot.table2}.{annot.att2};"
        conn = self.database_adapter.connect(**self.db_access_params)
        cur = conn.cursor()
        cur.execute(query)
        data = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        conn.close()
        return colnames, data

    def build_event_log(
            self, 
            exported_log_name='exported',
            activity_identifier=None,
            default_activity=DEFAULT_ACTIVITY,
            default_case_id=DEFAULT_CASE_ID,
            default_timestamp=DEFAULT_TIMESTAMP
        ):
        '''
        Build an event log based on the parameter provided to the converter
        '''

        # Loop through the annotations and retrieve the tables
        db_tables = set()
        table_store = dict()
        event_table_adj_annots = []
        event_table_not_adj_annots = []
        for a in self.annotations:
            db_tables.add(a.table1)
            db_tables.add(a.table2)
            if a.table1 == self.event_table or a.table2 == self.event_table:
                event_table_adj_annots.append(a)
            else:
                event_table_not_adj_annots.append(a)
        for table in db_tables:
            cols, data = self.retrieve_table(table)
            table_store[table] = pd.DataFrame(columns=cols, data=data)

        # Merging table on edges not adjacent to the event table
        while len(event_table_not_adj_annots) > 0:
            an = event_table_not_adj_annots.pop()
            if an.annot_type in [AnnotationType.MANY_TO_MANY, AnnotationType.ONE_TO_ONE]: 
                new_table = pd.merge(table_store[an.table1], table_store[an.table2], left_on=an.att1, right_on=an.att2)
            else: # ONE TO MANY
                new_table = pd.merge(table_store[an.table1], table_store[an.table2], left_on=an.att1, right_on=an.att2, how='right')
            if an.att1 + "_x" in new_table:
                new_table = new_table.drop([an.att1 + "_x"])
            if an.att2 + "_y" in new_table:
                new_table = new_table.drop([an.att2 + "_y"])
            table_store[an.table1] = new_table
            for ann in event_table_adj_annots + event_table_not_adj_annots:
                if ann.table1 == an.table2:
                    ann.table1 = an.table1
                if ann.table2 == an.table2:
                    ann.table2 = an.table1

        # Transforming tables adjacent to event table to dict-like
        for annot in event_table_adj_annots:
            if annot.table1 != self.event_table:
                t = annot.table1
                att = annot.att1
                if annot.annot_type in [AnnotationType.ONE_TO_MANY, AnnotationType.ONE_TO_ONE]:
                    table_store[t] = table_store[t].set_index([att]).transpose().to_dict()
                else:
                    temp = dict()
                    for index, row in table_store[t].iterrows():
                        if row[att] not in temp:
                            temp[row[att]] = []
                        temp[row[att]].append(row.to_dict())
                    table_store[t] = temp
            else:
                t = annot.table2
                att = annot.att2
                if annot.annot_type == AnnotationType.ONE_TO_ONE:
                    table_store[t] = table_store[t].set_index([att]).transpose().to_dict()
                else:
                    temp = dict()
                    for index, row in table_store[t].iterrows():
                        if row[att] not in temp:
                            temp[row[att]] = []
                        temp[row[att]].append(row.to_dict())
                    table_store[t] = temp        
        
        # Creating the base event stream by transforming the event table to an EventStream
        events = EventStream()
        for index, row in table_store[self.event_table].iterrows():
            event = Event()
            for k in row.index:
                event[k] = row[k]
            for a in event_table_adj_annots:
                if a.table1 != self.event_table:
                    event[a.table1] = table_store[a.table1][event[a.att2]]
                else:
                    event[a.table2] = table_store[a.table2][event[a.att1]]
            event['time:timestamp'] = event.get(self.timestamp_key, default_timestamp)
            event['case:concept:name'] = event.get(self.case_id_key, default_case_id)
            event['concept:name'] = event.get(self.activity_key, default_activity)
            for k in event:
                if isinstance(event[k], dict) and 'value' not in event[k]:
                    temp = event[k].copy()
                    event[k] = dict()
                    event[k]['value'] = k
                    event[k]['children'] = temp
            events.append(event)

        # Convert to event log
        parameters = {
            log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: 'case:concept:name',
        }
        event_log = log_converter.apply(events, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG)
        pm4py.write_xes(event_log, f"{exported_log_name}.xes")
        return event_log

if __name__ == "__main__":
    # annotations = [Annotation("receipt_cases", "receipt_activities", "caseconceptname", "caseconceptname", AnnotationType.ONE_TO_MANY)]
    # converter = RelationalToXESConverter("receipt_activities", "conceptname", "timetimestamp", "caseconceptname", annotations)
    # db_access_params = {
    #     'host': 'localhost',
    #     'port': '5432',
    #     'user': 'postgres',
    #     'password': 'password',
    #     'dbname': 'test'
    # }
    # converter.set_db_access_params(db_access_params)
    # print(converter.build_event_log())

    db_access_params = {
        'database': './Chinook_Sqlite.sqlite',
    }

    annotations = [
        Annotation('Customer', 'Invoice', 'CustomerId', 'CustomerId', AnnotationType.ONE_TO_MANY),
        Annotation('Invoice', 'InvoiceLine', 'InvoiceId', 'InvoiceId', AnnotationType.ONE_TO_MANY)
    ]

    converter = RelationalToXESConverter('Invoice', None, 'InvoiceDate', 'CustomerId', annotations)
    converter.set_database_adapter(sqlite3)
    converter.set_db_access_params(db_access_params)
    print(converter.build_event_log("ChinookLog"))