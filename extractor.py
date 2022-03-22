import psycopg2
import copy
import uuid
import pandas as pd
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.exporter.xes import exporter as xes_exporter

class Extractor:
    DUPLICATE_MARKER = "@@duplicate"

    def __init__(self, case_table, activity_table, case_id_key, activity_id_key, timestamp_id_key, annotations):
        self.case_table = case_table
        self.activity_table = activity_table
        self.case_id_key = case_id_key
        self.activity_id_key = activity_id_key
        self.timestamp_id_key = timestamp_id_key
        self.annotations = annotations # 5-tuples of (table1, table2, att1, att2, annot_type)
        self.view_name = 'a'+uuid.uuid4().hex
        self.col_names = dict()
        self.case_columns = []

    def get_case_columns(self, db_acces_params):
        conn = psycopg2.connect(**db_acces_params)
        cur = conn.cursor()
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{self.case_table}';")
        self.case_columns = cur.fetchall()
        self.case_columns = [x[0] for x in self.case_columns]
        cur.close()

    def get_table_columns(self, db_access_params):
        conn = psycopg2.connect(**db_access_params)
        cur = conn.cursor()
        for annot in self.annotations:
            t1, t2 = annot[0], annot[1]
            cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{t1}';")
            self.col_names[t1] = cur.fetchall()
            self.col_names[t1] = [x[0] for x in self.col_names[t1]]
            cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{t2}';")
            self.col_names[t2] = cur.fetchall()
            self.col_names[t2] = [x[0] for x in self.col_names[t2]]
        conn.close()

    def get_extract_query(self):
        '''
        Build an SQL query to extract a tabular event log, given the annotations
        '''
        annotations = copy.deepcopy(self.annotations)
        query = ""
        counter = 0
        while len(annotations) > 0:
            annot = annotations.pop()
            table1, table2, att1, att2, annot_type = annot
            if annot_type == "1-1":
                new_query = f"WITH table{counter} AS (\
                \n\tSELECT *\
                \n\tFROM {table1} JOIN {table2}\
                \n\tON {table1}.{att1} = {table2}.{att2}\
                \n)\n\n"
            elif annot_type == "1-n":
                new_query = f"WITH table{counter} AS (\
                \n\tSELECT *\
                \n\tFROM {table1} RIGHT JOIN {table2}\
                \n\tON {table1}.{att1} = {table2}.{att2}\
                \n)\n\n"
            query += new_query
            for x in annotations:
                if x[0] == table1 or x[0] == table2:
                    x[0] = f"table{counter}"
                if x[1] == table1 or x[1] == table2:
                    x[1] = f"table{counter}"
            counter += 1
        last_table = f"table{counter-1}"
        query += f"SELECT * FROM table{counter-1};"
        return query

    def extract(self, db_access_params):
        '''
        Extract the tabular log
        return the tabular log with its column names for further processing
        '''
        conn = None
        try:
            self.get_table_columns(db_access_params)
            self.get_case_columns(db_access_params)
            conn = psycopg2.connect(**db_access_params)
            cur = conn.cursor()
            query = self.get_extract_query()
            cur.execute(query)
            data = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            unique_cols = set()
            duplicate_cols = []
            for i in range(len(colnames)):
                col = colnames[i]
                if col not in unique_cols:
                    unique_cols.add(col)
                else:
                    colnames[i] += Extractor.DUPLICATE_MARKER
                    duplicate_cols.append(colnames[i])
            df = pd.DataFrame(data=data, columns=colnames)
            df = df.drop(columns=duplicate_cols)
            df.sort_values(by=self.timestamp_id_key)
            parameters = {
                log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: 'caseconceptname',
                log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ATTRIBUTE_PREFIX: 'case'
            }
            event_log = log_converter.apply(df, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG)
            xes_exporter.apply(event_log, './exported.xes')
            if conn is not None:
                conn.close()
                print('Database connection closed.')
            return event_log
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            

if __name__ == "__main__":
    # Test 1: Extract data from given the example csvs
    annotations = [
        ["receipt_cases", "receipt_activities", "caseconceptname", "caseconceptname", "1-n"]
    ]
    extractor = Extractor("receipt_cases", "receipt_activities", "caseconceptname", "conceptname", "timetimestamp", annotations)
    df = extractor.extract({
        'host': 'localhost',
        'port': '5432',
        'user': 'postgres',
        'password': 'password',
        'dbname': 'test'
    })
    print(df)

    # Test 2: Complex sql query
    # annotations2 = [
    #     ['TA', 'TB', 'A1', 'B1', '1-n'],
    #     ['TB', 'TC', 'B2', 'C1', '1-1'],
    #     ['TC', 'TD', 'C2', 'D1', '1-n'],
    #     ['TE', 'TC', 'E1', 'C3', '1-n']
    # ]
    # extractor2 = Extractor('TA', 'TC', annotations2)
    # print(extractor2.get_extract_query())