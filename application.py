#Name: Abhinav Joshi
#Course Number: CSE 6331 Section 004
#Lab Number: 5

import boto
import time
import csv
from boto.s3.key import Key
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, GlobalAllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.exceptions import JSONResponseError
from flask import Flask, request, render_template

_BUCKET_NAME = 'vehicledata'
_CSV_FILE = 'TLC_Vehicle_Insurance.csv'

application = Flask(__name__)

# Displays index page
@application.route('/')
def index():
    return render_template('index.html')

# Request to populate data from S3 to DynamoDB table
@application.route('/loaddata')
def loaddata():
    print 'Entering import_data function'
    s3_connection = boto.connect_s3('AKIAJDA26BP7XE7IY3EA','9k6IzCtN6lfPgBOIC72qvc+0oEnqnn/rBhyXGlfK')
    bucket = s3_connection.get_bucket(_BUCKET_NAME)
    file = Key(bucket)
    start = time.time()
    try:
        file.key = _CSV_FILE
        file.get_contents_to_filename(_CSV_FILE)
        table = get_table()
        populate_db(table)
        
    except Exception, exe:
        raise
    end = time.time()
    duration = end - start
    print 'Importing data from S3 to DynamoDB took {} seconds'.format(duration)
    return 'Data populated'

# Search data using parameters from Web interface.
# Also measures time taken to fetch results
@application.route('/search', methods=['POST'])
def search():
    start = time.time()
    results = search_table(request.form['type'], request.form['param'])
    duration = time.time() - start
    return render_template('results.html', duration=duration, data=results)
    
# Creates table as per the name given, if not already created     
def get_table():
    print 'Entering get_table function'
    try:
        consumer_table = Table.create('TLC_Vehicle_Insurance',
                                      schema=[HashKey('Automobile_Insurance_Policy_Number_Index')],
                                      global_indexes=[GlobalAllIndex('Policy_Indes', parts=[HashKey('Automobile_Insurance_Policy_Number_Index')]),
                                      GlobalAllIndex('Vehicle_Owner_Name', parts=[HashKey('Vehicle_Owner_Name_Index')])],
                                      connection=boto.dynamodb2.connect_to_region('us-west-2'))
        time.sleep(10)
    except JSONResponseError, resperr:
        if resperr.status == 400:
            consumer_table = Table('TLC_Vehicle_Insurance', connection=boto.dynamodb2.connect_to_region('us-west-2'))
        else:
            raise
    except Exception, exe:
        raise
    print 'Number of items in table - '  + str(consumer_table.count())
    return consumer_table

# Search function to retrieve data from DynamoDB table.
# param_type decides the table column on which querying needs to be done.
def search_table(param_type, param_value):
    print 'Entering search_table function'
    consumer_table = get_table()
    if(param_type == '1'):
        complaints = consumer_table.query_2(zip_code__eq=param_value, index='Automobile_Insurance_Policy_Number_Index')
    else:
        complaints = consumer_table.query_2(company__eq=param_value, index='Vehicle_Owner_Name_Index')

    results = []    
    for row in complaints:
        results.append(dict([('Automobile_Insurance_Policy_Number',row['Automobile_Insurance_Policy_Number']),
                             ('VIN',row['VIN']),
                             ('DMV_Plate',row['DMV_Plate']),
                             ('Vehicle_Owner_Name',row['Vehicle_Owner_Name'])
                             ]))
    return results

# Reads the csv file and import its data to DynamoDB.
def populate_db(consumer_table):
    print 'Entering populate_db function'
    csv_file = None
    try:        
        csv_file =  open(_CSV_FILE, 'rb')
        reader = csv.DictReader(csv_file)
        counter = 0
        # Insert queries are bacthed using batch_write() feature.
        with consumer_table.batch_write() as batch:
            # Reads each row in CSV. For each set of 25 records
            # batch execution is done. This is the internal limit of DynamoDB.
            for row in reader:
                try:
                    batch.put_item(data={'TLC_License_Type':row['TLC_License_Type'],
                                            'TLC_License_Number':row['TLC_License_Number'],
                                            'DMV_Plate':row['DMV_Plate'],
                                            'VIN':row['VIN'],
                                            'Automobile_Insurance_Code':row['Automobile_Insurance_Code'],
                                            'Automobile_Insurance_Policy_Number':row['Automobile_Insurance_Policy_Number'],
                                            'Vehicle_Owner_Name':row['Vehicle_Owner_Name'],
                                            'Affiliated_Base_or_Taxi_Agent_or_Fleet_License_Number':row['Affiliated_Base_or_Taxi_Agent_or_Fleet_License_Number'],
                                            })
                    counter = counter + 1
                except Exception, exe:
                    print str(exe)
                    print 'inserted - ' + str(counter)
    
    except Exception, exe:
        raise
    finally:
        if csv_file:
            csv_file.close()   

if __name__ == '__main__':
    # Setting debug to True enables debug output. This line should be
    # removed before deploying a production app.
    application.debug = False
    application.run()

