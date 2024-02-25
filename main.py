import boto3
import pandas as pd
import datetime
import psycopg2
import environ
from pandasql import sqldf
import time
import json
from smtplib import SMTP_SSL
from email.message import EmailMessage
from pretty_html_table import build_table

file=open("config.json")
config=json.load(file)

s3 = boto3.resource(
    service_name="s3",
    region_name="us-east-1",
    aws_access_key_id=config['aws_access_key_id'],
    aws_secret_access_key=config["aws_secret_access_key"]
)

# for buckets in s3.buckets.all():
#   print(buckets.name)

# for objects in s3.Bucket('automationtesting2509').objects.all():
#    print(objects.key)

currdate = datetime.datetime.now().strftime("%Y%m%d")
d1 = int(currdate) - 1
prevdate = str(d1)
#print(prevdate)
fileTotal=[]
print("-------------------------File Count-------------------------------------------------------")
for objects in s3.Bucket('automationtesting2509').objects.all():
    if (objects.key.__contains__(currdate)):
        if(objects.key.__contains__("csv")==False):
            data = s3.Object('automationtesting2509', objects.key).get()
            df = pd.read_csv(data['Body'], index_col=0)
            # print(df)
            total_rows = df.count()[1]
            print(f"{objects.key} has {total_rows} rows")
            fileTotal.append((objects.key,total_rows))



    #Quering in that file with sql query for validation
    # try:
    #    print(sqldf('''SELECT *  FROM df where id=1953'''))
    # except Exception as e:
    #   print(f"Error in file {objects.key}. The error is {e}")

print("-------------------------Athena Count-------------------------------------------------------")
# Getting snapshot details
# def getTableName(database):
#     client = boto3.client('glue',
#                           region_name='us-east-1',
#                           aws_access_key_id="AKIA4EEJ6XBM3IE234TB",
#                           aws_secret_access_key="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"
#                           )
#
#     responseGetTables = client.get_tables(DatabaseName=database)
#     tableList = responseGetTables['TableList']
#     for tableDict in tableList:
#         tableName=tableDict['Name']
#         if(tableName.__contains__(currdate)):
#             print('Snapshot name: ' + tableName)
#             return tableName


# athena count
athenaTotal=[]
output = "s3://glue-test-08/output/"

athena_client = boto3.client('athena',
                             region_name="us-east-1",
                             aws_access_key_id=config['aws_access_key_id'],
                             aws_secret_access_key=config["aws_secret_access_key"]
                             )
print("connected")


def get_athena_count(query, database):
    count = 0
    response_query_execution_id = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': output,
        }
    )

    response_get_query_details = athena_client.get_query_execution(
        QueryExecutionId=response_query_execution_id['QueryExecutionId'])
    #print(response_get_query_details)
    status = 'Running'
    # check for query execution status in every seconds for 120 seconds
    t = 120
    while (t > 0):
        t = t - 1
        response_get_query_details = athena_client.get_query_execution(
            QueryExecutionId=response_query_execution_id['QueryExecutionId'])
        status = response_get_query_details['QueryExecution']['Status']['State']
        print(status)
        if (status == "FAILED") or (status == "CANCLED"):
            print("Job Failed")
            return "JOb Failed"

        elif (status == "SUCCEEDED"):
            count=1
            location = response_get_query_details['QueryExecution']['ResultConfiguration']['OutputLocation']
            response_query_result = athena_client.get_query_results(
                QueryExecutionId=response_query_execution_id['QueryExecutionId'])
            # print("lacation "+location)
            #print(response_query_result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
            return response_query_result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
            break
        else:
            time.sleep(1)
    if(count==0):
        print("Time out")
# Connecting with glue to take database and today's created snapshot table info to pass into get_athena_count() function to genarate output
client = boto3.client('glue',
                          region_name='us-east-1',
                          aws_access_key_id=config['aws_access_key_id'],
                          aws_secret_access_key=config["aws_secret_access_key"]
                          )
responseGetDatabases = client.get_databases()
databaseList = responseGetDatabases['DatabaseList']
for databaseDict in databaseList:
    databaseName = databaseDict['Name']
    #print ('\ndatabaseName: ' + databaseName)
    responseGetTables = client.get_tables( DatabaseName = databaseName )
    tableList = responseGetTables['TableList']
    for tableDict in tableList:
         tableName = tableDict['Name']
         #print ('\n-- tableName: '+tableName)
         if (tableName.__contains__(currdate)):
             #print('Snapshot name: ' + tableName)
             query = f'''
                 SELECT count(*) FROM {databaseName}.{tableName};
                 '''
             r = get_athena_count(query, databaseName)
             print(tableName + " snapshot has " + r + " counts for today ")
             athenaTotal.append((tableName,r))

print("-------------------------PostgreSQl Count-------------------------------------------------------")
postTotal=[]
# Connecting to postgre table
# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    user='postgres',
    password=config['dbpass'],
    database='autotest'
)
# Create a cursor object to execute SQL commands
cursor = conn.cursor()
cursor.execute('''SELECT tablename
                FROM pg_catalog.pg_tables
                WHERE schemaname != 'pg_catalog' AND 
                schemaname != 'information_schema'
                order by tablename;'''
               )
table=[]
for i in cursor:
    table.append(str(i[0]))
#print(table)
for t in table:
    query = f'''select count(*) from {t};'''
    cursor.execute(query)
    result=str(cursor.fetchall()[0][0])
    print(t+" has "+result+" no of records")
    postTotal.append((t,result))

filecolumns = ["File Name","File count"]
athenacolumns=["Snapshot Name","Athena count"]
postcolumns=["Landing Table Name","Landing Table Count"]
coulumns=["File Name","Snapshot Name","Landing Table Name","File count","Athena count","Landing Table Count"]
df=pd.DataFrame(data = fileTotal, columns =filecolumns)
df[athenacolumns]=athenaTotal
df[postcolumns]=postTotal
df=df.reindex(columns=coulumns)
df=df.reset_index(drop=True,inplace=False)
# print(fileTotal)
# print(athenaTotal)
#print(df.to_string())

#Sending The mail

sender_mail="1981sankarsarkar@gmail.com"
password=config["gmail"]
reciever_mail="2000ankushseal@gmail.com"

mail_body=f"""
Hi Team,<br>
We have validated the AA files for {datetime.datetime.now().strftime("%Y/%m/%d")}. All details are;
{format(build_table(df,"blue_light"))}

Regards;<br>
Ankush seal
"""
subject=f"""AA file count for {datetime.datetime.now().strftime("%Y/%m/%d")}"""
def send_mail(sender_mail,reciever_mail,password,subject,mail_body):
    msg=EmailMessage()
    msg["From"]=sender_mail
    msg["To"]=reciever_mail
    #msg["CC"]="abc@g.com,abc@g.com"
    msg["Subject"]=subject
    msg.add_alternative(mail_body,subtype="html")
    with SMTP_SSL("smtp.gmail.com",465) as smtp:
        smtp.login(sender_mail,password)
        smtp.send_message(msg)
        smtp.close()

send_mail(sender_mail,reciever_mail,password,subject,mail_body)
print("Mail Sent")