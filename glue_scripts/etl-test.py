import unittest
import sys
import boto3
import filecmp
import time

glue = boto3.client('glue')
client = boto3.client('cloudformation')
athena = boto3.client('athena')
s3 = boto3.client('s3')

def getStackResources(stackname):
    response = client.describe_stack_resources(StackName=stackname)
    return response['StackResources']

def deleteDatabase(databasename):
    try:
        glue.delete_database(Name=databasename)
        print("Database Deleted")
    except :
        print("table "+ databasename + " did not exists")

def getcrawlerDatabase(crawlername):
    response = glue.get_crawler(Name=crawlername)
    return 	response['Crawler']['DatabaseName']

def runCrawler(crawlername):
    print("Running Crawler")
    glue.start_crawler(Name=crawlername)
    response = glue.get_crawler(Name=crawlername)
    state = response['Crawler']['State']
    while state == 'RUNNING' or state == 'STOPPING':
        time.sleep(60)
        response = glue.get_crawler(Name=crawlername)
        state = response['Crawler']['State']
    print("final state " + state)
    print("last crawl " + response['Crawler']['LastCrawl']['Status'])
    print("Crawler Ended")
    return response['Crawler']['LastCrawl']['Status']

def runJob(jobname):
    print("Running Job...")
    response = glue.start_job_run(JobName=jobname)
    jobRunid = response['JobRunId']
    response = glue.get_job_run(JobName=jobname,RunId=jobRunid)
    state = response['JobRun']['JobRunState']
    print("state " + state)
    while state == 'RUNNING':
        time.sleep(180)
        response = glue.get_job_run(JobName=jobname,RunId=jobRunid)
        state = response['JobRun']['JobRunState']
        print("state " + state)
    print("final state " + state)
    print("Job Ended")
    return state


class MyTestCase(unittest.TestCase):
    def test_data_lake(self):
        # resources_raw = getStackResources(self.STACKNAME)
        # resourcesdict = {}
        # for resource in resources_raw:
        #     resourcesdict[resource['LogicalResourceId']] = resource['PhysicalResourceId']
        # sourceDatabase = getcrawlerDatabase(resourcesdict['rawcrawler'])
        # destinationDatabase = getcrawlerDatabase(resourcesdict['datalakecrawler'])
        sourceDatabase = 'pa_technographic_data'
        destinationDatabase = 'Palo_Alto_Technographic/output-files/run-1531171472447-part-r-00002'

        #delete previous created databases
        deleteDatabase(sourceDatabase)
        deleteDatabase(destinationDatabase)

        #evaluate first crawlers
        self.assertEqual(runCrawler('pa_techno_data_crawler'), 'SUCCEEDED')

        #evaluate glue job
        self.assertEqual(runJob('pa_etl_job'), 'SUCCEEDED')

        #evaluate result crawler
        self.assertEqual(runCrawler('pa_technographic_json_crawler'), 'SUCCEEDED')

if __name__ == '__main__':
    # if len(sys.argv) > 1:
    #     MyTestCase.STACKNAME = sys.argv.pop()
    unittest.main()
