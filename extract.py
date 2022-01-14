#!/usr/bin/python3
#################################################################################################################
#    EPCR EXTRACT PROGRAM - VERSION 1.0                                                           2021-12-30    #
#                                                                                                               #
#    THIS PROGRAM IS DESIGNED TO RUN WITH ZOLL RESCUENET EPCR VERSION 6.5 (THOUGH IT WILL LIKELY WORK WITH      #
#    EARLIER AND LATER VERSIONS THAT CONTAIN AN EXTRACT DB. USE YOUR OWN VARIABLES FOR MS SQL ACCESS BELOW      #
#    AND MAKE SURE YOU HAVE INSTALLED THE MSSQL DRIVER AND TOOLS FOR YOUR VERSION OF LINUX. THIS PROGRAM IS     #
#    CAPABLE OF EXTRACTING 12K XML FILES PER HOUR. YOU MAY BE ABLE TO EXTRACT MORE DEPENDING ON YOUR SETUP      #
#    AND UTILIZING MULTIPROCESSING LIBRARY FOR PYTHON, BUT THIS WILL ALSO HIT YOUR DB SERVERS HARDER. THIS      #
#    PROGRAM CAN EXTRACT A SINGLE PCR BASE ON RUN NUMBER (DEFAULT TO MOST RECENT PCR FOR THAT RUN NUMBER) OR    #
#    RUN NUMBER AND YEAR TO GET SPECIFIC RUN IN A YEAR. CAN ALSO BE DONE WITH A RUN NUMBER RANGE OR SPECIFIC    #
#    RUN NUMBERS LOADING INTO A COMMA SEPARATED VALUE (CSV) FILE. THIS PROGRAM DOES NOT REGENERATE A NEW XML    #
#    LIKE BATCH EXPORTING DOES, SO THE PCR MUST HAVE AN ASSOCIATED EXTRACT XML IN THE DATABASE TO FUNCTION.     #
#################################################################################################################
#

import pyodbc
import re
import sys
import argparse
import csv
import datetime
import signal
import readchar

#################################################################################################################
# CHANGE ONLY VARIABLES BELOW THIS LINE:                                                                        #
#################################################################################################################

server = 'tcp:(Insert Server Hostname)' 
database = 'RCSql' 
username = '(Insert Username)' 
password = '(Insert Password)' 
DRIVER = 'ODBC Driver 17 for SQL Server'            #DON'T CHANGE THIS UNLESS YOUR MSSQL DRIVER NAME IS DIFFERENT
Output = ''                                         #DEFAULT OUTPUT DIRECTORY
#################################################################################################################
# CHANGE ONLY VARIABLES ABOVE THIS LINE:                                                                        #
#################################################################################################################

start_time = datetime.datetime.now()

def SignalHandler(signum, frame):
    print("Control-C was pressed. Do you want to end XML extraction? (Y/N) ", end="", flush=True)
    input = readchar.readchar()
    if (input.upper() == 'Y'):
        print('Execution Halted by User.')
        end_time = datetime.datetime.now()
        exec_time = (end_time - start_time)
        print(f'Total execution time: {exec_time}')
        sys.exit(1)


def main():
    #Load system variables passed from command line
    parser = argparse.ArgumentParser()
    parser.add_argument("-y", "--year", help="Two/Four Digit Year from which to obtain records (no value = most recent)", required=False)
    parser.add_argument("-r", "--run", help="Run Number or Run Number Range (Dash Separator: 10000-20000). Required unless using CSV.", required=False)
    parser.add_argument("-c", "--csv", help="Use specified Comma-Separated Value (CSV) file", required=False)
    parser.add_argument("-o", "--output", help="Output XML files into specified directory", required=False)
    args = parser.parse_args()
    CSV_File = ''
    Date = ''
    global Output

    if ((args.year)):
        if (len(args.year) in (2,4)):           #CHECK IF YEAR VARIABLE WAS PASSED
            if len(args.year) == 2:                             #2 DIGIT YEAR
                print ("YR")
                Date="20"+args.year+"-01-01"
            elif len(args.year) == 4:                           #4 DIGIT YEAR
                print("YEAR")
                Date = args.year+"-01-01"
        elif (re.search('\d{4}-\d{1,2}-\d{1,2}', args.year)): #IF FULLY QUALIFIED DATE WAS PASSED, USE THAT.
            Date = str(args.year)
        else:                                       #IF THERE IS A DATE THAT DOES NOT MATCH THE EXPECTED FORMAT
            print('Invalid year/date format. Proceeding without year specified.')

    if (args.run):                                          #CHECK IF RUN NUMBER VARIABLE WAS PASSED
        if (re.search('\-', args.run)):
            RunNumber = args.run.split("-")                 #IF RUN NUMBER IS A RANGE
            if (int(RunNumber[0]) < int(RunNumber[1])):                                 #IF FIRST NUMBER IS LESS THAN SECOND
                RunNumber = list(range(int(RunNumber[0]), int(RunNumber[1])))           #POPULATE ARRAY WITH RANGE
            elif (int(RunNumber[0]) > int(RunNumber[1])):                               #IF FIRST NUMBER IS GREATER THAN SECOND (WHY!?)
                RunNumber = list(range(int(RunNumber[1]), int(RunNumber[0])))           #POPULATE ARRAY WITH RANGE
            elif (int(RunNumber[0]) == int(RunNumber[1])):                              #IF RANGE IS EXACTLY THE SAME (WHY!?!)
                RunNumber = RunNumber[0]
        elif (re.search('\d+', args.run)):                                               #IF RUN NUMBER IS JUST A RUN NUMBER
            RunNumber = str(args.run)
        else:
            print ('ERROR: Invalid value for Run Number! Use only numeric value or contact system administrator.')
            exit(1)
    else:
        if (not(args.csv)):
            print('ERROR: Program requires target run number, run number range, or CSV file containing run numbers in order to run.')
            exit(1)

    if (args.csv):
        with open(args.csv) as CSV:
            file_read = csv.reader(CSV)
            RunNumber = list(file_read)
            RunNumber = list(RunNumber[0])
            
    if (args.output):
        Output = str(args.output)

    #INITIATE DATABASE CONNECTION
    cnxn = pyodbc.connect('DRIVER={'+DRIVER+'};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()

    count = 0
    for Run in RunNumber:
        Run = str(Run).strip()
        count+=1
        if (Date):
            SQL_GetPCRIds = "SELECT [RunNumber],[g2pcrid] FROM [RCSql].[dbo].[FDC_Trips] where RunNumber ='"+Run+"' AND tdate > '"+Date+"'"
        else:
            SQL_GetPCRIds = "SELECT TOP 1 [RunNumber],[g2pcrid] FROM [RCSql].[dbo].[FDC_Trips] where RunNumber ='"+Run+"' order by g2pcrid desc"
        cursor.execute(SQL_GetPCRIds)
        result = cursor.fetchone()
        if (result):
            PCR_Id = str(result[1])
            print (PCR_Id)
            SQL_STATEMENT = "SELECT CAST(ResultDocument as varchar(max)) as xmlResult,* FROM [Extract].[Extract].[BatchResults] batchResult, Extract.Extract.BatchObjects batchObject where batchResult.BatchKey = batchObject.BatchKey and ZollObjectKey like '%"+PCR_Id+"%'"
            cursor.execute(SQL_STATEMENT)
            row = cursor.fetchone()
            if (row):
                XML_STRING = str(row[0])
                XML_STRING = re.sub('&lt;', '<', XML_STRING)
                XML_STRING = re.sub('&gt;', '>', XML_STRING)
                XML_STRING = XML_STRING.split("<EMSDataSet", 1)
                XML_STRING = XML_STRING[1].split("</string>")
                XML_STRING = "<EMSDataSet" + XML_STRING[0]
                try:
                    output_file = open((Output + Run + ".xml"), "w")
                    output_file.write(XML_STRING)
                    output_file.close()
                except:
                    print(f'Unable to write to file: {Output}{Run}.xml - Check path or folder permissions.')
                    exit(1)
                print(f'Extracting {PCR_Id} as {Run} - ' + str(len(RunNumber) - count) + ' remaining in job.')
            else:
                print(f"Extract Not Found for PCR ID {PCR_Id} / Run Number {Run}")
        else:
            print(f'Unable to find matching PCR ID for Run Number {Run}')
                
    end_time = datetime.datetime.now()
    exec_time = (end_time - start_time)
    print(f'Total execution time: {exec_time}')
    
if __name__ == '__main__':   #Name Guard
    signal.signal(signal.SIGINT, SignalHandler)
    main()
else:
    print('This program not meant for import!')
    exit(1)
