#!/usr/local/bin/python3.7
__author__ = 'DE team'

# importing modules
import os
import sys
import argparse
import logging
import time
from datetime import datetime
import subprocess
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

# Agruments parsing:
parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, \
description='This script is used to load data files directly to Snowflake.\n\
Only requirement is to have the data file and snowflake table same number of fields/columns in same order.')
parser.add_argument("data_file_name", help="Data File Name without extension.")
parser.add_argument("snowflake_schema_name", help="snowflake schame name for the table.")
parser.add_argument("tgt_table_name", help="Snowflake Target Table Name.")
parser.add_argument("-sfdb","--sfdb",dest="snowflake_database",help="Snowflake database name. Default: DEFAULT_WH.",default="DEFAULT_DB")
parser.add_argument("-dfext","--dfext",dest="data_file_extension",help="Datafile extension. Default: dat.",default="DEFAULT_DFEXT")
parser.add_argument("-hdrln","--hdrln",dest="header_lines",help="Indicate the number of header lines to skip. Default is zero, means no skipping.Default: 0 (no exit)",type=int,default=0)
parser.add_argument("-sff","--sff",dest="snowflake_file_format",help="Snowflake file format to load data into table. Default: SHARED_OBJECTS.ODI_CSV_UTF8_BEL_ENQ.",default="DEFAULT_FF")
parser.add_argument("-dtfrmt","--dtfrmt",dest="date_format",help="Date format for Snowflake.Default: yyyy-MM-dd.",default="DEFAULT")
parser.add_argument("-tsfrmt","--tsfrmt",dest="time_stamp_format",help="Timestamp format for Snowflake.Default: yyyy-MM-dd-HH24:MI:SS.",default="DEFAULT")
parser.add_argument("-dd","--dd",dest="data_directory_name",help="Directory where ODI will create the data files. Default: /xxsw_interfaces/obid/snowflake_files/temp_files.",default="DEFAULT")
parser.add_argument("-sfd","--sfd",dest="snowflake_home_directory",help="Snowflake code home directory. Default: /swdata/odi_data/xxsw_snowflake.",default="/swdata/odi_data/xxsw_snowflake")
parser.add_argument("-sfu","--sfu",dest="snowflake_user",help="Snowflake user to conenct to the database. Default: ODI_ETL.",default="DEFAULT_USER")
parser.add_argument("-sfp","--sfp",dest="snowflake_password",help="Snowflake user password to conenct to the database. Default: DEFAULT_PASSWORD.",default="DEFAULT_PASSWORD")
parser.add_argument("-sfrl","--sfrl",dest="snowflake_role",help="Snowflake role. Default: DEFAULT_WH_DEVELOPER_ROLE.",default="DEFAULT_ROLE")
parser.add_argument("-sfwh","--sfwh",dest="snowflake_warehouse",help="Snowflake warehouse. Default: DEFAULT_WH.",default="DEFAULT_WH")
parser.add_argument("-sfstg","--sfstg",dest="snowflake_stage",help="Snowflake stage. Default: ODI_STAGE.STAGE_FILES.",default="DEFAULT_STG")
parser.add_argument("-odisrvr","--odisrvr",dest="odi_server", help="ODI server name.Default: odi server1",default=os.uname().nodename.split('.')[0])
parser.add_argument("-ddf","--ddf",dest="delete_data_files_on_success",help="1 to keep the data file after successful data load.Default: 0 (delete data files after successful load.)",default=0)
parser.add_argument("-lfn","--lfn",dest="log_file_name",help="Log file name.Default: file_to_snowflake_sync.log.",default="DEFAULT")
args = parser.parse_args()

# Check ODI Server
list_odi_server=['odi server1','odi server2]
if(args.odi_server not in list_odi_server):
    print(os.uname().nodename.split('.')[0],": ODI Server not in the list.")
    sys.exit(1)

# check directory structure
v_log_directory=args.snowflake_home_directory+"/logs"

if not os.path.exists(v_log_directory):
    print("\nLog directory does not exists. Please check. Log Directory:",v_log_directory)
    sys.exit(1)

#working directory for the script
os.chdir(v_log_directory)

# Set Default values for Snowflake Data Directory
if(args.data_directory_name=="DEFAULT"):
    if(args.odi_server=="odi server1"):
    else:
        v_data_directory_name="/xxsw_interfaces/obid/snowflake_files/temp_files"
else:
     v_data_directory_name=args.data_directory_name

# Set Default values:
if(args.log_file_name=="DEFAULT"):
    v_log_file_name='fts_'+args.snowflake_schema_name+"_"+args.tgt_table_name+'.log'
else:
    v_log_file_name=args.log_file_name

if(args.date_format=="DEFAULT"):
    v_date_format='yyyy-MM-dd'
else:
    v_date_format=args.date_format

if(args.time_stamp_format=="DEFAULT"):
    v_time_stamp_format='yyyy-MM-dd-HH24:MI:SS'
else:
    v_time_stamp_format=args.time_stamp_format

if(args.data_file_extension=="DEFAULT_DFEXT"):
    v_data_file_extension='dat'
else:
    v_data_file_extension=args.data_file_extension

if(args.snowflake_user=="DEFAULT_USER"):
    v_sf_user='ODI_ETL'
else:
    v_sf_user=args.snowflake_user.upper()

if(args.snowflake_file_format=="DEFAULT_FF"):
    v_sf_file_format='SHARED_OBJECTS.ODI_CSV_UTF8_BEL_ENQ'
else:
    v_sf_file_format=args.snowflake_file_format

if(args.snowflake_stage=="DEFAULT_STG"):
    v_sf_stage='ODI_STAGE.STAGE_FILES'
else:
    v_sf_stage=args.snowflake_stage


#Set Snowflake DB, Warehouse and Role.
v_sf_database=args.snowflake_database
v_sf_warehouse=args.snowflake_warehouse
v_sf_role=args.snowflake_role

logging.basicConfig(filename=v_log_directory+"/"+v_log_file_name,level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
logging.info("**********************************************************************************************************************")
logging.info("*************************************************Starting now : %s****************************",datetime.now())
logging.info("**********************************************************************************************************************")
logging.info("Python script hostname: %s",os.uname().nodename.split('.')[0])
logging.info("ODI Server: %s",args.odi_server)
logging.info("Snowflake Database: %s",v_sf_database)
logging.info("Snowflake Schema Name: %s",args.snowflake_schema_name)
logging.info("Snowflake Table Name: %s",args.tgt_table_name)
logging.info("Data File Directory: %s",v_data_directory_name)
logging.info("Data File Name: %s",args.data_file_name)
logging.info("Data File Extension: %s",v_data_file_extension)
logging.info("Number of header lines to skip: %s",args.header_lines)
logging.info("Date Format: %s",v_date_format)
logging.info("Timestamp Format: %s",v_time_stamp_format)
logging.info("Snowflake User: %s",v_sf_user)
logging.info("Snowflake Role: %s",v_sf_role)
logging.info("Snowflake Warehouse: %s",v_sf_warehouse)
logging.info("Snowflake File Format: %s",v_sf_file_format)
logging.info("Snowflake Stage: %s",v_sf_stage)
logging.info("**********************************************************************************************************************\n")

return_value=0
v_table_name=args.tgt_table_name
v_data_extract_flag='N'
v_data_file_name=args.data_file_name
v_full_data_file_name=v_data_directory_name+"/"+v_data_file_name+"."+v_data_file_extension
if not os.path.exists(v_full_data_file_name):
    logging.error("Data File does not exists. Please check. Data File:%s",v_full_data_file_name)
    sys.exit(1)

# set logging level for the azure storage to CRITICAL, as we don't need lot of message at storage level.
for logger_name in ['azure.storage','snowflake.connector.connection']:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.CRITICAL)

#define snowflake operations/queris
sf_truncate_table="truncate table {table_name}"
sf_put="put file://{data_file_directory}/{data_file_name}.{data_file_type}.gz @{stage_name} AUTO_COMPRESS=FALSE PARALLEL={putparalellism}"
sf_copy="COPY INTO {table_name} FROM @{stage_name}/{data_file_name}.{data_file_type}.gz FILE_FORMAT=(FORMAT_NAME='{fileformat}' SKIP_HEADER={skipheader} DATE_FORMAT='{dateformat}' TIMESTAMP_FORMAT='{timestampformat}')"
sf_purge_files="{purgestagefiles} @{stage_name}/{data_file_name}.{data_file_type}.gz"

# Check Snowflake connection and schema before moving forward
with open("/swdata/odi_data/xxsw_snowflake/scripts/snowflake_keys/rsa_key.p8","rb") as key:
    v_private_key=serialization.load_pem_private_key(
                  key.read(),
                  password= <SF Password>,
                  backend=default_backend())

pkb=v_private_key.private_bytes(
                  encoding=serialization.Encoding.DER,
                  format=serialization.PrivateFormat.PKCS8,
                  encryption_algorithm=serialization.NoEncryption())
try:
    logging.info("**************************************** checking snowflake connection ***********************************************\n")
    if (v_sf_user=='ODI_ETL'):
        sf_con = snowflake.connector.connect(
                user=v_sf_user,
                private_key=pkb,
                account='sherwin1.east-us-2.azure',
                warehouse=v_sf_warehouse,
                database=v_sf_database,
                schema=args.snowflake_schema_name,
                role=v_sf_role,
                timezone='America/New_York',
                client_session_keep_alive=True,
                autocommit=True)
    else:
        sf_con = snowflake.connector.connect(
                user=v_sf_user,
                password=args.snowflake_password,
                account='sherwin1.east-us-2.azure',
                warehouse=v_sf_warehouse,
                database=v_sf_database,
                schema=args.snowflake_schema_name,
                role=v_sf_role,
                timezone='America/New_York',
                client_session_keep_alive=True,
                autocommit=True)
    # Create cursors from the snowflake connection to check schema exists
    try:
        sf_cursor = sf_con.cursor()
        for (name) in sf_cursor.execute("use schema "+args.snowflake_schema_name):
            logging.info('{0}'.format(name))
    except snowflake.connector.errors.ProgrammingError as e:
        print('Error: {0}'.format(e.msg))
        logging.error('Error: {0}'.format(e.msg))
        sf_con.close()
        sys.exit(1)
    logging.info("**************************************** finished checking snowflake connection **************************************\n")
except snowflake.connector.errors.DatabaseError as e:
    print('Error: {0}'.format(e.msg))
    logging.error('Error: {0}'.format(e.msg))
    sys.exit(1)


v_data_file_size = os.stat(v_full_data_file_name).st_size
logging.info("Data file size(Bytes) unzipped: %s",v_data_file_size)
if(v_data_file_size>0):
    logging.info("Data file size(MB) unzipped: %s",round(v_data_file_size/(1024*1024)))
    if (os.path.exists(v_full_data_file_name+".gz")):
        os.remove(v_full_data_file_name+".gz")
        logging.warning("Older Zipped file removed.")
    gzip_command="gzip -1q "+v_full_data_file_name
    gz_result=subprocess.run(gzip_command,shell=True,universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if (gz_result.returncode!=0):
        logging.error("Process failed while zipping the data file.")
        logging.error("Issue during zipping: %s",gz_result.stderr)
        return_value=1
    else:
        v_data_file_size_mb = round(os.stat(v_full_data_file_name+".gz").st_size/(1024*1024))
        if(v_data_file_size_mb >= 1024):
            v_parallelism=8
        elif(v_data_file_size_mb >= 50):
            v_parallelism=4
        elif(v_data_file_size_mb >= 25):
            v_parallelism=2
        else:
            v_parallelism=1
        v_data_extract_flag = 'Y'
        logging.info("Data file zipped successfully. Data file size(MB) zipped: %s",v_data_file_size_mb)
        logging.info("put command parallelism: %s",v_parallelism)
else:
    logging.warning("Zero bytes data file. Nothing to load into snowflake.")
    if (args.delete_data_files_on_success==0 and os.path.exists(v_full_data_file_name)):
        os.remove(v_full_data_file_name)
        logging.info("Data file removed.")

if (v_data_extract_flag=='Y'):
    logging.info("Loading data in Snowflake.")
    try:
        sf_cursor.execute(sf_truncate_table.format(table_name = v_table_name))
        sf_cursor.execute(sf_put.format(data_file_directory = v_data_directory_name, data_file_name = v_data_file_name, data_file_type = v_data_file_extension, stage_name = v_sf_stage, putparalellism = v_parallelism))
        sf_cursor.execute(sf_copy.format(table_name = v_table_name, stage_name = v_sf_stage, data_file_name = v_data_file_name, data_file_type = v_data_file_extension, fileformat = v_sf_file_format, dateformat=v_date_format, timestampformat = v_time_stamp_format,skipheader=args.header_lines))
        sf_cursor.execute(sf_purge_files.format(purgestagefiles = "remove", stage_name = v_sf_stage, data_file_name = v_data_file_name, data_file_type = v_data_file_extension))
        logging.info("Data loaded successfully")
        if (args.delete_data_files_on_success==0 and os.path.exists(v_full_data_file_name+".gz")):
            os.remove(v_data_directory_name+"/"+v_data_file_name+".dat.gz")
            logging.info("Data file removed.")
        sf_con.close()
    except snowflake.connector.errors.ProgrammingError as sf_error:
        logging.error("snowflake errno: {0}".format(sf_error.errno))
        logging.error("snowflake msg: {0}".format(sf_error.raw_msg))
        logging.error("snowflake sqlstate: {0}".format(sf_error.sqlstate))
        logging.error("snowflake sfqid: {0}".format(sf_error.sfqid))
        sf_con.close()
        return_value=1
    except:
        logging.error("Issue during data load into snowflake. Please check log file.")
        sf_con.close()
        return_value=1
    finally:
        sys.exit(return_value)
