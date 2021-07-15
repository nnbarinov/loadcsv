# script for automated loading of files into APRM
# Created by Nikolai Barinov nnbarin2@mts.ru

import cx_Oracle as ora
import pandas as pd
import logging
import time
import shutil
from datetime import datetime
import configparser
import sys
import os
import subprocess 

read_config = configparser.ConfigParser()
read_config.read(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini"))
cur_dt_for_log = datetime.now().strftime('%Y%m%d')
log_path = read_config.get("MAIN_CONFIG", "files_log_path")
rd_log_path = read_config.get("MAIN_CONFIG", "rd_log_path")
max_rows = int(read_config.get("MAIN_CONFIG", "max_rows"))
min = int(read_config.get("MAIN_CONFIG", "min"))
max_f = int(read_config.get("MAIN_CONFIG", "max_f"))
max_r = int(read_config.get("MAIN_CONFIG", "max_r"))
dirs = read_config.get("MAIN_CONFIG", "dir_list")
disk_n = read_config.get("MAIN_CONFIG", "disk_n")
logname = log_path + '{}_move.log'.format(cur_dt_for_log)
logging.basicConfig(filename=logname,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)
logging.info("==============START UTILITY==============")
log = logging.getLogger('LOG')

# a function to start the RELEASE DAILY procedure and check if it succeeded
def release_daily():
    dirname = rd_log_path
    filenames = os.listdir(dirname)
    filepaths = [os.path.join(dirname, filename) for filename in filenames]
    files = [ f for f in filepaths if not os.path.isdir(f) and '.draft_imp' in f] # find dump import log files
    newest = max(files, key=lambda x: os.stat(x).st_mtime) # determining the newest dump log file
    log_status = False
    time.sleep(1)
    with open(newest, "r") as f:
        for line in f.readlines():
            if "Import terminated successfully without warnings." in line:
                    log_status = True
                    log.info("Release daily complete. Import terminated successfully without warnings.")
    f.close()
    return 	log_status 

# a function to determine if assessment processes are working
def check_proc():
    try:
        subprocess.check_output(
            "./check_proc.sh",
            stderr=subprocess.STDOUT,
            shell=True)
    except:
        return "Error"

# a function for creating a two-dimensional list with directories from where and where to transfer files
def get_directory_list(dirs):
    ax2 = []
    dirs =  dirs
    ax1 = dirs.replace(' ', '').split('|')
    for i in ax1:
        ax2.append(i.split('<'))
    return ax2

# function to check free space on a partition
def get_space(disk_n):
    du = shutil.disk_usage(disk_n)
    if du[1]/du[0] < 0.8:
        return True
    else:
        return False

# a function to fetch and check logical date	
def select_logic_date():
    sql = """select case
				when extract(day from d.logical_date) between 1 and 4 then
				'error'
				else
				',' || to_char(trunc(d.logical_date, 'MONTH'), 'yyyymmdd') || ','
				end logic_date
				from LOGICAL_DATE d
			where d.logical_date_type = 'B' """
    try:
        myconnection = ora.connect(read_config.get("MAIN_CONFIG", "login"), read_config.get("MAIN_CONFIG", "password"),read_config.get("MAIN_CONFIG", "dsn"), encoding="UTF-8")
        df1 = pd.read_sql(sql, myconnection)
        if (df1.iat[0,0]) == 'error':
            log.exception("wrong logical date")
            sys.exit()
        else:
            return (df1.iat[0,0])
    except:
        log.exception("error when selecting logical date")
        sys.exit()

# a function to check the number of files and records in progress
def get_files_lim():
    sql = """select nvl(sum(m.NUMBER_OF_FILES), 0) as FILES_QTY,
             nvl(sum(m.NUMBER_OF_RECORDS), 0) as RECORDS_QTY
             from MTS_FILE_STATUS_V m where m.FILE_STATUS != 'AF' """
    try:
        myconnection = ora.connect(read_config.get("MAIN_CONFIG", "login"), read_config.get("MAIN_CONFIG", "password"),read_config.get("MAIN_CONFIG", "dsn"), encoding="UTF-8")
        df2 = pd.read_sql(sql, myconnection)
        log.info(str(df2.iat[0,0]) + " files are in the queue with " + str(df2.iat[0,1]) + " rows" )
        if df2.iat[0, 0] == 0 and df2.iat[0, 1] == 0:
            return 0		
        elif df2.iat[0, 0] < max_f and df2.iat[0, 1] < max_r:
            return 1
        else:
            return 2
    except:
        log.exception("error when selecting data from MTS_FILE_STATUS_V")
        sys.exit()

# a	function to create a list of files suitable for upload	
def make_file_list(in_path):
    in_path = in_path
    file_list = []
    all_rows = 0
    in_rows = 0
    i = 0
    global n_rows
    n_rows = 0
    cl = select_logic_date() 
    for file in os.listdir(in_path):
        if '.CSV' in file:
            with open(in_path + file, "r") as f:
                for line in f.readlines():
                    all_rows += 1                        
                    if cl in line:      #check that a logical date in APRM equal to a date in files                   
                        in_rows += 1					
            if all_rows > 0 and all_rows <= max_rows and all_rows == in_rows and i < min: 
                file_list.append(file)
                n_rows = n_rows + in_rows    				
                i += 1
        all_rows = 0      
        in_rows  = 0  
    return file_list

	
def main():
    total_rows = 0
    total_files = 0
    disk_s = get_space(disk_n)
    start_time = datetime.now()
    directories = get_directory_list(dirs)
    if select_logic_date() != 'error':
        log.info("Connection established to DB. Logical date = " + str(select_logic_date()).replace(",", ""))
        print   ("Connection established to DB. Logical date = " + str(select_logic_date()).replace(",", ""))  
    if disk_s == False:
        log.exception("Free disk space less than 20%")
        print("ERROR: Free disk space less than 20%")
        sys.exit()
    if check_proc() == None:
        print("ARPM RELEASE DAILY process started")
        log.info("ARPM RELEASE DAILY process started")
        subprocess.run("./releasedaily.sh", stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print("Waiting 10 minutes until the RELEASE DAILY process finished")
        log.info("Waiting 10 minutes until the RELEASE DAILY process finished")
        time.sleep(600)		
        if release_daily() == True:
            print("ARPM batch process started")
            log.info("ARPM batch process started")
            subprocess.run("./start_all.sh", stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) # start APRM process 
        else:
            log.exception("ARPM RELEASE DAILY process faild!")
            print ("ERROR: ARPM RELEASE DAILY process faild!")
            sys.exit()	
    else:
        log.exception("ARPM batch process are working. Need hands on check to verify logic date and process status.")
        print("ERROR: ARPM batch process are working. Need hands on check to verify logic date and process status.")
        sys.exit()
    try:
        for dir in directories:
            in_fpath = dir[1]
            out_fpath = dir[0]
            file_list = make_file_list(in_fpath)
            if len(file_list) == 0:
                print   ("There are no files to processing in directory " + str(in_fpath))
                log.info("There are no files to processing in directory " + str(in_fpath))
            else:    
                while len(file_list) != 0:
                    if get_files_lim() < 2:
                        for file in file_list:
                            log.info('Moved file ' + str(file) + ' into directory ' + str(out_fpath))
                            shutil.move(str(in_fpath + file), str(out_fpath))
                            shutil.move(str(in_fpath + file[:-3] + 'FIN'), str(out_fpath))
                            total_files += 1
                        total_rows = total_rows + n_rows
                        print   (str(len(file_list)) + " files with " + str(n_rows) + " rows moved into directory " + str(out_fpath))
                        log.info(str(len(file_list)) + " files with " + str(n_rows) + " rows moved into directory " + str(out_fpath))						
                        log.info("delay 10 seconds")
                        time.sleep(10)   
                        file_list = make_file_list(in_fpath)
                    else:
                        log.info("The file transfer process paused for 50 seconds until next check of the evaluation queue length")
                        time.sleep(50)               
        while get_files_lim() != 0:
            log.info("There is a pause of 50 seconds to wait for the end of the process")
            time.sleep(50)
        subprocess.run("./stop_all.sh", stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) #stop batching process APRM after finish 
        print   ("Stop batching process APRM after finish")
        log.info("Stop batching process APRM after finish")
    except:
        log.exception("ERROR!:")
    print('Total time ' + str(datetime.now() - start_time))
    print("Total files = " +  str(total_files) + " with " + str(total_rows) + " rows")
    log.info("Total files = " +  str(total_files) + " with " + str(total_rows) + " rows")
    log.info('Total time ' + str(datetime.now() - start_time))
    print("FINISH")
    log.info("FINISH")

main()



