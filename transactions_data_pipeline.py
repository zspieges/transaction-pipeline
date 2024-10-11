###################IMPORT PACKAGES###################
import pandas as pd
import datetime 
import os
import shutil

chrdir = '/Users/zoespiegelhoff/Documents/analytics-engineer-project-interview-zspieges'
os.chdir(chrdir)

###################CREATE FUNCTIONS###################
def filenameextractor(file):
    newname = file.rsplit('/', 1)[1]
    return newname

def is_unique(df):
    try:
        boolean = df.duplicated().any()
        if boolean == True:
            raise ValueError(f'Dataframe fails the unique test. Exiting...')
    except ValueError as err:
        raise (err)

def is_null(df,column):
    try:
        if True in df[column].isna().to_list():
            raise ValueError(f"Column {column} fails non null test. Exiting...")
    except ValueError as err:
        raise (err)

def filemover(list):
    archivepath = os.getcwd()+'/archive_transactions'
    if not os.path.exists(archivepath):
        os.makedirs(archivepath)
    for file in list:
            shutil.move(file, archivepath)
        
###################FIND ALL FILES TO BE INGESTED###################
#NOTE: this assumes the location of files within repo is the same. else, change
rnlst=[]
trans_dir = os.getcwd() + '/transactions'

years = [ f.path for f in os.scandir(trans_dir) if f.is_dir() ]
for year in years:
    months = [ f.path for f in os.scandir(year) if f.is_dir() ]
    for month in months:
        days = [ f.path for f in os.scandir(month) if f.is_dir() ]
        for day in days:
            files = [ f.path for f in os.scandir(day) if f.is_file() ]
            for file in files:
                rnlst.append(file)

###################AGGREGATE INTO 1 DATASET###################
full_data = []
for file in rnlst:
    data = pd.read_csv(file)
    data['filename'] = filenameextractor(file) + '.csv'
    data['loadtimestamp'] = datetime.datetime.now()
    full_data.append(data)

if not full_data:
    print('No new files to load. Exiting.') #add a log here too?
    os._exit(0)
else:
    full_data = pd.concat(full_data)

###################MOVE FILES INJESTED TO ARCHIVE###################
#This is to avoid reingesting the data
filemover(rnlst)

###################UNION NEW INJEST WITH PREVIOUS###################
current_data = pd.read_csv(os.getcwd() + '/dbt_project/seeds/transactions_raw.csv')
final_data = pd.concat([full_data, current_data])

###################QA###################
is_unique(final_data)
is_null(final_data,'id')

###################FINAL EXPORT###################
seed_dir = os.getcwd() + '/dbt_project/seeds/'
final_data.to_csv(seed_dir+'transactions_raw.csv', index = False)

count_files = len(rnlst)
print(f'transactions-data-pipeline complete. {count_files} files ingested.')