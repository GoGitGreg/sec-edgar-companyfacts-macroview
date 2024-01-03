import pandas as pd
import io
import os
import zipfile
import csv
import requests

# https://www.sec.gov/edgar/sec-api-documentation
# https://www.sec.gov/developer
# https://www.sec.gov/os/accessing-edgar-data

# Get the zip file
zip_file_url = 'https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip'
headers = {'User-Agent': input('Individual or Entity Name Making the Request') + " " +  input('Email Address')}

output_file_path = 'test-companyfacts.zip'
response = requests.get(zip_file_url, headers=headers, stream=True)

if response.status_code == 200:
    with open(output_file_path, 'wb') as output_file:
        # Stream and save the content in chunks to the local file
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                output_file.write(chunk)
else:
    print('Error downloading zip file')

# Extract zip file

file_path = 'companyfacts.zip'
destion_file_path = 'CompanyFacts'

with zipfile.ZipFile(file_path,'r') as zip_ref:
    zip_ref.extractall(destion_file_path)

# iterate through each json file, define data deta structure, export to csv

audit_folder = 'Audit'
data_folder = 'Data'
os.makedirs(f'{data_folder}',exist_ok=True)
os.makedirs(f'{audit_folder}',exist_ok=True)

file_path_Labels = os.path.join(f'{data_folder}/Labels.csv')
if os.path.exists(file_path_Labels):
    pass
else:
    with open(file_path_Labels, 'w', newline='') as file:
        writer = csv.writer(file)
        headers = ['label','description']
        writer.writerow(headers)

file_path_Labels = os.path.join(f'{audit_folder}/Audit_File.csv')
if os.path.exists(file_path_Labels):
    pass
else:
    with open(file_path_Labels, 'w', newline='') as file:
        writer = csv.writer(file)
        headers = ['cik','Issue','Identified_Categories','Total_Categories']
        writer.writerow(headers)

file_list = os.listdir('CompanyFacts')
headers = ['start','end','val','accn','fy','fp','form','filed','frame','units','cik']

# Running the code for all files took a little over 2 hours on my machine.  Thus the code is set to run only for the first 100 files.  Can be modified by adjusting the next line of code.
for file_name in file_list[:100]:
    category_count = 0
    with open(f'CompanyFacts/{file_name}') as file:
        file_contents = file.read()
        df = pd.read_json(io.StringIO(file_contents)).reset_index()
        if len(df) > 0:
            try:
                dei = df[df['index'] == 'dei']
                index_rownum = df[df['index'] == 'dei'].index
                dei = pd.json_normalize(dei['facts'].iloc[0])
                for column in dei.columns:
                    if 'label' in column:
                        Labels = pd.json_normalize(df['facts'].iloc[index_rownum[0]][f"{column.split('.')[0]}"]).filter(['label','description'])
                        Labels.to_csv(f'{data_folder}/Labels.csv',mode='a',header=False,index=False)
                    elif 'units' in column:
                        name1 = column.split('.')[0]
                        name2 = column.split('.')[1]
                        name3 = column.split('.')[2]

                        file_path = os.path.join(f'{data_folder}/{name1}.csv')
                        if os.path.exists(file_path):
                            pass
                        else:
                            with open(file_path, 'w', newline='') as file:
                                writer = csv.writer(file)
                                writer.writerow(headers)

                        Units = pd.json_normalize(df['facts'].iloc[index_rownum[0]][f'{name1}'][f'{name2}'][f'{name3}'])

                        if 'start' in Units.columns:
                            pass
                        else:
                            Units['start'] = ''

                        if 'end' in Units.columns:
                            pass
                        else:
                            Units['end'] = ''
                        
                        if 'frame' in Units.columns:
                            pass
                        else:
                            Units['frame'] = ''

                        Units['units'] = name3
                        Units['cik'] = file_name.replace('.json','').replace('CIK','')
                        Units = Units[headers]
                        Units.to_csv(f'{data_folder}/{name1}.csv',mode='a',header=False,index=False)

                category_count += 1
            except:
                pass

            try:
                invest = df[df['index'] == 'ifrs-full']
                invest_rownum = df[df['index'] == 'ifrs-full'].index
                invest = pd.json_normalize(invest['facts'].iloc[0])
                for column in invest.columns:
                    if 'label' in column:
                        file_path = os.path.join(f'{data_folder}/Labels.csv')
                        if os.path.exists(file_path):
                            pass
                        else:
                            with open(file_path, 'w', newline='') as file:
                                writer = csv.writer(file)

                        Labels = pd.json_normalize(df['facts'].iloc[invest_rownum[0]][f"{column.split('.')[0]}"]).filter(['label','description'])
                        Labels.to_csv(f'{data_folder}/Labels.csv',mode='a',header=False,index=False)
                    elif 'units' in column:
                        name1 = column.split('.')[0]
                        name2 = column.split('.')[1]
                        name3 = column.split('.')[2]

                        file_path = os.path.join(f'{data_folder}/{name1}.csv')
                        if os.path.exists(file_path):
                            pass
                        else:
                            with open(file_path, 'w', newline='') as file:
                                writer = csv.writer(file)
                                writer.writerow(headers)

                        Units = pd.json_normalize(df['facts'].iloc[invest_rownum[0]][f'{name1}'][f'{name2}'][f'{name3}'])

                        if 'start' in Units.columns:
                            pass
                        else:
                            Units['start'] = ''

                        if 'end' in Units.columns:
                            pass
                        else:
                            Units['end'] = ''

                        if 'frame' in Units.columns:
                            pass
                        else:
                            Units['frame'] = ''
                        
                        Units['units'] = name3
                        Units['cik'] = file_name.replace('.json','').replace('CIK','')
                        Units = Units[headers]
                        Units.to_csv(f'{data_folder}/{name1}.csv',mode='a',header=False,index=False)

                category_count += 1
            except:
                pass
        
            try:
                invest = df[df['index'] == 'invest']
                invest_rownum = df[df['index'] == 'invest'].index
                invest = pd.json_normalize(invest['facts'].iloc[0])
                for column in invest.columns:
                    if 'label' in column:
                        file_path = os.path.join(f'{data_folder}/Labels.csv')
                        if os.path.exists(file_path):
                            pass
                        else:
                            with open(file_path, 'w', newline='') as file:
                                writer = csv.writer(file)

                        Labels = pd.json_normalize(df['facts'].iloc[invest_rownum[0]][f"{column.split('.')[0]}"]).filter(['label','description'])
                        Labels.to_csv(f'{data_folder}/Labels.csv',mode='a',header=False,index=False)
                    elif 'units' in column:
                        name1 = column.split('.')[0]
                        name2 = column.split('.')[1]
                        name3 = column.split('.')[2]

                        file_path = os.path.join(f'{data_folder}/{name1}.csv')
                        if os.path.exists(file_path):
                            pass
                        else:
                            with open(file_path, 'w', newline='') as file:
                                writer = csv.writer(file)
                                writer.writerow(headers)

                        Units = pd.json_normalize(df['facts'].iloc[invest_rownum[0]][f'{name1}'][f'{name2}'][f'{name3}'])
                        
                        if 'start' in Units.columns:
                            pass
                        else:
                            Units['start'] = ''

                        if 'end' in Units.columns:
                            pass
                        else:
                            Units['end'] = ''

                        if 'frame' in Units.columns:
                            pass
                        else:
                            Units['frame'] = ''

                        Units['units'] = name3
                        Units['cik'] = file_name.replace('.json','').replace('CIK','')
                        Units = Units[headers]
                        Units.to_csv(f'{data_folder}/{name1}.csv',mode='a',header=False,index=False)

                category_count += 1
            except:
                pass

            try:
                invest = df[df['index'] == 'srt']
                invest_rownum = df[df['index'] == 'srt'].index
                invest = pd.json_normalize(invest['facts'].iloc[0])
                for column in invest.columns:
                    if 'label' in column:
                        file_path = os.path.join(f'{data_folder}/Labels.csv')
                        if os.path.exists(file_path):
                            pass
                        else:
                            with open(file_path, 'w', newline='') as file:
                                writer = csv.writer(file)

                        Labels = pd.json_normalize(df['facts'].iloc[invest_rownum[0]][f"{column.split('.')[0]}"]).filter(['label','description'])
                        Labels.to_csv(f'{data_folder}/Labels.csv',mode='a',header=False,index=False)
                    elif 'units' in column:
                        name1 = column.split('.')[0]
                        name2 = column.split('.')[1]
                        name3 = column.split('.')[2]

                        file_path = os.path.join(f'{data_folder}/{name1}.csv')
                        if os.path.exists(file_path):
                            pass
                        else:
                            with open(file_path, 'w', newline='') as file:
                                writer = csv.writer(file)
                                writer.writerow(headers)

                        Units = pd.json_normalize(df['facts'].iloc[invest_rownum[0]][f'{name1}'][f'{name2}'][f'{name3}'])

                        if 'start' in Units.columns:
                            pass
                        else:
                            Units['start'] = ''

                        if 'end' in Units.columns:
                            pass
                        else:
                            Units['end'] = ''

                        if 'frame' in Units.columns:
                            pass
                        else:
                            Units['frame'] = ''
                        
                        Units['units'] = name3
                        Units['cik'] = file_name.replace('.json','').replace('CIK','')
                        Units = Units[headers]
                        Units.to_csv(f'{data_folder}/{name1}.csv',mode='a',header=False,index=False)

                category_count += 1
            except:
                pass

            try:
                invest = df[df['index'] == 'us-gaap']
                invest_rownum = df[df['index'] == 'us-gaap'].index
                invest = pd.json_normalize(invest['facts'].iloc[0])
                for column in invest.columns:
                    if 'label' in column:
                        file_path = os.path.join(f'{data_folder}/Labels.csv')
                        if os.path.exists(file_path):
                            pass
                        else:
                            with open(file_path, 'w', newline='') as file:
                                writer = csv.writer(file)

                        Labels = pd.json_normalize(df['facts'].iloc[invest_rownum[0]][f"{column.split('.')[0]}"]).filter(['label','description'])
                        Labels.to_csv(f'{data_folder}/Labels.csv',mode='a',header=False,index=False)
                    elif 'units' in column:
                        name1 = column.split('.')[0]
                        name2 = column.split('.')[1]
                        name3 = column.split('.')[2]

                        if len(name1) > 100:
                            # workaround
                            name1_capped = name1[:120]
                        else:
                            name1_capped = name1

                        file_path = os.path.join(f'{data_folder}/{name1_capped}.csv')
                        if os.path.exists(file_path):
                            pass
                        else:
                            with open(file_path, 'w', newline='') as file:
                                writer = csv.writer(file)
                                writer.writerow(headers)

                        Units = pd.json_normalize(df['facts'].iloc[invest_rownum[0]][f'{name1}'][f'{name2}'][f'{name3}'])

                        if 'start' in Units.columns:
                            pass
                        else:
                            Units['start'] = ''

                        if 'end' in Units.columns:
                            pass
                        else:
                            Units['end'] = ''

                        if 'frame' in Units.columns:
                            pass
                        else:
                            Units['frame'] = ''
                        
                        Units['units'] = name3
                        Units['cik'] = file_name.replace('.json','').replace('CIK','')
                        Units = Units[headers]
                        Units.to_csv(f'{data_folder}/{name1_capped}.csv',mode='a',header=False,index=False)

                category_count += 1
            except:
                pass

        else:
            pass
        
        # Audit Check - main data category counts
        try:
           
            if len(df) == category_count:
                issue = 'No'
            else:
                issue= 'Yes'

            Audit_Data = {
                'cik':[file_name.replace('.json','')],
                'Issue': issue,
                'Identified_Categories':category_count,
                'Total_Categories': len(df)
            }

            Audit_File = pd.DataFrame(Audit_Data)
            Audit_File.to_csv(f'{audit_folder}/Audit_File.csv', mode='a',header=False,index=False)
        except:
            print('Audit_File.csv failed')

# Audit Check - column totals

file_list = os.listdir(f'{data_folder}')
first_iteration = True

for file_name in file_list:

    if file_name == 'Labels.csv':
        pass
    else:
        df = pd.read_csv(f'{data_folder}/{file_name}')

        if len(df.columns) == len(headers):
            issue = "No"
        else:
            issue = "Yes"

        Audit_Data = {
        'file_name':[file_name],
        'Issue': [issue],
        'Expected_Columns': len(headers),
        'Total_Columns':len(df.columns)
        }

        Audit_File_2 = pd.DataFrame(Audit_Data)

        if first_iteration == True:
            Audit_File_2.to_csv(f'{audit_folder}/Audit_File_2.csv', mode='a',header=True,index=False)
            first_iteration = False
        else:
            Audit_File_2.to_csv(f'{audit_folder}/Audit_File_2.csv', mode='a',header=False,index=False)

# Audit Check - main data category list

file_list = os.listdir('CompanyFacts')

categories = []

for file_name in file_list[:100]:
    with open(f'CompanyFacts/{file_name}') as file:
        file_contents = file.read()
        df = pd.read_json(io.StringIO(file_contents)).reset_index()
        categories.extend(df['index'].tolist())

categories = sorted(list(set(categories)))

with open(f'{audit_folder}/Categories.txt', 'w') as file:
    for value in categories:
        file.write(str(value) + '\n')
