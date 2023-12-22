

import os
import re
import json
import math
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta

load_dotenv()

access_token = None
# Refresh access token 
refresh_token = os.getenv("REFRESH_TOKEN")
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
URL = "https://www.zohoapis.com/crm/v4/Leads"

bus_data = pd.read_csv('./BUS_DATA_EXPORT.csv',encoding='cp1252')
gas = pd.read_csv('./Gas.csv')
electric = pd.read_csv('./Electric.csv')

def get_access_token():
    token_url = f'https://accounts.zoho.com/oauth/v2/token?refresh_token={refresh_token}&client_id={client_id}&client_secret={client_secret}&grant_type=refresh_token'
    token_data = requests.post(url=token_url)
    if token_data.json().get('access_token'):
        return token_data.json().get('access_token')
    return None



def compare_dates(date1,date2):
    return max(date1,date2)

def row_to_create_task(electric_rows):
        temp_date = datetime(1800,1,1)
        final_date = None
        for obj in electric_rows:
            if type(obj['End']) == float:
                continue
            else:
                date = datetime.strptime(obj['End'],"%d/%m/%Y")
                final_date = compare_dates(date,temp_date)
                temp_date = final_date
        if final_date is None:
            return None    
        final_date = datetime.strftime(final_date,"%d/%m/%Y")            
        for obj in electric_rows:
            if str(obj['End']) == str(final_date):
                return obj

        
def get_note(client_code):
    notes_list = []
    ced_notes_list = []
    ced_dates = [] 
    for obj in bus_data.to_dict('records'):
        if obj["Client Code"] == client_code:
            notes = obj['Notes']
            if type(notes) == float:
                continue
            elif ('mpan' in notes.lower() and 'ced' in notes.lower())  or ('mpan' in notes.lower() and 'ced' in notes.lower()):
                pass
                notes = re.sub('ï¿½','',notes)
                date = ced_date(2,notes)
                ced_notes_list.append(notes)
                ced_dates.append(date)
            elif 'mpan' in notes.lower() or 'mprn' in notes.lower() or 'deal done' in notes.lower():
                notes_list.append(notes)

    temp_date = datetime(1800,1,1)
    final_date = None
    for date in ced_dates:
        if date != None :
            date = datetime.strptime(date, "%Y-%m-%d")
            final_date= compare_dates(date,temp_date)
            temp_date = final_date

    if final_date is not None:        
        final_date = datetime.strftime(final_date,"%d/%m/%Y")
        for notes in ced_notes_list:
            if final_date in notes:
                return notes
            
    if final_date is None:
        for note in notes_list:
            if note is not None:
                return note

def create_notes(data): 
    main_note = """"""
    if data:
        for row in data:
            for k,v in row.items():
                if not k == "Client Code":
                    if k == "KVA Chrage": 
                        main_note += f"{k} = {v}\n \n" 
                    else: 
                        main_note += f"{k} = {v}\n"
    return main_note



def check_note(note:str):
    if "deal done" in note.lower() or "mpan" in note.lower() or "mprn" in note.lower():
        return True
    else: False

def create_tasks(rows, parent_id, name, date, headers):
    """
    to create call back for gas and electric data
    """
    CALL_BACK_URL = "https://www.zohoapis.com/crm/v4/Tasks"
    if rows:
            end_date = rows["End"]
            end_date = end_date.split('/')
            day = int(end_date[0])
            month = int(end_date[1])
            year = int(end_date[2])
            end_date = datetime(year,month,day)
            end_date = end_date - relativedelta(years=1)
            end_date = end_date + relativedelta(days=1)
            end_date = end_date.strftime("%Y-%m-%d")
            tasks_payload = {
                "data": [
                    {
                        "Subject": name,
                        "Due_Date": f"{end_date}",
                        "Priority": "High",
                        "Status": "Not Started",
                        "What_Id": parent_id,
                        "se_module": "Leads"
                    }
                ]
            }
            tasks_res = requests.post(url=CALL_BACK_URL, headers=headers, data=json.dumps(tasks_payload))
    if date:
            tasks_payload = {
                "data": [
                    {
                        "Subject": name,
                        "Due_Date": date,
                        "Priority": "High",
                        "Status": "Not Started",
                        "What_Id": parent_id,
                        "se_module": "Leads"
                    }
                ]
            }
            tasks_res = requests.post(url=CALL_BACK_URL, headers=headers, data=json.dumps(tasks_payload))
            # print(tasks_res.json())
                   
def ced_date(number,notes):
    try:
        ced_pattern = r"CED\s*[-: ]\s*(\d{2})[-/. ](\d{2})[-/. ](\d{4})"
    except:
        pass
    try:
        ced_pattern = r"CED\s*[-: ]\s*(\d{2})[-/. ](\d{2})[-/. ](\d{2})"
    except:
        pass
    try:
        ced_pattern = r"CED\s*-\s*(\d{2})/(\d{2})/(\d{4})"
    except:
        pass
    try:
        ced_pattern = r"CED\s*[ ]\s*(\d{2})/(\d{2})/(\d{4})"
    except:
        pass
    if ced_pattern:
        if number == 1:
            value = re.search(ced_pattern, notes)
            if value:
                day = int(value.group(1))              
                month = int(value.group(2))                            
                year =  value.group(3)  
                if len(year) == 4:
                    year = int(value.group(3))
                elif len(year) == 2:
                    year = int('19' + value.group(3))   
                ced_date = datetime(year, month, day)
                ced_date = ced_date - relativedelta(years=1)
                ced_date = ced_date + timedelta(days=1)
                ced_date = ced_date.strftime("%Y-%m-%d") 
                return ced_date
        else:
            value = re.search(ced_pattern, notes)
            if value:
                day = int(value.group(1))              
                month = int(value.group(2))                            
                year =  value.group(3)
                if len(year) == 4:
                    year = int(value.group(3))
                elif len(year) == 2:
                    year = int('19' + value.group(3))   
                ced_date = datetime(year, month, day)
                ced_date = ced_date.strftime("%Y-%m-%d") 
                return ced_date
    return None

bus_data_1 = bus_data.drop_duplicates(subset="Client Code")
total_data = len(bus_data_1)
def main_loop():
    i=0
    access_token = get_access_token()
    
    if not access_token:
        print("Failed to get access token")
        return
    headers = {'Authorization': f'Bearer {access_token}'}
    
    for obj in bus_data_1.to_dict('records'):
            client_code = obj['Client Code']
            print(client_code,' started *************')                 
            electric_rows = electric.loc[electric['Client Code'] == client_code]
            electric_rows_2 = electric_rows.to_dict('records')
            electric_rows = electric_rows.to_dict('records')
            gas_rows = gas.loc[gas['Client Code'] == client_code]
            gas_rows_2 = gas_rows.to_dict('records') 
            gas_rows = gas_rows.to_dict('records') 
            note = get_note(client_code)
            email = obj['Email']
            if type(email) == float:
                email_id = 'noemail@example.com'
            else:
                if re.match(r"[^@]+@[^@]+\.[^@]+", email):
                    email_id = email
                else:
                    email_id = 'noemail@example.com'
            if electric_rows:    
                print('inside the elec filtering')
                electric_rows = row_to_create_task(electric_rows)
            if gas_rows:
                print('inside the gas filtering')
                gas_rows = row_to_create_task(gas_rows)
                   
            if gas_rows or electric_rows or note:
                payload = {
                "data":[
                    {
                    "Company": obj['Client Name'],
                    "Last_Name": obj.get("Surname") if obj.get("Surname") else "X Surname",
                    "First_Name": obj['Forename'],
                    "Email": str(email_id),
                    "Street": obj['Address Line 1'],
                    "Zip_Code": obj['Address Postcode'],
                    "Phone" : obj['Phone'],
                    "Designation":obj['Title']
                    }
                ]
                }
                # API Hit to create Leads Object
                response = requests.post(url=URL, headers=headers, data=json.dumps(payload))
                response = response.json()
                print(response)
                
                if response['data'][0]['status'] == 'success':
                    parent_id = response['data'][0]['details']['id']
                    NOTES_URL = f"https://www.zohoapis.com/crm/v2/Leads/{parent_id}/Notes"
                    gas_note = create_notes(gas_rows_2)
                    electric_note = create_notes(electric_rows_2)
                    notes= str(get_note(client_code))
                    notes = re.sub('ï¿½','',notes)
                    try:
                        notes = notes.replace('-??','') 
                    except:
                        pass
                    notes_payload = {
                        "data": [
                            {
                                "Note_Title": "Notes",
                                "Note_Content": f"""{notes} \n \n {electric_note} \n \n {gas_note}""",
                                "Parent_Id": parent_id,
                                "se_module": "Leads"
                            }
                        ]
                    }
                    # API Hit to create Notes for Leads
                    notes_res = requests.post(url=NOTES_URL, headers=headers, data=json.dumps(notes_payload))
                    # Create Call back(Tasks)
                    date = ced_date(1,notes)  
                    if not gas_rows and not electric_rows:
                        if 'mpan' in notes.lower() and date:
                            create_tasks(None, parent_id, "RN - Elec", date, headers)
                        if 'mprn' in notes.lower() and date:
                            create_tasks(None, parent_id, "RN - Gas", date, headers)
                    else: 
                        if electric_rows:                  
                            create_tasks(electric_rows, parent_id, "RN - Elec", None, headers)
                        elif 'mpan' in notes.lower() and date:
                            create_tasks(None, parent_id, "RN - Elec", date, headers)
                        if gas_rows:
                            create_tasks(gas_rows, parent_id, "RN - Gas", None, headers)
                        elif 'mprn' in notes.lower() and date:
                            create_tasks(None, parent_id, "RN - Gas", date, headers)
                else:
                    continue
                
            i = i + 1 
            if i % 1000 == 0:
                access_token = get_access_token()
                print("Refresh access token generated") 
            if not access_token:
                print("Refresh access token not generated")
                return    
            headers = {'Authorization': f'Bearer {access_token}'}
            print(client_code,'Ended *************')
            print(i ,'/',total_data)
    


main_loop()