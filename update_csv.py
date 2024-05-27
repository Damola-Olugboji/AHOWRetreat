from distutils.command.upload import upload
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import csv
from pprint import pprint
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv
import os
import glob
import time

load_dotenv()
"""
['Registered Email', 'Event ID', 'Event Name', 'No. of Adult Attendees',
'No. of Child Attendees', 'Is All Family Members', 'Payment Option',
'Total Amount', 'Amount Paid', 'Remaining Amount', 'Adult Room Number',
'Child Room Number', 'Payment Status', 'Adult Attendees Data',
'Child Attendees Data', 'Special Discount']
"""
def get_latest_file(directory):
    files = glob.glob(os.path.join(directory, '*'))
    files.sort(key=os.path.getmtime, reverse=True)
    if files:
        return files[0]
    else:
        return None
    
def pull_raw_data():
    """
        Logs into the AHOW Website pulls the latest csv into the "pulled" dir
    """
    wp_email = os.getenv("AHOWFC_EMAIL") 
    wp_pass = os.getenv("AHOWFC_PASSWORD")
    save_dir = r"/Users/damolaolugboji/Desktop/code/AhowIT/retreat/pulled/"
    url = "https://www.ahowfc.org/wp-admin"
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        context = browser.new_context()

        page = context.new_page()
        page.goto(url)
        
        page.locator("input[id='user_login']").fill(wp_email)
        page.locator("input[id='user_pass']").fill(wp_pass)
        page.get_by_text("Log In").click()
        
        page.goto("https://www.ahowfc.org/wp-admin/admin.php?page=account-info")
        with page.expect_download() as download:
            # Perform the action that initiates download
            page.get_by_text("Export data").click()
        download = download.value
        download.save_as(save_dir + download.suggested_filename)
        print(f"Downloaded CSV from AHOW WP Website to {save_dir}")
        
        context.close()
        browser.close()
        
    latest_file = get_latest_file(save_dir)
    return latest_file
    
def get_current_date():
    """
    Helper function to get current date
    """
    from datetime import datetime
    current_datetime = datetime.now()
    date_string = current_datetime.strftime('%Y-%m-%d')
    return date_string
    
def map_age_to_bucket(age):
    """
    Maps ages to buckets
    """
    if age == "NA":
        return "NA"
    age = int(age)
    age_buckets = [
        (50, float('inf'), "50+"),
        (36, 49, "36-49"),
        (26, 35, "26-35"),
        (19, 25, "19-25"),
        (15, 18, "15-18"),
        (12, 14, "12-14"),
        (9, 11, "9-11"),
        (6, 8, "6-8"),
        (3, 5, "3-5"),
        (0, 2, "0-2")
    ]
    for lower_bound, upper_bound, bucket in age_buckets:
        if lower_bound <= age <= upper_bound:
            return bucket
    return age

def parse_attendees_string(input_string):
    """
    Parses adult attendes fields into dictionaries
    """
    if input_string == None:
        return []
    people = []
    entries = input_string.split("\n")
    
    for entry in entries:
        fields = [field.strip() for field in entry.split("|")]
        person = {}
        for field in fields:
            key, value = field.split(":")
            key = key.strip()
            value = value.strip()
            person[key] = value
        people.append(person)
        
    return people
        
def extract_values_from_csv(csv_path):
    """
        Extracts values that we want to keep 
        keys_to_keep = ['Registered Email','First Name', 'Last Name', 'Phone', 'Age', "Room Number", "Breakout Session","T-shirt", "Remaining Balance"]
    """
    print("Extracting values from CSV")
    individuals = []
    registrant_count = 0
    with open(csv_path, newline='\n') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for row in reader:
            registrant_count += 1 
            registered_email = row[0]
            remaining_balance = float(row[9]) if row[9] != "" else 0
            adult_attendees = row[13] 
            child_attendees = row[14] if row[14] != "" else None
            people = parse_attendees_string(adult_attendees)
            children = parse_attendees_string(child_attendees) 
            for person in people:
                person["Remaining Balance"] = remaining_balance
                person["Registered Email"] = registered_email
                person["Age"] = map_age_to_bucket(person["Age"]) if "-" not in person["Age"] and "+" not in person["Age"] else person["Age"]
                person["Breakout Session"] = ""
                person["Room Number"] = ""
                person["isChild"] = "no"
            
            for child in children:
                child["Remaining Balance"] = remaining_balance
                child["Registered Email"] = registered_email
                child["Age"] = map_age_to_bucket(child["Age"]) if "-" not in child["Age"] and "+" not in child["Age"] else child["Age"]
                child["Breakout Session"] = ""
                child["Room Number"] = ""
                child["isChild"] = "yes"
                
            individuals.extend(people)
            individuals.extend(children)
            
    keys_to_keep = ['Registered Email','First Name', 'Last Name', 'Phone', 'Age', "Room Number", "Breakout Session","T-shirt", "Remaining Balance", "isChild"]
    filtered_people = [{key: person[key] for key in keys_to_keep if key in person} for person in individuals]
    print(f"Parsed through csv - found {len(individuals)} attendees from {registrant_count} registrants")

    return filtered_people
    
def create_formatted_csv(individuals, filename):
    """
        Creates the formatted CSV and stores in "formatted" 
    """
    base_path = "./formatted/"
    save_path = f"{base_path}{filename}.csv"
    with open(save_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        header = individuals[0].keys()
        writer.writerow(header)
        for person in individuals:
            writer.writerow(person.values())
    return save_path

def upload_to_sheets(csv_path):
    """
        uploads the formatted csv to google sheets, highlighting the first occurence of the PK, bolding the header
    """
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('./credentials.json', scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key("1x3IogKi4g3DlcJB4-DvukI8Y_YdOXtxZr963Ts2QzUw").worksheet("Retreat Registration Data")
    sheet.clear()
    with open(csv_path, newline='') as file:
        csv_reader = csv.reader(file)
        data = list(csv_reader)
    
    if len(data) * len(data[0]) > 5000:  
        print("Data set too large to upload in a single batch.")
        return
    cell_range = gspread.utils.rowcol_to_a1(1, 1) + ':' + gspread.utils.rowcol_to_a1(len(data), len(data[0]))
    sheet.update(cell_range, data)
    
    fmt_range = 'Ac1:' + chr(64 + len(data[0])) + '1'
    header_format = {
        'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
        'backgroundColor': {'red': 173/255, 'green': 216/255, 'blue': 230/255}
    }
    sheet.format(fmt_range, header_format)
    
    email_column = sheet.col_values(1)
    light_gray = {'red': 211/255, 'green': 211/255, 'blue': 211/255}
    seen_emails = {}
    for i, email in enumerate(email_column[1:], start=2):  # Skip the header row, start from row 2
        if email not in seen_emails:
            seen_emails[email] = True
            # Highlight this row
            highlight_range = f'A{i}:' + chr(64 + len(data[0])) + str(i)
            sheet.format(highlight_range, {'backgroundColor': light_gray})
            time.sleep(5)
    print("Successfully uploaded formatted data to Google Sheets")

    
def main():
    raw_csv_path = pull_raw_data()
    formatted_csv_filename = f"formatted_retreat_{get_current_date()}"
    individuals = extract_values_from_csv(raw_csv_path)
    save_path = create_formatted_csv(individuals, formatted_csv_filename)
    upload_to_sheets(save_path)

if __name__ == "__main__":
    main() 
    
    