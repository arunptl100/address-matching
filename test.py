from csv import DictReader
import csv
import re



# parse the dataset storing addresses in a ll
sa1 = []
sa2 = []
# src: https://stackoverflow.com/questions/33975696/find-and-replace-multiple-comma-space-instances-in-a-string-python
pattern = re.compile(r'(,\s){2,}')

count = 0
with open('AddressBaseCore_FULL_2020-07-20_001.csv', 'r', encoding="utf8") as read_obj:
    csv_dict_reader = DictReader(read_obj)
    for row in csv_dict_reader:
        address = (row['ORGANISATION'] + "," +  row['SUB_BUILDING'] + "," +
                   row['BUILDING_NAME'] + "," + row['BUILDING_NUMBER'] + "," +
                   row['STREET_NAME'] + "," + row['LOCALITY'] + "," +
                   row['TOWN_NAME'] + "," + row['POST_TOWN'] + "," +
                   row['ISLAND'] + "," + row['POSTCODE'] + ",")
        # clean up the address removing extra commas ,
        address = (re.sub(pattern, ', ', address).lstrip(',')).rstrip(',')
        sa1.append(address)
        if count == 10000:
            break
        count += 1
        # print(address)

# parse the 2nd dataset
count = 0
with open('all-domestic-certificates/domestic-E06000001-Hartlepool/domestic-E06000001-Hartlepool-certificates.csv', 'r', encoding="utf8") as read_obj:
    csv_dict_reader = DictReader(read_obj)
    for row in csv_dict_reader:
        address = (row['ADDRESS1'] + "," +  row['ADDRESS2'] + "," +
                   row['ADDRESS3'] + "," + row['POSTCODE'] + ",")
        # clean up the address removing extra commas ,
        address = (re.sub(pattern, ', ', address).lstrip(',')).rstrip(',')
        sa2.append(address)
        if count == 10000:
            break
        count += 1
        # print(address)
