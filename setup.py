import glob
from csv import DictReader
import re
import os
import csv


# increasing tolerance will increase matches but decrease match quality
tolerance = 3
# verbose will display match details
verbose = True


#  1/2 = 16526393
# Load up to limit records in the csv
# To  load all records, set the number to max int / very lage value
limit = 1000

# src: https://stackoverflow.com/questions/33975696/find-and-replace-multiple-comma-space-instances-in-a-string-python
pattern = re.compile(r'(,\s){2,}')

# specifies the exact location the results are stored (output csv)
output_path = 'out/link.csv'

# the output headers
# For example, in this example the output csv will have the headings:
# UPRN | LMK_KEY | ...
# These should be the unique ID attribute name from each dataset
output_matching_ids = ['UPRN', 'LMK_KEY']


# helper class storing an address and an identifier
# depending on the dataset the address came from e.g - UPRN num
# These objects are appneded to a list during the dataset parsing procedure
class parsed_address:
    def __init__(self, addr, id):
        self.addr = addr
        self.id = id


# Parse the first dataset
# Expected Output:
# list of parsed_address objects
def parse_dataset_1():
    # example code below ... Add your own parsing code if required
    count = 0
    sa1_l = []
    print("**Parsing AddressBaseCore_FULL_2020-07-20_001.csv**", flush=True)

    with open(r'C:/Users/rish/Downloads/Files_For_James/AddressBaseCore/AddressBaseCore_FULL_2020-07-20_001.csv', 'r', encoding="utf8") as read_obj:
        csv_dict_reader = DictReader(read_obj)
        for row in csv_dict_reader:
            if count == limit:
                break
            count += 1

            address = (row['ORGANISATION'] + "," +  row['SUB_BUILDING'] + "," +
                       row['BUILDING_NAME'] + "," + row['BUILDING_NUMBER'] + "," +
                       row['STREET_NAME'] + "," + row['LOCALITY'] + "," +
                       row['TOWN_NAME'] + "," + row['POST_TOWN'] + "," +
                       row['ISLAND'] + "," + row['POSTCODE'] + ",")
            # clean up the address removing extra commas ,
            address = (re.sub(pattern, ', ', address).lstrip(',')).rstrip(',')
            sa1_l.append(parsed_address(address, row['\ufeffUPRN']))
            # print(address)
    return sa1_l


# Parse the first dataset
# Expected Output:
# list of parsed_address objects
def parse_dataset_2():
    # example code below ... Add your own parsing code if required
    count = 0
    # parse the 2nd dataset
    sa2_l = []
    # iterate through all the directories to get all domestic csvs
    # src: https://stackoverflow.com/questions/2212643/python-recursive-folder-read
    # recursively go through every file from the base director root_dir
    # if the file is a csv file and is not recommendation then add it to
    # domestic_certs_paths
    root_dir = "C:/Users/rish/Downloads/Files_For_James/non-domestic/all-non-domestic-certificates"
    non_domestic_certs_paths = []
    for filename in glob.iglob(root_dir + '**/**', recursive=True):
        if(("recommendations" not in filename) and (filename.endswith("certificates.csv"))):
            non_domestic_certs_paths.append(filename)
            print("**Parsing", filename.rsplit("\\", 2)[-2], "csv **", flush=True)

    # parse every csv pointed to in the list domedomestic_certs_paths
    for csv_file in non_domestic_certs_paths:
        with open(csv_file, 'r', encoding="utf8") as read_obj:
            csv_dict_reader = DictReader(read_obj)
            for row in csv_dict_reader:
                if count == limit:
                    break
                count += 1
                address = (row['ADDRESS1'] + "," +  row['ADDRESS2'] + "," +
                           row['ADDRESS3'] + "," + row['POSTCODE'] + ",")
                # clean up the address removing extra commas ,
                address = (re.sub(pattern, ', ', address).lstrip(',')).rstrip(',')
                sa2_l.append(parsed_address(address, row['LMK_KEY']))
            # print(address)
    return sa2_l


# sets up the output CSV
def setup_output_csv():
    # setup the output csv
    # check if the output csv already exists
    if os.path.exists(output_path):
        # then delete the file
        os.remove(output_path)
    # now create a new output csv
    open(output_path, 'w')
    # setup the csv headers
    with open(output_path, 'a') as csvfile:
        filewriter = csv.writer(csvfile, delimiter=',',
                                quotechar="|", quoting=csv.QUOTE_MINIMAL)
        filewriter.writerow([output_matching_ids[0], output_matching_ids[1], 'Address 1', 'Address 2', 'Tier'])
