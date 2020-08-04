from fuzzywuzzy import fuzz
from openpyxl import load_workbook


# parse the xlsx dataset using openpyxl
dataset = load_workbook('resouces/Sample_Address_Data.xlsx')
sheet = dataset.active
row_count = sheet.max_row
for i in range(row_count):
   print(sheet.cell(row=i, column=2).value)

# print(fuzz.ratio('geeksforgeeks', 'geeksgeeks'))
