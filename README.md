# Address matching 

This project involved writing software that given datasets of UK addresses, finds all matching pairs of addresses in the dataset.

### Software requirements 
- Python 3
- Python package: fuzzywuzzy
	- `pip install fuzzywuzzy`
- Python package: openpyxl
	- `pip install openpyxl `

### Guide
- The script looks for the dataset in the containing folder `resources/Sample_Address_Data.xlsx`
- The dataset must have addresses in the columns A (1) from one dataset and column B (2) from the other dataset
-  From the shell run `$  python addr_match.py` 
