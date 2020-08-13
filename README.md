# Address matching

This project involved writing software that given datasets of UK addresses, finds all matching pairs of addresses in the dataset.

### Software requirements
- Python 3
- Install required dependencies
- - `$ pip install -r requirements.txt`

### Guide
- The script looks for the dataset in the containing folder `resources/Sample_Address_Data.xlsx`
- The dataset must have addresses in the columns A (1) from one dataset and column B (2) from the other dataset
-  From the shell run `$  python addr_match.py`

### Performance
- The script will make full use of the executing CPU due its multiprocessing implementation
- Multiprocessing has been achieved using the python `Ray` library - Splitting the dataset into into n parts (where n is the number of cpu cores available on the host machine) and processing these parts in parallel before re-combining the results.
