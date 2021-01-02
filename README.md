
# Address matching

This project involved writing software that finds all of the matching UK addresses from 2 different datasets.
- For example, the address "3, Steeple Court, Vicarage road, , Egham, Surrey, TW20 9GL, England" from one dataset would match to "FLAT 3, STEEPLE COURT, VICARAGE ROAD, EGHAM, TW20 9GL" from another dataset

### Software requirements
- Python 3
- Install required dependencies
- - `$ pip install -r requirements.txt`

### Usage

1. Clone the repository
- - If you are not using a new dataset, then skip to step 6
2. Open `setup.py` with a text editor
3. Set the output file path, by setting the variable `output_path`. The default path is set to `/out/link.csv`
4. Set the output file headers, by setting the variable  `output_matching_id`. By default, the ID attirbute names used for the headers are `UPRN` and `LMK_KEY`
 - - `UPRN` and `LMK_KEY` correspond to the attribute name of the ID field from each dataset
 5. Complete the function definition for the functions `parse_dataset_1()` and 	`parse_dataset_2()`
- - These functions must return a list of `parsed_address` objects.
- - Example definitions of these functions are given and can be adapted for the new dataset
- - `setup_output_csv()`can be modified to change the structure of the output file
5.  From the shell run `$  python addr_match.py`
- Depending on the size of the datasets used, the script may take a while to load in the required resources.
- Matching addresses are pushed to the output file at `output_path`
- - Matches are saved as they are found meaning the program may be interrupted (for example stopped) at any     time.

### Matching a single address
- This use-case describes matching a single address (instead of many addresses in a dataset) to addresses in a another large dataset.
1) Use the script as normal but with a dataset of size 1, containing the single address to be matched.
2) Load this single element dataset in the function `parse_dataset_1()`


### Performance
- Large datasets will require a large amount of RAM. This can be avoided by setting a low `limit` value in `setup.py` relative to the size of the dataset.
- The script will make full use of the executing CPU due its multiprocessing implementation
- Multiprocessing has been achieved using the python `Ray` library - Splitting the dataset into into n parts (where n is the number of cpu cores available on the host machine) and processing these parts in parallel before re-combining the results.
- - This implementation helps combat the Ω(|size_dataset|^2) time complexity nature of the address matching problem.
