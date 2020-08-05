from fuzzywuzzy import fuzz
from openpyxl import load_workbook

# benchmark
# # Σ Addresses: 3046
# matched 1554 addresses
# # Σ unmatched addresses: 1492


# class representing an address and its fuzzy matching ratio
# Intended purpose: for every address, store a list of addr_match objects
# ,of the matching addresses in the other dataset(s)
class addr_match:
    def __init__(self, addr, f_ratio, f_tkn_ratio):
        self.addr = addr
        self.f_ratio = f_ratio
        self.f_tkn_ratio = f_tkn_ratio


# Helper function that standardises a given string address
def standardise_addr(addr):
    # apply any transformations to addresses
    # strip all commas away from the address
    addr = addr.replace(',', '').upper()
    # remove the word 'flat'
    addr = addr.replace('FLAT', '')
    # remove the word 'apartment'
    addr = addr.replace('APARTMENT', '')
    # strip() removes leading and trailing whitespace
    addr = addr.strip()
    return addr


# parse the xlsx dataset using openpyxl into lists
# ISSUE: large dataset, too much to parse into memory
# parse each address as you go along?
sa1_l = []
sa2_l = []
dataset = load_workbook('resources/Sample_Address_Data.xlsx')
worksheet = dataset.active
# iterate over Sample Address 1 addresses
for col_cells_sa1 in worksheet.iter_cols(min_col=1, max_col=1, min_row=2):
    for cell_sa1 in col_cells_sa1:
        sa1_l.append(cell_sa1.value)
# iterate over Sample Address 2 addresses
for col_cells_sa2 in worksheet.iter_cols(min_col=2, max_col=2, min_row=2):
    for cell_sa2 in col_cells_sa2:
        sa2_l.append(cell_sa2.value)

# determine number of address before matching
num_addresses = len(sa1_l) + len(sa2_l)

# find matching addresses from each list
matched_addr = []
for addr_1 in sa1_l:
    # populate a list of matching addr_match objects for some address in list 1
    matches = []
    # iterate through every address in list 2
    mod_addr_2 = ''
    # standardise the address first
    mod_addr_1 = standardise_addr(addr_1)
    for addr_2 in sa2_l:
        # standardise the address first
        mod_addr_2 = standardise_addr(addr_2)

        # determine fuzzy matching ratio
        f_ratio = fuzz.ratio(mod_addr_1, mod_addr_2)
        f_tkn_ratio = fuzz.token_sort_ratio(mod_addr_1, mod_addr_2)

        # If the first part of the address is a number, check if they both match
        # E.g 1, Charter House and 2, Charter House should not match!
        # At this point, the Address has been stripped of commas and is ^case
        # And the word Flat has been removed
        # get first part of the address
        addr_num1 = mod_addr_1.split(' ')[0]
        addr_num2 = mod_addr_2.split(' ')[0]
        # check if the first parts of the address are both numeric
        if (addr_num1.isnumeric()) and (addr_num2.isnumeric()):
            if addr_num1 != addr_num2:
                # if the nums dont match reduce their ratio 'score' by 20
                f_tkn_ratio -= 20
                f_ratio -= 20

        # check if the address is a potential match
        if (f_tkn_ratio >= 73) and (f_ratio >= 63):
            # append the potential match to the matches list
            matches.append(addr_match(addr_2, f_ratio, f_tkn_ratio))

    # choose the address from matches with the highest ratio
    # check there's at least one potential match to begin with
    if len(matches) > 0:
        match = matches[0]
        for addr in matches:
            if addr.f_tkn_ratio > match.f_tkn_ratio:
                match = addr
        # now append the match and addr_1 to matched_addr list
        print("Found a match\n  ", addr_1, "||", match.addr, "\n\tratio: ",
            match.f_ratio, "tkn ratio: ", match.f_tkn_ratio)
        # TODO: create an object storing the address and its matching address
        matched_addr.append(match.addr)
        matched_addr.append(addr_1)
        # remove the matching address from sa2_l

print("# Σ Addresses:", num_addresses)
print("matched", len(matched_addr), "addresses")
print("# Σ unmatched addresses:", (num_addresses - len(matched_addr)))
