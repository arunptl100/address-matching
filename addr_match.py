from fuzzywuzzy import fuzz
from openpyxl import load_workbook


# class representing an address and its fuzzy matching ratio
# Intended purpose: for every address, store a list of addr_match objects
# ,of the matching addresses in the other dataset(s)
class addr_match:
    def __init__(self, addr, f_ratio, f_tkn_ratio):
        self.addr = addr
        self.f_ratio = f_ratio
        self.f_tkn_ratio = f_tkn_ratio



# parse the xlsx dataset using openpyxl into lists
sa1_l = []
sa2_l = []
dataset = load_workbook('resources/Sample_Address_Data.xlsx')
worksheet = dataset.active
# iterate over Sample Address 1 addresses
for col_cells_sa1 in worksheet.iter_cols(min_col=1, max_col=1, min_row=2):
    for cell_sa1 in col_cells_sa1:
        sa1_l.append(cell_sa1.value.upper())
# iterate over Sample Address 2 addresses
for col_cells_sa2 in worksheet.iter_cols(min_col=2, max_col=2, min_row=2):
    for cell_sa2 in col_cells_sa2:
        sa2_l.append(cell_sa2.value.upper())

# find matching addresses from each list to find matches
matched_addr = []
for addr_1 in sa1_l:
    # populate a list of matching addr_match objects
    matches = []
    for addr_2 in sa2_l:
        f_ratio = fuzz.ratio(addr_1, addr_2)
        f_tkn_ratio = fuzz.token_sort_ratio(addr_1, addr_2)
        if f_ratio >= 83:
            # print("Found a potential match\n  ", addr_1, "||", addr_2, "ratio: ",
            #     f_ratio, " tkn ratio: ", f_tkn_ratio)
            # matched_addr.append(addr_1)
            # matched_addr.append(addr_2)
            matches.append(addr_match(addr_2, f_ratio, f_tkn_ratio))
    # choose the address from matches with the highest ratio
    # check there's at least one potential match to begin with
    if len(matches) > 0:
        match = matches[0]
        for addr in matches:
            if addr.f_ratio > match.f_ratio:
                match = addr
        # now append the match and addr_1 to matched_addr list
        print("Found a match\n  ", addr_1, "||", match.addr, "ratio: ",
            match.f_ratio, " tkn ratio: ", match.f_tkn_ratio)
        matched_addr.append(match.addr)
        matched_addr.append(addr_1)

# determine number of address before matching
print("# Addresses:", len(sa1_l) + len(sa2_l))
# determine number of unmatched addresses
print("matched", len(matched_addr), "addresses")
print("# unmatched addresses:", (len(sa1_l) + len(sa2_l)) - len(matched_addr))
