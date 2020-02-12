import os
import re
from collections import OrderedDict, namedtuple
#import metadata_parser
from omero_metadata_parser.metadata_parser import MetadataParser
import abc

import pandas as pd


# Initialise default variables and regular expressions
ct_col_names = ['names', 'exposure', 'skip', 'zsect', 'start', 'mode', 'gain', 'voltage']
channel_table = pd.DataFrame(columns=ct_col_names)
channel_header_rx = '^\s*(Channel name),\s*(Exposure time),\s*(Skip),' \
                                '\s*(Z sect.),\s*(Start time),\s*(Camera mode),' \
                                '\s*(EM gain),\s*(Voltage)\s*$'
channel_row_rx = '^\s*(Brightfield|GFP|GFPFast|cy5|mCherry),\s*(-?\d+\.?\d*),' \
                            '\s*(-?\d+\.?\d*),\s*(-?\d+\.?\d*),\s*(-?\d+\.?\d*),\s*(-?\d+\.?\d*),' \
                            '\s*(\-?\d+\.?\d*),\s*(\-?\d+\.?\d*)$'

zsect_col_names = ['sections', 'spacing', 'PFSon', 'anyz', 'drift', 'method']
zsect_table = pd.DataFrame(columns=zsect_col_names)
zsect_header_rx = '^\s*(Sections),\s*(Spacing),\s*(PFSon\?),\s*(AnyZ\?),' \
                            '\s*(Drift),\s*(Method)'
zsect_vals_rx = '^\s*(-?\d+\.?\d*),\s*(-?\d+\.?\d*),\s*(-?\d+\.?\d*),\s*(-?\d+\.?\d*),' \
                            '\s*(-?\d+\.?\d*),\s*(-?\d+\.?\d*)'

time_dict = dict()
time_dict_keys = ['istimelapse', 'interval', 'ntimepoints','totalduration']

pt_col_names = ['name', 'xpos', 'ypos', 'zpos', 'PFSoffset', 'group']
points_table = pd.DataFrame(columns=pt_col_names)
points_header_rx = '^\s*(Position name),\s*(X position),\s*(Y position),\s*(Z position),' \
                                '\s*(PFS offset),\s*(Group)' \
                                '((?:,\s*)(Brightfield|GFP\b|GFPFast|cy5|mCherry))+$'
                                # '((?:,\s*)?(mCherry))?,' \
                                # '((?:,\s*)?(GFPFast))?((?:,\s)?(cy5))?((?:,\s*)?(GFP))?'
points_header_rx =  '^\s*(Position name),\s*(X position),\s*(Y position),\s*(Z position),\s*(PFS offset),\s*(Group)(?:,\s*)(Brightfield)?(?:,\s*)?(GFP\b)?(?:,\s*)?(GFPFast)?(?:,\s*)?(cy5)?(?:,\s*)?(mCherry)?'

# use this regex if we're just capturing the first five numeric columns, then appending channel values later
points_row_rx = '^\s*([a-z]{3}\d*_?\d*),\s*(-?\d+\.?\d*),\s*(-?\d+\.?\d*),\s*(-?\d+\.?\d*),' \
                        '\s*(-?\d+\.?\d*),\s*(-?\d+\.?\d*)(?:.*)'

npumps = 0
pst_col_names = ['port', 'diameter', 'rate', 'direction', 'isrunning', 'contents']
pump_start_table = pd.DataFrame(columns=pst_col_names)
pst_header_rx = '^\s*(Pump port),\s*(Diameter),\s*(Current rate),' \
                        '\s*(Direction),\s*(Running),\s*(Contents)$'
pst_row_rx = '^\s*(COM\d*),\s*(-?\d+\.?\d*),\s*(-?\d+\.?\d*),\s*(INF|OUF),' \
                            '\s*(-?\d+\.?\d*),\s*(.*)'

# representation of the switch parameters as they are in the acq file
switch_params_in = OrderedDict({'switchvol': list(), 'switchrate': list(), 'nchanges': list(),
                 'switchtimes': list(), 'switchto': list(), 'switchfrom': list(),
                 'switchflow': list()})

# representation of the switch parameters as they are stored in the Matlab struct
switch_params = OrderedDict({'volume': list(), 'rate': list(), 'nchanges': list(),
                 'switchtimes': list(), 'switchto': list(), 'switchfrom': list(),
                 'pumpflow': list()})

multi_line_params = {'switchflow': 2}
multi_line_count = 0

# Specify the regular expressions to test:
raw_section_rxs = ['^Channels:$','^Channel name,','^Z_sectioning:$',
              '^Sections,Spacing,','^Time_settings:$','^Points:$',
              '^Position name, X position','^Flow_control:$',
              '^Syringe pump details:','^Pump states at beginning of experiment:$',
              '^Pump port, Diameter,','^Dynamic flow details:$',
              '^Number of pump changes:','^Switching parameters:',
              '^Infuse/withdraw volumes:$','^Infuse/withdraw rates:$','^Times:',
              '^Switched to:','^Switched from:','^Flow post switch:$']

section_names = ['channels','channels','zsect','zsect','times','positions',
                 'positions','flow','npumps','pumpstart','pumpstart','pumpstartend',
                 'nchanges','switchparams','switchvol','switchrate','switchtimes',
                 'switchto','switchfrom','switchflow']

raw_npumps_rx = '^Syringe pump details: (\d+) pumps.$'
raw_nchanges_rx = '^Number of pump changes:(\d+)$'
raw_switch_params_rx = '^Switching parameters:(\d+),(\d+)$'
raw_switch_times_rx = '^Times:(\d+.*)$'
raw_switch_to_rx = '^Switched to:(\d+.*)$'
raw_switch_from_rx = '^Switched from:(\d+.*)$'
raw_num_val_rx = '^(-?\d+\.?\d*)$'
raw_times_rx = '^\s*(-?\d+\.?\d*),\s*(-?\d+\.?\d*),\s*(-?\d+\.?\d*),\s*(-?\d+\.?\d*)\s*$'

active_section = ''
section_line_num = 0

PROJECT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..")

# Load the input file to a variable
print PROJECT_DIR

class AcqMetadataParser(MetadataParser):
    '''
    Populate switch_params structure with specified parameter
    '''
    def update_params(self, line, raw_rx, param_name):
        rx = re.compile(raw_rx)
        search_res = re.split(rx, line)

        # retrieve the corresponding param name in output form
        idx = switch_params_in.keys().index(param_name)
        param_name_out = list(switch_params)[idx]

        if search_res is not None:
            if len(search_res) > 1:
                param_val = search_res[1]
                if param_val:
                    switch_params[param_name_out].append(float(param_val))
                    return
            else:
                rx = re.compile(raw_num_val_rx)
                search_res = re.search(rx, line)

                if search_res is not None:
                    param_val_args = search_res.groups()

                    if param_val_args is not None:
                        switch_params[param_name_out].append(float(param_val_args[0]))

                        # handle cases where param values are listed over successive lines
                        if param_name in multi_line_params:
                            global multi_line_count
                            multi_line_count = multi_line_count+1
                            max_lines = multi_line_params[param_name]
                            if multi_line_count == max_lines:
                                multi_line_count = 0
                                return
                            else:
                                return param_name
                        else:
                            return

            return param_name


    '''
    Populate the specified data frame row by row
    '''
    def build_table(self, line, raw_header_rx, raw_row_rx, table_name, df):
        row_rx = re.compile(raw_row_rx)
        search_res = re.findall(row_rx, line)

        # if search_res is not None:
        if len(search_res) > 0:
            search_res = re.split(row_rx, line)
            print line
            df.loc[len(df)] = list(filter(None, search_res))
        else:
            header_rx = re.compile(raw_header_rx)
            search_res = re.findall(header_rx, line)

            # if search_res is not None:
            if len(search_res) > 0:
                # it's the header row; nothing to do since dataframe is already init'ed
                search_res = re.split(header_rx, line)

        print df

        return table_name


    def handle_section(self, line, section_name):
        global switch_params

        # --------------------- npumps --------------------  #
        if section_name is 'npumps':
            global npumps
            rx = re.compile(raw_npumps_rx)
            npumps = float(re.search(rx, line).group(1))

            # print parsed_line
        # --------------------- channels --------------------  #
        elif section_name is 'channels':
            return_val = self.build_table(line, channel_header_rx, channel_row_rx,
                                    section_name, channel_table)

            return return_val
        # --------------------- points --------------------  #
        elif section_name is 'positions':
            return_val = self.build_table(line, points_header_rx, points_row_rx,
                                    section_name, points_table)

            return return_val
        # --------------------- zsect --------------------  #
        elif section_name is 'zsect':
            return_val = self.build_table(line, zsect_header_rx, zsect_vals_rx,
                                    section_name, zsect_table)

            return return_val
        # --------------------- pumpstart --------------------  #
        elif section_name is 'pumpstart':
            return_val = self.build_table(line, pst_header_rx, pst_row_rx,
                                    section_name, pump_start_table)

            return return_val
        # --------------------- switchparams --------------------  #
        elif section_name is 'switchparams':
            rx = re.compile(raw_switch_params_rx)
            search_res = re.split(rx, line)

            if search_res is not None:
                if len(search_res) > 1:
                    switch_params['volume'] = search_res[1]
                    switch_params['rate'] = search_res[2]
                    return
            else:
                return_val = section_name

                return return_val
        # --------------------- times --------------------  #
        elif section_name is 'times':
            global time_dict
            rx = re.compile(raw_times_rx)
            search_res = re.findall(rx, line)

            if len(search_res) > 0:
                search_res = list(filter(None, re.split(rx, line)))

                time_dict = dict(zip(time_dict_keys, search_res))
                return

            return section_name
        # --------------------- switchto --------------------  #
        # --------------------- switchfrom --------------------  #
        # --------------------- switchvol --------------------  #
        # --------------------- switchvol --------------------  #
        # --------------------- switchrate --------------------  #
        elif section_name in switch_params_in:
            return_val = self.update_params(line, raw_switch_from_rx, section_name)

            return return_val


    def create_acq_metadata_obj(self):
        acq_annot = namedtuple('AcqAnnotation', [], verbose=False)
        acq_annot.channels = channel_table
        acq_annot.zsections = zsect_table
        acq_annot.times = time_dict
        acq_annot.positions = points_table
        acq_annot.npumps = npumps
        acq_annot.pump_init = pump_start_table
        acq_annot.switch_params = dict(switch_params)

        acq_annot.table_dict = dict()
        acq_annot.table_dict = {'channels': channel_table, 'zsections': zsect_table,
            'positions':points_table, 'pumpstart':pump_start_table}

        kvp_list = self.build_kvps('Number of pumps', str(npumps))

        acq_annot.kvp_list = kvp_list

        return acq_annot


    def extract_metadata(self, filename):
        global section_line_num
        print 'hello'

        section_rxs = map(re.compile, raw_section_rxs)

        active_section = None

        file = open(filename)  # This is a big file

        # read each line in the file
        for line in file:
            line = line.strip()

            if not line:
                continue

            section_line_num += 1

            # match = next((x for x in raw_section_rx if x in line), False)
            # print line
            # print match

            if any(regex.search(line) for regex in section_rxs):
                # find a way to get the matched regex...
                # https://stackoverflow.com/questions/3389574/check-if-multiple-strings-exist-in-another-string
                # https://docs.python.org/2/library/re.html#search-vs-match

                # https://github.com/lark-parser/lark
                # https://tomassetti.me/parsing-in-python/
                # https://github.com/google/textfsm

                for section_rx in section_rxs:
                    # print('Looking for "%s" in "%s" ->' % (section_rx, line))
                    if re.search(section_rx, line):

                        section_rx_idx = section_rxs.index(section_rx)

                        current_section = section_names[section_rx_idx]


                        if line == 'Switching parameters:':
                            line = 'Switching parameters:2,6'
                            line = 'Switching parameters:'
                        active_section = self.handle_section(line, current_section)

            elif active_section is not None:
                active_section = self.handle_section(line, current_section)

        acq_annot = self.create_acq_metadata_obj()

        file.close()

        return acq_annot


def main():
    global switch_params
    print PROJECT_DIR
    input_path = os.path.join(PROJECT_DIR, '..', 
                              "Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1_00",
                              "Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1Acq.txt")

    #input_path = os.path.join(PROJECT_DIR, "tests", "test_data", "sample_logfiles",
     #                         "dataset_846", "lowglc_screen_hog1_gln3_mig1_msn2_yap1Acq.txt")

    #input_path = os.path.join(PROJECT_DIR, "tests", "test_data", "sample_logfiles",
     #                         "dataset_8939", "sga_glc0_1_Mig1Nhp_Maf1Nhp_Msn2Maf1_Mig1Mig1_Msn2Dot6Acq.txt")

    #input_path = os.path.join(PROJECT_DIR, "tests", "test_data", "sample_logfiles",
     #                         "dataset_12655", "20171205_vph1hxt1Acq.txt")

    #input_path = os.path.join(PROJECT_DIR, "tests", "test_data", "sample_logfiles",
    #                          "dataset_13606", "Hxt4GFP_hxt1Acq.txt")

    #input_path = os.path.join(PROJECT_DIR, "tests", "test_data", "sample_logfiles",
     #                      "dataset_14507", "Batgirl_Morph_OldCamera_Myo1_Lte1_Bud3_Htb2_Hog1Acq.txt")

    # input_file = open(input_path)
    #acq_annot = parse_acq_file(input_file)

    metadata_parser = AcqMetadataParser()
    metadata = metadata_parser.extract_metadata(input_path)

    print metadata.channels
    print metadata.times
    print metadata.switch_params
    print metadata.zsections
    print metadata.positions

if __name__ == "__main__":
    main()


