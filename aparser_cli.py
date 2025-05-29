import argparse
import pathlib
import writers

from atop_reader import Facade

# Writers configuration
supported_writers = {
    'csv': writers.CsvWriter,
    'json': writers.JsonWriter
}

# Parsing routine
parser = argparse.ArgumentParser(
    prog='aparser',
    description='Parses data from atop files to various formats')
parser.add_argument('-t', "--target", help="path to atop log file or logs directory (no recursive)", default='', type=str)
parser.add_argument('-o', "--out", help="output file path", default='', type=str)
parser.add_argument('-of', "--out_format", help="output file format (csv, json)", default='csv', type=str)

args = parser.parse_args()

if args.target == '':
    parser.error('Must specify file or directory as target')
    exit(1)

if args.out == '':
    parser.error('Must specify output file name')
    exit(1)

formats_supported = list(supported_writers.keys())
selected_format = args.out_format
selected_format = formats_supported[0] if not selected_format else selected_format

formats_supported_text = ', '.join(formats_supported)
if not selected_format in formats_supported:
    parser.error(f'Format {args.out_format} is unsupported. Supported formats: {formats_supported_text}')

path_to_target = pathlib.Path(args.target).absolute()
path_to_out_file = pathlib.Path(args.out).absolute()
out_format = selected_format

print(f'Parsing: {path_to_target}\nTo: {path_to_out_file}\nFormat: {out_format}')


def parse():
    f = Facade()
    if out_format == 'csv':
        f.parse_to_csv(src_file=path_to_target, dst_file=path_to_out_file)
    elif out_format == 'json':
        f.parse_to_json(src_file=path_to_target, dst_file=path_to_out_file)
    else:
        print("There's no way this is going to happen.")
        exit(1)

try:
    parse()
except Exception as e:
    print('Somehow error occurred!')
    print(f'Details: {e}')

#
# time_related_records = time_related_records_iterator(records=records)
#
# stats_selector = StatsSelector(time_related_records_iterator=time_related_records)
# stats_generator = stats_selector.stats_generator()

# dict_rows = [x.to_dict_flat() for x in stats_generator]
# csv_writer = writers.CsvWriter()
# csv_writer.write_csv(path=path_to_out_file, rows=dict_rows)
#
