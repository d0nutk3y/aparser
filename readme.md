## About
With `aparser` you can convert one or more `atop` files to CSV or JSON format for further analytics.
The native atop tool stores raw data but not calculated metrics.
But with human-readable format you can determine date&time of interest to view it with original `atop` more detailed. 

### Dependencies
+ Python ≥ 3.11
+ `atop` version v2.8.1 (tested)

### Features
+ Calculates stats with explicit formulas
+ Exports data to JSON or CSV
+ Flat output file structure
+ Extensible for custom use cases (see Modification section)
+ Supports CLI (argparse) and Python API
+ Only requires Python and `atop` (no additional libraries)


## Usage examples
### API
```
import pathlib
from atop_reader import Facade

f = Facade()

path_to_target = pathlib.Path('./atop').absolute()
path_to_out_file = pathlib.Path(./load_test_result).absolute()

f.parse_to_csv(src_file=path_to_target, dst_file=path_to_out_file)
f.parse_to_json(src_file=path_to_target, dst_file=path_to_out_file)
```

### CLI
#### Hint
```
usage: aparser [-h] [-t TARGET] [-o OUT] [-of OUT_FORMAT]

Parses data from atop files to various formats

options:
  -h, --help            show this help message and exit
  -t TARGET, --target TARGET
                        path to atop log file or logs directory (no recursive)
  -o OUT, --out OUT     output file path
  -of OUT_FORMAT, --out_format OUT_FORMAT
                        output file format (csv, json)

```

#### Example
```
aparser_cli.py -t ./atop_logs/web_stress -o ./test_results/web_stress.csv -of csv
```


## Output examples
You can manually run ``aparser_cli_tests.sh`` for CLI testing.\
Requires atop logs: ./test_logs/web_stress\
Expected output: CSV/JSON files in ./test_results/

### CSV (part)
```
./test_results/web_stress.csv
```

### JSON (part)
```
./test_results/web_stress.json
```

## Runtime explanation
Client code uses facade object that produces stats generator.\
The stats generator chains iterators to transform raw `atop` data into list of metrics objects.

### Sequence of data transformations
1. Common parser contains special parsers and converting single raw string at time.
Each special parser handles concrete raw data (cpu, mem, network, etc.).
2. Records iterator uses `atop` binary to get raw records from `atop` file(s).
Records iterator also uses (`1`) to handle raw records.
3. Time related records iterator processes records from (`2`) and yields timestamps with associated data.
4. Stats selector creates stats generator. 
Stats generator uses (`3`) to create Stats objects.
5. Each Stats object contains date&time and corresponding stats.
6. List of stats objects can be converted to CSV or JSON.


## Modification
To adapt Aparser for newer `atop` versions or custom use cases, modify these components:
+ `atop_reader.Facade.special_parsers` list of `SpecialRecordParser` objects.
You can modify current special parsers or add new one to `parsers.SpecialParsers`.
Each special parser object contains schema which maps `atop` output (see "Parseable Output" section in `atop` manual)
Schemas (value order and types) may differ across atop versions.
+ `atop_reader.Facade.types_to_parse` parsed from `atop` output.
+ `parsers.SpecialParsers` contains schemas of ordered parsable values from `atop` output.
+ `atop_reader.Stats` contains `_update_xxx_stats` methods with stats calculation formulas.


## Links
+  https://github.com/Atoptool/atop
