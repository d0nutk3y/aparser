import csv
import json
import pathlib


class CsvWriter:
    class DefaultDialect(csv.Dialect):
        delimiter = ";"
        escapechar = '\\'
        doublequote = False
        skipinitialspace = True
        lineterminator = '\n'
        quoting = csv.QUOTE_NONE

    default_dialect = DefaultDialect()

    def write_csv(self, path: pathlib.Path, rows: list[dict]):
        def get_generic_fields():
            # Some records can contain different set of fields (disks, network interfaces)
            # so all possible fields must be represented for csv write
            fields_gen = (list(x.keys()) for x in rows)

            generic_field = next(fields_gen)
            for current_fields in fields_gen:
                for f in current_fields:
                    if f not in generic_field:
                        generic_field.append(f)

            return generic_field

        with open(path.absolute(), 'w') as csv_file:
            fields = get_generic_fields()
            dialect = self.default_dialect

            w = csv.DictWriter(f=csv_file, fieldnames=fields, dialect=dialect)
            w.writeheader()
            w.writerows(rows)


class JsonWriter:
    def write_json(self,
                   path: pathlib.Path,
                   dict_rows: list):
        with open(path.absolute(), 'w', encoding='utf-8') as json_file:
            json.dump(dict_rows, json_file, ensure_ascii=False, indent=2)
