import datetime
import pathlib
from subprocess import Popen, PIPE
from typing import Generator
import loggers
from parsers import CommonRecordParser, SpecialParsers
from writers import CsvWriter, JsonWriter

logger = loggers.LoggerFactory.get_logger(name=__name__)


def records_iterator(path_to_target: pathlib.Path,
                     record_types=('ALL',),
                     binary='atop',
                     parser: CommonRecordParser = None):
    if not path_to_target.exists():
        raise ValueError(f'Path not exists: {path_to_target} ')

    def paths():
        if path_to_target.is_dir():
            for child in path_to_target.iterdir():
                if child.is_file():
                    yield child
        else:
            yield path_to_target

    def lines(path):
        p = Popen([binary, '-r', path, '-P', ','.join(record_types)], stdout=PIPE, encoding='utf8')

        # first 'RESET' line usually log reset not machine reboot, so we skip it
        p.stdout.readline()

        for raw_line in p.stdout:
            if raw_line == 'RESET\n':
                continue

            if raw_line == 'SEP\n':
                continue

            line = parser.parse(raw_line=raw_line)
            yield line

    try:
        for current_path in paths():
            lines_gen = lines(path=current_path)
            for current_line in lines_gen:
                yield current_line
    except Exception as e:
        raise e


def time_related_records_iterator(records: Generator[dict, None, None]):
    timed_records = list()

    try:
        first_record = next(records)
        last_record = first_record
        timed_records.append(last_record)

        for current_record in records:
            last_epoch = int(last_record['epoch'])
            current_epoch = int(current_record['epoch'])
            logger.debug(f'last/current epoch: {last_epoch}/{current_epoch}')

            if current_epoch != last_epoch:
                logger.debug(f'yielding {len(timed_records)} records!')

                yield current_epoch, timed_records
                timed_records = list()

            timed_records.append(current_record)
            last_record = current_record
    except Exception as e:
        raise e


class Stats:
    def __init__(self):
        self.load_avg_1_min_per_core = None
        self.load_avg_5_min_per_core = None
        self.avg_cpu_usage = None
        self.mem_usage = None
        self.swap_usage = None
        self.disk_stats = None
        self.net_if_stats = None

        # initial data
        self.dt: datetime.datetime | None = None
        self.cpu: dict[str, str] | None = None
        self.cpus: list[dict[str, str]] | None = None
        self.cpl: dict[str, str] | None = None
        self.mem: dict[str, str] | None = None
        self.swap: dict[str, str] | None = None
        self.disk_list: list[dict[str, str]] | None = None
        self.net: dict[str, str] | None = None
        self.net_if_list: list[dict[str, str]] | None = None

    def to_dict(self):
        d = dict()
        d['dt'] = self.dt.strftime("%Y-%m-%d %H:%M:%S")
        d['load_avg_1_min_per_core'] = self.load_avg_1_min_per_core
        d['load_avg_5_min_per_core'] = self.load_avg_5_min_per_core
        d['avg_cpu_usage'] = self.avg_cpu_usage
        d['mem_usage'] = self.mem_usage
        d['swap_usage'] = self.swap_usage

        for d_name, d_stats in self.disk_stats.items():
            d[d_name] = d_stats

        d['net'] = self.net_stats
        for net_if_name, net_if_stats in self.net_if_stats.items():
            d[net_if_name] = net_if_stats

        return d

    def to_dict_flat(self):
        nested_dict = self.to_dict()

        result = dict()
        for k, v in nested_dict.items():
            if not isinstance(v, dict):
                result[k] = v
                continue

            for k_nested, v_nested in v.items():
                result[f'{k}_{k_nested}'] = v_nested

        return result

    def update(self):
        self._update_cpu_stats()
        self._update_cpl_stats()
        self._update_mem_stats()
        self._update_swap_stats()
        self._update_net_stats()

        self.disk_stats = dict()
        for d in self.disk_list:
            self._update_disk_stats(disk_dict=d)

        self.net_if_stats = dict()
        for net_if in self.net_if_list:
            self._update_net_if_stats(net_if_dict=net_if)

    def _update_cpu_stats(self):
        def calculate_single_cpu_usage(cpu: dict[str, str]):
            cpu_sys = float(cpu['cpu_sys'])
            cpu_usr = float(cpu['cpu_usr'])
            cpu_nice = float(cpu['cpu_niced'])
            cpu_idle = float(cpu['cpu_idle'])
            cpu_wait = float(cpu['cpu_wait'])
            cpu_irq = float(cpu['cpu_irq'])
            cpu_soft_irq = float(cpu['cpu_softirq'])
            cpu_steal = float(cpu['cpu_steal'])

            non_idle = cpu_sys + cpu_usr + cpu_nice + cpu_irq + cpu_soft_irq + cpu_steal
            idle = cpu_idle + cpu_wait
            total = non_idle + idle
            cpu_busy = non_idle / total
            return cpu_busy

        cpus_usage = [calculate_single_cpu_usage(cpu=c) for c in self.cpus]
        logger.debug(f'cpus usage: {cpus_usage}')
        total_cpu_usage = sum(cpus_usage) / len(cpus_usage)

        self.avg_cpu_usage = round(total_cpu_usage, 3)

    def _update_cpl_stats(self):
        load1 = float(self.cpl['load_avg1']) / float(self.cpl['processors'])
        load5 = float(self.cpl['load_avg5']) / float(self.cpl['processors'])

        self.load_avg_1_min_per_core = round(load1, 2)
        self.load_avg_5_min_per_core = round(load5, 2)
        logger.debug(f'CPU: Load average for last one minutes: {self.load_avg_1_min_per_core}')
        logger.debug(f'CPU: Load average for last five minutes: {self.load_avg_5_min_per_core}')

    def _update_mem_stats(self):
        mem_page_size_bytes = int(self.mem['page_size'])
        mem_phys = int(self.mem['size_phys']) * mem_page_size_bytes
        mem_size_free = int(self.mem['size_free']) * mem_page_size_bytes
        mem_size_cache = int(self.mem['size_cache']) * mem_page_size_bytes
        mem_size_buf = int(self.mem['size_buf']) * mem_page_size_bytes
        mem_used = mem_phys - mem_size_free - mem_size_cache - mem_size_buf

        logger.debug(f'MEM: Used {self.bytes_to_mbytes(mem_used)} MB of {self.bytes_to_mbytes(mem_phys)} MB')

        mem_usage = mem_used / mem_phys
        self.mem_usage = round(mem_usage, 3)

        logger.debug(f'MEM usage: {self.mem_usage}')

    def _update_swap_stats(self):
        swap_page_size_bytes = int(self.swap['page_size'])

        swap_size_swp = int(self.swap['size_swp']) * swap_page_size_bytes
        swap_size_free = int(self.swap['size_free']) * swap_page_size_bytes
        swap_usage = 1 - (swap_size_free / swap_size_swp)

        logger.debug(f'SWP: free {self.bytes_to_mbytes(swap_size_free)} MB of {self.bytes_to_mbytes(swap_size_swp)} MB')

        self.swap_usage = round(swap_usage, 1)

    def _update_disk_stats(self, disk_dict: dict[str: str]):
        current_disk_name = disk_dict['name']
        elapsed_sec = float(disk_dict['interval'])

        spent_for_io_sec = float(disk_dict['ms_spent']) / 1000
        disk_utilization = spent_for_io_sec / elapsed_sec

        logger.debug(f'DSK {current_disk_name}: spent for io: {spent_for_io_sec} s, elapsed: {elapsed_sec} s')
        logger.debug(f'DSK {current_disk_name}: disk_utilization: {disk_utilization}')

        reads_per_interval = int(disk_dict['reads']) / elapsed_sec
        writes_per_interval = int(disk_dict['writes']) / elapsed_sec

        self.disk_stats[current_disk_name] = {
            "disk_utilization": round(disk_utilization, 3),
            'reads_per_sec': round(reads_per_interval, 2),
            'writes_per_sec': round(writes_per_interval, 2),
        }

    def _update_net_stats(self):
        chosen_stats = ['tcp_input_errors', 'tcp_rcv', 'udp_rcv', 'ip_rcv', 'ip_delivered']
        self.net_stats = {k: v for k, v in self.net.items() if k in chosen_stats}

    def _update_net_if_stats(self, net_if_dict: dict[str: str]):
        current_net_if_name = net_if_dict['name']
        elapsed_sec = float(net_if_dict['interval'])

        packets_rcv = int(net_if_dict['packets_rcv']) / elapsed_sec
        packets_snt = int(net_if_dict['packets_snt']) / elapsed_sec

        bytes_rcv = int(net_if_dict['bytes_rcv']) / elapsed_sec
        bytes_snt = int(net_if_dict['bytes_snt']) / elapsed_sec

        self.net_if_stats[current_net_if_name] = {
            'rcv_mb_per_second': self.bytes_to_mbytes(int(bytes_rcv)),
            'snt_mb_per_second': self.bytes_to_mbytes(int(bytes_snt)),
            'rcv_packets_per_second': round(packets_rcv, 0),
            'snt_packets_per_second': round(packets_snt, 0),
        }

    @staticmethod
    def bytes_to_mbytes(value: int):
        return round(value / 1024 / 1024, 2)


class StatsSelector:
    def __init__(self, time_related_records_iterator: Generator[object, None, None]):
        self.time_related_records = time_related_records_iterator

    suffix_mapping = {
        "CPU_N": "proc_n",
        "DSK": "name",
        "NET_IF": "name",
    }

    def create_named_record(self, record):
        name = record['record_type']
        try:
            suffix_name = self.suffix_mapping[name]
            suffix = record[suffix_name]
            name_w_suffix = f'{name}_{suffix}'
            return name_w_suffix, record
        except KeyError:
            return name, record

    def stats_generator(self):
        while True:
            try:
                epoch, records = next(self.time_related_records)
                dt = datetime.datetime.fromtimestamp(epoch)
                total_records_per_dt = len(records)
                logger.debug(f'{dt} total records: {total_records_per_dt}')

                named_records = dict([self.create_named_record(r) for r in records])

                s: Stats = Stats()
                s.dt = dt
                s.cpu = named_records['CPU']
                s.cpus = [r for r in records if r['record_type'] == 'CPU_N']
                s.cpl = named_records['CPL']
                s.mem = named_records['MEM']
                s.swap = named_records['SWP']
                s.disk_list = [r for r in records if r['record_type'] == 'DSK']
                s.net = named_records['NET']
                s.net_if_list = [r for r in records if r['record_type'] == 'NET_IF']
                s.update()
                yield s

            except StopIteration:
                break


class Facade:
    special_parsers = [
        SpecialParsers.CPU,
        SpecialParsers.CPU_N,
        SpecialParsers.CPL,
        SpecialParsers.MEM,
        SpecialParsers.SWP,
        SpecialParsers.NET,
        SpecialParsers.NET_IF,
        SpecialParsers.DSK,
    ]

    # These types are only parsed from atop output
    types_to_parse = ['CPU', 'cpu', 'CPL', 'MEM', 'SWP', 'NET', 'DSK']

    def _create_stats_generator(self, src_file: pathlib.Path):
        common_parser = CommonRecordParser(special_parsers=self.special_parsers)

        records = records_iterator(path_to_target=src_file,
                                   record_types=self.types_to_parse,
                                   parser=common_parser)

        time_related_records = time_related_records_iterator(records=records)

        stats_selector = StatsSelector(time_related_records_iterator=time_related_records)
        stats_generator = stats_selector.stats_generator()

        return stats_generator

    def parse_to_csv(self,
                     src_file: pathlib.Path,
                     dst_file: pathlib.Path):
        stats_generator = self._create_stats_generator(src_file=src_file)
        csv_writer = CsvWriter()
        dict_rows = [x.to_dict_flat() for x in stats_generator]
        csv_writer.write_csv(path=dst_file, rows=dict_rows)

    def parse_to_json(self,
                      src_file: pathlib.Path,
                      dst_file: pathlib.Path):
        stats_generator = self._create_stats_generator(src_file=src_file)

        json_writer = JsonWriter()
        rows = [stat.to_dict_flat() for stat in stats_generator]
        json_writer.write_json(path=dst_file, dict_rows=rows)


if __name__ == '__main__':
    path_to_file = pathlib.Path('test_logs/atop_cpu_stress')

    f = Facade()

    path_to_csv_file = pathlib.Path(f'{path_to_file.stem}.csv')
    f.parse_to_csv(src_file=path_to_file,
                   dst_file=path_to_csv_file)

    path_to_csv_file = pathlib.Path(f'{path_to_file.stem}.json')
    f.parse_to_json(src_file=path_to_file,
                    dst_file=path_to_csv_file)

    # 'Common parser' contains special parsers for parsing different types of records.
    # 'Records iterator' uses common parser to yield records (as dicts).
    # 'Time related records iterator' uses records iterator to yield records with same timestamp.
    # 'Stats selector' uses 'time related records iterator' for creating stats generator.
    # 'Stats generator' yields stats objects each contains records with the same timestamp
    # 'Stat' object use method "update" to calculate metrics from records data
    # 'Stat' object can represent calculated metrics to dict that can be written as csv row
    # 'CsvWriter' object writes rows of dicts to file
