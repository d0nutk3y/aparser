import loggers

logger = loggers.LoggerFactory.get_logger(name=__name__)


class RecordParser:
    def parse(self, raw_line: str) -> dict:
        raise NotImplementedError()


class SpecialRecordParser(RecordParser):
    def __init__(self, name: str, schema: dict[str:str]):
        self.name = name
        self.schema = schema

    def parse(self, raw_line: str) -> dict:
        values = raw_line.split()
        names = list(self.schema.keys())

        total_values = len(values)
        total_names = len(names)
        logger.debug(f'{self.name} schema names/values: {total_names}/{total_values}')

        total = total_names if total_names <= total_values else total_values

        result = dict()
        for _ in range(total):
            result[names[_]] = values[_]
        return result


class CommonRecordParser(RecordParser):
    def __init__(self, special_parsers: list[SpecialRecordParser]):
        self.mapping: dict[str, RecordParser] = {p.name: p for p in special_parsers}

    def parse(self, raw_line: str) -> dict:
        try:
            record_type, _, epoch, _, _, interval, raw_records = raw_line.split(maxsplit=6)
        except Exception as ex:
            logger.warning(f'Can not parse: {ex}')
            return {}

        # distinguish network stats from network interface stats
        first_raw_record = raw_records.split()[0]
        if record_type == 'NET' and first_raw_record != 'upper':
            record_type = 'NET_IF'

        # distinguish overall cpu stats from current cpu stats
        if record_type == 'cpu':
            record_type = 'CPU_N'

        try:
            parser = self.mapping[record_type]
            raw_records_parsed = parser.parse(raw_line=raw_records)
        except KeyError:
            logger.warning(f'Parser for type {record_type} not found')
            letters = (letter for letter in range(100))
            raw_records_parsed = {next(letters): value for value in raw_records.split()}

        result = {'record_type': record_type, 'epoch': epoch, 'interval': interval}
        return result | raw_records_parsed


class SpecialParsers:
    CPU = SpecialRecordParser(
        name='CPU',
        schema={
            'cpu_tot': 'total number of clock-ticks per second for this machine',
            'processors': 'number of processors',
            'cpu_sys': 'consumption for all CPUs in system mode (clock-ticks)',
            'cpu_usr': 'consumption for all CPUs in user mode (clock-ticks)',
            'cpu_niced': 'consumption for all CPUs in user mode for niced processes (clock-ticks)',
            'cpu_idle': 'consumption for all CPUs in idle mode (clock-ticks)',
            'cpu_wait': 'consumption for all CPUs in wait mode (clock-ticks)',
            'cpu_irq': 'consumption for all CPUs in irq mode (clock-ticks)',
            'cpu_softirq': 'consumption for all CPUs in softirq mode (clock-ticks)',
            'cpu_steal': 'consumption for all CPUs in steal mode (clock-ticks)',
            'cpu_guest': 'consumption for all CPUs in guest mode (clock-ticks) overlapping user mode',
            'freq': 'frequency of all CPUs',
            'freq_pct': 'frequency percentage of all CPUs',
        }
    )

    CPU_N = SpecialRecordParser(
        name='CPU_N',
        schema={
            "cpu_tot": "total number of clock-ticks per second for this machine",
            "proc_n": "processor-number",
            "cpu_sys": "consumption of this CPUs in system mode (clock-ticks)",
            "cpu_usr": "consumption of this CPUs in user mode (clock-ticks)",
            "cpu_niced": "consumption of this CPUs in user mode for niced processes (clock-ticks)",
            "cpu_idle": "consumption of this CPUs in idle mode (clock-ticks)",
            "cpu_wait": "consumption of this CPUs in wait mode (clock-ticks)",
            "cpu_irq": "consumption of this CPUs in irq mode (clock-ticks)",
            "cpu_softirq": "consumption of this CPUs in softirq mode (clock-ticks)",
            "cpu_steal": "consumption of this CPUs in steal mode (clock-ticks)",
            "cpu_guest": "consumption of this CPUs in guest mode (clock-ticks) overlapping user mode",
            "freq": "frequency of this CPU",
            "freq_prc": "frequency percentage of this CPU",
        }
    )

    CPL = SpecialRecordParser(
        name='CPL',
        schema={
            "processors": "number of processors",
            "load_avg1": "load average for last minute",
            "load_avg5": "load average for last five minutes",
            "load_avg15": "load average for last fifteen minutes",
            "ctx_switches": "number of context-switches",
            "interrupts": "number of device interrupts",
        }
    )

    MEM = SpecialRecordParser(
        name='MEM',
        schema={
            "page_size": "page size for this machine (in bytes)",
            "size_phys": "size of physical memory (pages)",
            "size_free": "size of free memory (pages)",
            "size_cache": "size of page cache (pages)",
            "size_buf": "size of buffer cache (pages)",
            "size_slab": "size of slab (pages)",
            "size_cache_dirty": "dirty pages in cache (pages)",
            "size_slab_recl": "reclaimable part of slab (pages),",
            "size_vmware_balloon": "total size of vmware's balloon pages (pages)",
            "size_shared_tot": "total size of shared memory (pages)",
            "size_shared_res": "size of resident shared memory (pages)",
            "size_shared_swp": "size of swapped shared memory (pages)",
            "page_size_huge": "huge page size (in bytes)",
            "size_huge_tot": "total size of huge pages (huge pages)",
            "size_huge_free": "size of free huge pages (huge pages)"
        }
    )

    SWP = SpecialRecordParser(
        name='SWP',
        schema={
            "page_size": "page size for this machine (in bytes)",
            "size_swp": "size of swap (pages)",
            "size_free": "size of free swap (pages)",
            "NONE": "0 (future use)",
            "size_committed": "size of committed space (pages)",
            "committed_limit": "limit for committed space (pages)",
        }
    )

    NET = SpecialRecordParser(
        name='NET',
        schema={
            "NONE": "the verb 'upper'",
            "tcp_rcv": "number of packets received by TCP",
            "tcp_snt": "number of packets transmitted by TCP",
            "udp_rcv": "number of packets received by UDP",
            "udp_snt": "number of packets transmitted by UDP",
            "ip_rcv": "number of packets received by IP",
            "ip_snt": "number of packets transmitted by IP",
            "ip_delivered": "number of packets delivered to higher layers by IP",
            "op_fwd": "number of packets forwarded by IP",
            'upd_input_errors': 'number  of  input  errors (UDP)',
            'udp_noport_errors': 'number  of noport errors (UDP)',
            'tcp_active_opens': 'number of active opens (TCP)',
            'tcp_passive_opens': 'number of passive opens (TCP)',
            'tcp_established': 'number of established connections at this moment (TCP)',
            'tcp_retransmitted': 'number of retransmitted segments (TCP)',
            'tcp_input_errors': 'number of input errors (TCP)',
            'tcp_output_resets': 'number of output resets (TCP)',
        }
    )

    NET_IF = SpecialRecordParser(
        name='NET_IF',
        schema={
            "name": "name of the interface",
            "packets_rcv": "number of packets received by the interface",
            "bytes_rcv": "number of bytes received by the interface",
            "packets_snt": "number of packets transmitted by the interface",
            "bytes_snt": "number of bytes transmitted by the interface",
            "speed": "interface speed",
            "duplex": "duplex mode (0=half 1=full)",
        }
    )

    DSK = SpecialRecordParser(
        name='DSK',
        schema={
            "name": "interface name",
            "ms_spent": "number of milliseconds spent for I/O",
            "reads": "number of reads issued",
            "reads_sectors": "number of sectors transferred for reads",
            "writes": "number of writes issued",
            "writes_sectors": "number of sectors transferred for write",
            "discards": "number of discards issued",
            "sectors_for_discards": "sectors transferred for discards",
            "requests_flight": "number of requests currently in flight",
            "busy_queue": "average queue depth while the disk was busy",
        }
    )


if __name__ == '__main__':
    lines = [
        'CPU xxx 1741208401 2025/03/06 00:00:01 605594 100 1 221178 274283 19 59824907 26535 0 2628 8861 0 2399 100 0 0',
        'CPU xxx 1741209001 2025/03/06 00:10:01 300 100 1 124 155 0 29587 14 0 5 4 0 2399 100 0 0',
        'CPU xxx 1741209301 2025/03/06 00:15:01 300 100 1 137 190 0 29541 16 0 3 6 0 2399 100 0 0',
        'CPU xxx 1741209601 2025/03/06 00:20:01 300 100 1 134 169 0 29553 10 0 8 5 0 2399 100 0 0',
    ]

    parsers = [
        SpecialParsers.CPU,
    ]
    crp = CommonRecordParser(special_parsers=parsers)

    for line in lines:
        print(crp.parse(raw_line=line))

