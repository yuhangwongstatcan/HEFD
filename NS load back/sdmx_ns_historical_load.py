'''
CCEI - Nova Scotia historical hourly load
Reads the NSPI "hourly loads report for NBP-SO" CSV and writes an SDMX-CSV
file (same structure as sdmx_ns.py) to the output folder.

Source file layout:
    Nova Scotia Power in-province load,,,,,
    ,,,,,
    HOUR,Date/time,Load [MW],,,
    ,,,,,
    1,2024-01-01 01:00,1472.10,,,
    ...
    24,2024-01-01 00:00,1604.97,,,   <- "00:00" is hour ending 24 (midnight of the NEXT day)

Timestamps are hour-ending, local Atlantic time. On DST days the file follows
the wall clock: in spring one hour is missing (02:00 -> 04:00), in fall the
02:00 hour appears twice. Rows are strictly sequential, so the UTC time of each
row is taken as (UTC of row 1) + (HOUR - 1) hours and then checked against the
local label.

Usage:
    python sdmx_ns_historical_load.py [input_csv] [--output-dir output]
'''
import argparse
import logging
import os
import sys

import pandas as pd

log = logging.getLogger(__name__)
debug = False

# DEFAULT_INPUT = "2024---NSPI-hourly-loads-report-for-NBP-SO(CSV).csv"
# DEFAULT_INPUT = "2025---NSPI-hourly-loads-report-for-NBP-SO(CSV).csv"
DEFAULT_INPUT = "2026-nspi-hourly-loads-report-for-nbp-so(CSV).csv"
DEFAULT_OUTPUT_DIR = "output"
LOCAL_TZ = "America/Halifax"

SDMX_COLUMNS = ['DATAFLOW', 'FREQ', 'REF_AREA', 'COUNTERPART_AREA', 'ENERGY_FLOWS', 'TIME_PERIOD',
                'OBS_VALUE', 'DATETIME_LOCAL', 'DAYLIGHT_OFFSET', 'DATETIME_LOCAL_OFFSET',
                'UNIT_MEASURE', 'UNIT_MULT', 'MEASURE_TYPE', 'OBS_STATUS', 'CONF_STATUS']


def read_source(path):
    '''Read the NSPI CSV and return HOUR / DATETIME_LABEL / OBS_VALUE rows.'''
    raw = pd.read_csv(path, header=None, dtype=str, usecols=[0, 1, 2],
                      names=['HOUR', 'DATETIME_LABEL', 'OBS_VALUE'],
                      encoding='utf-8-sig', skip_blank_lines=True)
    raw = raw.apply(lambda c: c.str.strip())
    # keep only data rows (first column is the running hour number)
    df = raw[raw['HOUR'].str.fullmatch(r'\d+', na=False)].copy()
    if df.empty:
        raise ValueError(f'No data rows found in {path}')
    df['HOUR'] = df['HOUR'].astype(int)
    # drop blanks and text such as "Data Lost Due To Cybersecurity Incident"
    bad = pd.to_numeric(df['OBS_VALUE'], errors='coerce').isna()
    if bad.any():
        reasons = df.loc[bad, 'OBS_VALUE'].fillna('<blank>').replace('', '<blank>').value_counts()
        log.warning('Dropping %d rows with no numeric load value: %s',
                    bad.sum(), ', '.join(f'{k!r} x{v}' for k, v in reasons.items()))
        df = df[~bad]
    return df.reset_index(drop=True)


def hour_ending_label(utc):
    '''Local hour-ending label for UTC instants: wall time at the start of the
    interval + 1h (so the DST transition instant keeps the old offset's label).'''
    start_local = (utc - pd.Timedelta(hours=1)).tz_convert(LOCAL_TZ)
    return start_local, start_local.tz_localize(None) + pd.Timedelta(hours=1)


def assign_utc(label):
    '''Match each local hour-ending label, in file order, to a UTC instant.

    The HOUR column is not reliable (numbering gaps, missing repeated hour in
    fall), so each row is matched to the next hourly UTC instant whose local
    label equals the row's label. A repeated 02:00 in fall maps to the two
    consecutive instants; hours absent from the file are simply skipped.'''
    first = pd.Timestamp(label.iloc[0]).tz_localize(LOCAL_TZ, ambiguous=True,
                                                   nonexistent='shift_forward').tz_convert('UTC')
    last = pd.Timestamp(label.iloc[-1]).tz_localize(LOCAL_TZ, ambiguous=False,
                                                   nonexistent='shift_forward').tz_convert('UTC')
    grid = pd.date_range(first - pd.Timedelta(hours=2), last + pd.Timedelta(hours=2), freq='h')
    _, grid_label = hour_ending_label(grid)

    candidates = {}
    for instant, lbl in zip(grid, grid_label):
        candidates.setdefault(lbl, []).append(instant)

    utc, previous = [], None
    for i, lbl in enumerate(label):
        match = next((t for t in candidates.get(lbl, []) if previous is None or t > previous), None)
        if match is None:
            raise ValueError(f'Row {i} label {lbl} is out of order or duplicated')
        utc.append(match)
        previous = match
    utc = pd.DatetimeIndex(utc)

    missing = grid[(grid >= utc[0]) & (grid <= utc[-1])].difference(utc)
    if len(missing):
        log.warning('%d hours are missing from the file (UTC), e.g. %s',
                    len(missing), [t.strftime('%Y-%m-%dT%H:%M') for t in missing[:5]])
    return utc


def build_sdmx(df):
    '''Convert source rows to the SDMX data frame used by sdmx_ns.py.'''
    # local wall-clock label; "00:00" means midnight at the end of that date
    label = pd.to_datetime(df['DATETIME_LABEL'], format='%Y-%m-%d %H:%M')
    label = label.where(~((label.dt.hour == 0) & (label.dt.minute == 0)),
                        label + pd.Timedelta(days=1))

    utc_index = assign_utc(label)
    start_local, _ = hour_ending_label(utc_index)
    utc = pd.Series(utc_index)

    # DST flag / offset of the hour being reported (same offset as its label)
    is_dst = pd.Series([bool(t.dst()) for t in start_local])

    out = pd.DataFrame()
    out['DATAFLOW'] = ["CCEI:DF_HFED_NS(1.0)"] * len(df)
    out['FREQ'] = "H"
    out['REF_AREA'] = "CA_NS"
    out['COUNTERPART_AREA'] = "_Z"
    out['ENERGY_FLOWS'] = "LOAD"
    out['TIME_PERIOD'] = utc.dt.strftime("%Y-%m-%dT%H:%M:%S")
    out['OBS_VALUE'] = df['OBS_VALUE'].values
    out['DATETIME_LOCAL'] = label.dt.strftime("%Y-%m-%dT%H:%M:%S")
    out['DAYLIGHT_OFFSET'] = is_dst.map({True: 1, False: 0}).values
    out['DATETIME_LOCAL_OFFSET'] = is_dst.map({True: 3, False: 4}).values
    out['UNIT_MEASURE'] = "MW"
    out['UNIT_MULT'] = "0"
    out['MEASURE_TYPE'] = "ENERGY"
    out['OBS_STATUS'] = "A"
    out['CONF_STATUS'] = "F"

    out = out[SDMX_COLUMNS]
    out = out.drop_duplicates(subset=['FREQ', 'REF_AREA', 'COUNTERPART_AREA', 'ENERGY_FLOWS', 'TIME_PERIOD'],
                              keep='first')
    return out


def main(argv=None):
    parser = argparse.ArgumentParser(description='Convert NSPI hourly load CSV to SDMX-CSV.')
    parser.add_argument('input', nargs='?', default=DEFAULT_INPUT, help='NSPI hourly loads CSV')
    parser.add_argument('--output-dir', default=DEFAULT_OUTPUT_DIR, help='folder for the SDMX file')
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')

    df = read_source(args.input)
    sdmx = build_sdmx(df)
    if debug:
        print(sdmx)

    years = pd.to_datetime(sdmx['DATETIME_LOCAL']).dt.year
    year = years.mode().iloc[0]
    os.makedirs(args.output_dir, exist_ok=True)
    out_path = os.path.join(args.output_dir, f"data_ns_historical_load_{year}.csv")
    sdmx.to_csv(out_path, index=False)

    log.info('Read %d rows from %s', len(df), args.input)
    log.info('Wrote %d observations (%s to %s UTC) to %s', len(sdmx),
             sdmx['TIME_PERIOD'].iloc[0], sdmx['TIME_PERIOD'].iloc[-1], out_path)
    return out_path


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        log.exception(e)
        sys.exit(1)
