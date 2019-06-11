#!/usr/bin/env python3
import codecs
import csv
from datetime import datetime
from datetime import timedelta
import pytz
import os
import json

import singer
from singer import utils, Transformer
from singer import metadata

from google.cloud import storage

REQUIRED_CONFIG_KEYS = [
    'key_file',
    'start_date',
    'bucket_name',
    'package_name'
]
STATE = {}

LOGGER = singer.get_logger()

BOOKMARK_DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


class Context:
    config = {}
    state = {}
    catalog = {}
    tap_start = None
    stream_map = {}
    new_counts = {}
    updated_counts = {}

    @classmethod
    def get_catalog_entry(cls, stream_name):
        if not cls.stream_map:
            cls.stream_map = {s["tap_stream_id"]: s for s in cls.catalog['streams']}
        return cls.stream_map.get(stream_name)

    @classmethod
    def get_schema(cls, stream_name):
        stream = [s for s in cls.catalog["streams"] if s["tap_stream_id"] == stream_name][0]
        return stream["schema"]

    @classmethod
    def is_selected(cls, stream_name):
        stream = cls.get_catalog_entry(stream_name)
        if stream is not None:
            stream_metadata = metadata.to_map(stream['metadata'])
            return metadata.get(stream_metadata, (), 'selected')
        return False

    @classmethod
    def print_counts(cls):
        LOGGER.info('------------------')
        for stream_name, stream_count in Context.new_counts.items():
            LOGGER.info('%s: %d new, %d updates',
                        stream_name,
                        stream_count,
                        Context.updated_counts[stream_name])
        LOGGER.info('------------------')


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():
        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            # TODO Events may have a different key property than this. Change
            # if it's appropriate.
            'key_properties': [
                'date',
                'package_name',
                'dimension_name',
                'dimension_value'
            ]
        }
        streams.append(catalog_entry)

    return {'streams': streams}


def csv_to_list(content):
    lines = content.split('\n')
    header = [s.lower().replace(' ', '_') for s in lines[0].split(',')]

    data = []
    for row in csv.reader(lines[1:]):
        if len(row) == 0:
            continue
        line_obj = {}
        for i, column in enumerate(header):
            if i < len(row):
                line_obj[column] = row[i].strip()
        data.append(line_obj)

    return data, header


def sync(bucket):
    # Write all schemas and init count to 0
    for catalog_entry in Context.catalog['streams']:
        stream_name = catalog_entry["tap_stream_id"]
        singer.write_schema(stream_name, catalog_entry['schema'], catalog_entry['key_properties'])

        Context.new_counts[stream_name] = 0
        Context.updated_counts[stream_name] = 0

    query_report(bucket)


def query_report(bucket):
    stream_name = 'installs'
    dimension_name = 'os_version'  # 'device'
    catalog_entry = Context.get_catalog_entry(stream_name)
    stream_schema = catalog_entry['schema']
    package_name = Context.config['package_name']

    bookmark = datetime.strptime(get_bookmark(stream_name), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC)
    delta = timedelta(days=1)

    extraction_time = singer.utils.now()

    iterator = bookmark
    singer.write_bookmark(
        Context.state,
        stream_name,
        'start_date',
        iterator.strftime(BOOKMARK_DATE_FORMAT)
    )

    with Transformer(singer.UNIX_SECONDS_INTEGER_DATETIME_PARSING) as transformer:
        while iterator + delta <= extraction_time:

            iterator_str = iterator.strftime("%Y%m")  # like 201906
            report_key = f"stats/installs/installs_{package_name}_{iterator_str}_{dimension_name}.csv"

            rep_data = bucket.get_blob(report_key).download_as_string()
            bom = codecs.BOM_UTF16_LE
            if rep_data.startswith(bom):
                rep_data = rep_data[len(bom):]

            rep_csv = rep_data.decode('utf-16le')

            rep, fields = csv_to_list(rep_csv)

            for index, line in enumerate(rep, start=1):
                data = line
                data['dimension_name'] = dimension_name
                data['dimension_value'] = data[fields[2]]
                del data[fields[2]]

                rec = transformer.transform(data, stream_schema)

                singer.write_record(
                    stream_name,
                    rec,
                    time_extracted=extraction_time
                )

                Context.new_counts[stream_name] += 1

            singer.write_bookmark(
                Context.state,
                stream_name,
                'start_date',
                (iterator + delta).strftime(BOOKMARK_DATE_FORMAT)
            )

            singer.write_state(Context.state)
            iterator += delta

    singer.write_state(Context.state)


def get_bookmark(name):
    bookmark = singer.get_bookmark(Context.state, name, 'start_date')
    if bookmark is None:
        bookmark = Context.config['start_date']
    return bookmark


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))

    else:
        Context.tap_start = utils.now()
        if args.catalog:
            Context.catalog = args.catalog.to_dict()
        else:
            Context.catalog = discover()

        Context.config = args.config
        Context.state = args.state

        client = storage.Client.from_service_account_json(Context.config['key_file'])
        bucket = client.get_bucket(Context.config['bucket_name'])

        sync(bucket)


if __name__ == '__main__':
    main()
