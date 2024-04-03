import logging
import re

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pandas as pd


class ExtractAndFilterDoFn(beam.DoFn):
    def process(self, element):
        lat = element['LATITUDE'][0]
        lon = element['LONGITUDE'][0]
        bulbtemp = element['HourlyWetBulbTemperature']
        windspeed = element['HourlyWindSpeed']
        month = pd.to_datetime(element['DATE']).dt.month

        return (lat, lon, [[windspeed, bulbtemp]], month)

def run(input_csv_path):
    p = beam.Pipeline()
    (p
        | 'Read CSV File' >> beam.io.ReadFromText(input_csv_path, skip_header_lines=1)
                | 'Extract and Filter' >> beam.ParDo(ExtractAndFilterDoFn())
     )
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()