import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import re

# Creds
project_id = 'ds561-398719'
bucket_name = 'bu-ds561-bwong778-hw2-bucket'
region = 'us-east1'


options = PipelineOptions(
    runner='DataflowRunner',
    project=project_id,
    region=region,
    temp_location=f'gs://{bucket_name}/temp',
    staging_location=f'gs://{bucket_name}/staging'
)

def extract_outgoing_links(element):
    _, content = element
    return re.findall(r'href="(\d+\.html)"', content)

def extract_file_and_links(element):
    file_name, content = element
    links = re.findall(r'href="(\d+\.html)"', content)
    return [(link, file_name) for link in links]

def format_result(element):
    file_name, count = element
    return f'{file_name}: {count} links'

with beam.Pipeline(options=options) as p:
    # Read files and their names
    files = p | 'ReadFromGCS' >> beam.io.ReadAllFromTextWithFilename(f'gs://{bucket_name}/generated_files/*.html')

    # Outgoing links: Count links in each file
    outgoing_links = (
        files
        | 'ExtractOutgoingLinks' >> beam.FlatMap(extract_outgoing_links)
        | 'CountOutgoingLinks' >> beam.combiners.Count.PerElement()
        | 'Top5OutgoingLinks' >> beam.combiners.Top.Of(5, key=lambda x: x[1])
        | 'FormatOutgoingResults' >> beam.Map(format_result)
        | 'PrintTopOutgoingLinks' >> beam.Map(print)
    )

    # Incoming links: Count how many times each file is linked to
    incoming_links = (
        files
        | 'ExtractIncomingLinks' >> beam.FlatMap(extract_file_and_links)
        | 'CountIncomingLinks' >> beam.combiners.Count.PerElement()
        | 'Top5IncomingLinks' >> beam.combiners.Top.Of(5, key=lambda x: x[1])
        | 'FormatIncomingResults' >> beam.Map(format_result)
        | 'PrintTopIncomingLinks' >> beam.Map(print)
    )
