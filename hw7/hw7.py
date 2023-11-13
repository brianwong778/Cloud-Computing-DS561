import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import re
import os

# Custom DoFn to extract file name and content
class ExtractFileNameAndContent(beam.DoFn):
    def __init__(self, file_pattern):
        self.file_pattern = file_pattern

    def process(self, element):
        file_name = os.path.basename(re.search(r'gs://.*/(.*)', self.file_pattern).group(1))
        yield file_name, element

# Function to extract outgoing links from a file
def extract_outgoing_links(element):
    _, content = element
    return re.findall(r'href="(\d+\.html)"', content)

# Function to extract file and incoming links
def extract_file_and_links(element):
    file_name, content = element
    links = re.findall(r'href="(\d+\.html)"', content)
    return [(link, file_name) for link in links]

# Function to format the results
def format_result(element):
    try:
        file_name, count = element
        return f'{file_name}: {count} links'
    except ValueError:
        # Handle cases where element does not have two values
        return f'Error: Element {element} does not match expected format'

#Credentials
project_id = 'ds561-398719'
bucket_name = 'bu-ds561-bwong778-hw2-bucket'
region = 'us-east1'
file_pattern = f'gs://{bucket_name}/generated_files/*.html'

# Define output file locations
outgoing_links_output = f'gs://{bucket_name}/results/outgoing_links.txt'
incoming_links_output = f'gs://{bucket_name}/results/incoming_links.txt'

# Pipeline options
options = PipelineOptions(
    runner='DataFlowRunner',
    project=project_id,
    region=region,
    temp_location=f'gs://{bucket_name}/temp',
    staging_location=f'gs://{bucket_name}/staging'
)

with beam.Pipeline(options=options) as p:
    # Read files and their names
    files = (
        p
        | 'ReadFromGCS' >> beam.io.ReadFromText(file_pattern)
        | 'ExtractFileNameAndContent' >> beam.ParDo(ExtractFileNameAndContent(file_pattern))
    )

    # Outgoing links: Count links in each file
    outgoing_links = (
        files
        | 'ExtractOutgoingLinks' >> beam.FlatMap(extract_outgoing_links)
        | 'CountOutgoingLinks' >> beam.combiners.Count.PerElement()
        | 'Top5OutgoingLinks' >> beam.combiners.Top.Of(5, key=lambda x: x[1])
        | 'FormatOutgoingResults' >> beam.Map(format_result)
        | 'WriteOutgoingResults' >> beam.io.WriteToText(outgoing_links_output)
    )

    # Incoming links: Count how many times each file is linked to
    incoming_links = (
        files
        | 'ExtractIncomingLinks' >> beam.FlatMap(extract_file_and_links)
        | 'CountIncomingLinks' >> beam.combiners.Count.PerElement()
        | 'Top5IncomingLinks' >> beam.combiners.Top.Of(5, key=lambda x: x[1])
        | 'FormatIncomingResults' >> beam.Map(format_result)
        | 'WriteIncomingResults' >> beam.io.WriteToText(incoming_links_output)
    )
