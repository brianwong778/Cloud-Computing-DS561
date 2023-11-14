import apache_beam as beam
from apache_beam.io import fileio
import re
import logging
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

BUCKET_NAME = 'bu-ds561-bwong778-hw2-bucket'
DIRECTORY = 'generated_files'
OUTPUT_BUCKET_PATH = f'gs://{BUCKET_NAME}/output/'

class ReadHTMLFiles(beam.DoFn):
    def process(self, file_metadata):
        try:
            file_name = file_metadata.metadata.path
            with file_metadata.open() as file:
                contents = file.read().decode('utf-8')
                yield file_name, contents
        except Exception as e:
            logging.error(f"Error processing file {file_metadata.metadata.path}: {e}")

def find_hyperlinks(element):
    from bs4 import BeautifulSoup 
    try:
        file_name, content = element
        soup = BeautifulSoup(content, 'html.parser')
        for a in soup.find_all('a', href=True):
            link = a.get('href')
            if re.match(r'\d+\.html', link):
                yield (file_name, link)
    except Exception as e:
        logging.error(f'find_hyperlinks error: {e}')

def link_to_file_mapping(element):
    file_name, link = element
    return link, file_name

def calculate_incoming_links(element):
    target_link, sources = element
    return target_link, len(list(sources))

def count_total_links(element):
    file_name, links = element
    return file_name, len(list(links))

def main():
    options = beam.options.pipeline_options.PipelineOptions(
        runner='DataflowRunner',
        project='ds561-398719',
        temp_location=f'gs://{BUCKET_NAME}/temp',
        region='us-east1',
        requirements = 'requirements.txt'
    )

    with beam.Pipeline(options=options) as p:
        read_html = (
            p 
            | 'MatchHTMLFiles' >> fileio.MatchFiles(f'gs://{BUCKET_NAME}/{DIRECTORY}/*.html')
            | 'ReadHTMLMatches' >> fileio.ReadMatches()
            | 'ReadHTMLFiles' >> beam.ParDo(ReadHTMLFiles())
            | 'FindHyperlinks' >> beam.FlatMap(find_hyperlinks)
        )

        map_links = (
            read_html
            | 'LinkToFileMapping' >> beam.Map(lambda element: (element[1], element[0]))
        )

        outgoing_link_count = (
            read_html 
            | 'GroupByHTMLFile' >> beam.GroupByKey()
            | 'CountOutgoingLinks' >> beam.Map(count_total_links)
        )
        incoming_link_count = (
            map_links
            | 'GroupByHyperlink' >> beam.GroupByKey()
            | 'CalculateIncomingLinks' >> beam.Map(calculate_incoming_links)
        )

        top_outgoing_links = (
            outgoing_link_count
            | 'TopOutgoingLinks' >> beam.transforms.combiners.Top.Of(5, key=lambda x: x[1])
        )

        top_incoming_links = (
            incoming_link_count
            | 'TopIncomingLinks' >> beam.transforms.combiners.Top.Of(5, key=lambda x: x[1])
        )

        # Output the results to Google Cloud Storage
        top_outgoing_links | 'WriteTopOutgoingLinks' >> beam.io.WriteToText(OUTPUT_BUCKET_PATH + 'outgoing_links')
        top_incoming_links | 'WriteTopIncomingLinks' >> beam.io.WriteToText(OUTPUT_BUCKET_PATH + 'incoming_links')

if __name__ == '__main__':
    main()
