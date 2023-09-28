import re
from statistics import mean, median, quantiles
#from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage


def get_outgoing_links(html_content):
    links = re.findall(r'<a HREF="(\d+\.html)">', html_content)
    return links

def compute_data_and_graph(bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.lookup_bucket(bucket_name)
    
    if not bucket:
        print("Bucket does not exist!")
        return None, None

    graph = {}
    outgoing_links_count = {}
    incoming_links_count = {}

    blobs = bucket.list_blobs()
    for blob in blobs:
        content = blob.download_as_text()
        outgoing_links = get_outgoing_links(content)
        
        # Update the graph
        graph[blob.name] = outgoing_links
        
        # Update outgoing links count
        outgoing_links_count[blob.name] = len(outgoing_links)

        # Update incoming links count
        for link in outgoing_links:
            if link in incoming_links_count:
                incoming_links_count[link] += 1
            else:
                incoming_links_count[link] = 1
                
    return graph, (outgoing_links_count, incoming_links_count)

def print_link_statistics(outgoing_links_count, incoming_links_count):
    # Outgoing Links Statistics
    outgoing_values = list(outgoing_links_count.values())
    print("Outgoing Links:")
    print("Average:", mean(outgoing_values))
    print("Median:", median(outgoing_values))
    print("Max:", max(outgoing_values))
    print("Min:", min(outgoing_values))
    print("Quintiles:", quantiles(outgoing_values, n=5))
    print()  

    # Incoming Links Statistics
    incoming_values = list(incoming_links_count.values())
    print("Incoming Links:")
    print("Average:", mean(incoming_values))
    print("Median:", median(incoming_values))
    print("Max:", max(incoming_values))
    print("Min:", min(incoming_values))
    print("Quintiles:", quantiles(incoming_values, n=5))
    print()


def compute_pagerank(graph, max_iterations=100, convergence_threshold=0.005):
    num_pages = len(graph)
    pagerank = {page: 1/num_pages for page in graph}
    incoming_links = {page: [] for page in graph}
    
    for page, links in graph.items():
        for link in links:
            if link in incoming_links:
                incoming_links[link].append(page)
    

    for _ in range(max_iterations):
        total_change = 0
        new_pagerank = {}
        
        for page in graph:
            total = sum(pagerank[incoming_page] / len(graph[incoming_page]) for incoming_page in incoming_links[page] if incoming_page in graph)
            new_pagerank[page] = 0.15 + (0.85 * total)
            total_change += abs(new_pagerank[page] - pagerank[page])
        
        pagerank = new_pagerank
            
        if total_change < convergence_threshold:
            break
               
    # Sort pages by PageRank and return top 5
    sorted_pages = sorted(pagerank.keys(), key=lambda x: pagerank[x], reverse=True)
    for page in sorted_pages[:5]:
        print(f"{page}: {pagerank[page]}")
    
def main():
    # Retrieve data
    graph, (outgoing_links_count, incoming_links_count) = compute_data_and_graph("bu-ds561-bwong778-hw2-bucket")

    # Print statistics
    if graph and outgoing_links_count and incoming_links_count:
        print_link_statistics(outgoing_links_count, incoming_links_count)
        
    #Compute and print the top 5 pages with highest PageRank
    compute_pagerank(graph)
    
if __name__ == "__main__":
  main()



