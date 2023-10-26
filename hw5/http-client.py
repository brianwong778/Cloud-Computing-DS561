#!env python3

import http.client
from urllib.parse import urljoin
import argparse
import random

import platform
import os
import ssl
from datetime import date


list_of_countries = [ 'Afghanistan', 'Albania', 'Algeria', 'Andorra',
                      'Angola', 'Antigua and Barbuda', 'Argentina', 'Armenia',
                      'Australia', 'Austria', 'Azerbaijan', 'Bahamas', 'Bahrain',
                      'Bangladesh', 'Barbados', 'Belarus', 'Belgium', 'Belize', 'Benin',
                      'Bhutan', 'Bolivia', 'Bosnia and Herzegovina', 'Botswana',
                      'Brazil', 'Brunei', 'Bulgaria', 'Burkina Faso', 'Burundi', 'Cabo Verde',
                      'Cambodia', 'Cameroon', 'Canada', 'Central African Republic',
                      'Chad', 'Chile', 'China', 'Colombia', 'Comoros',
                      'Congo, Democratic Republic of the', 'Congo, Republic of the',
                      'Costa Rica', 'Cote dIvoire', 'Croatia', 'Cuba', 'Cyprus',
                      'Czechia', 'Denmark', 'Djibouti', 'Dominica', 'Dominican Republic',
                      'Ecuador', 'Egypt', 'El Salvador', 'Equatorial Guinea',
                      'Eritrea', 'Estonia', 'Eswatini', 'Ethiopia', 'Fiji', 'Finland',
                      'France', 'Gabon', 'Gambia', 'Georgia', 'Germany', 'Ghana',
                      'Greece', 'Grenada', 'Guatemala', 'Guinea', 'Guinea-Bissau',
                      'Guyana', 'Haiti', 'Honduras', 'Hungary', 'Iceland', 'India',
                      'Indonesia', 'Iran', 'Iraq', 'Ireland', 'Israel', 'Italy',
                      'Jamaica', 'Japan', 'Jordan', 'Kazakhstan', 'Kenya', 'Kiribati',
                      'Kosovo', 'Kuwait', 'Kyrgyzstan', 'Laos', 'Latvia', 'Lebanon',
                      'Lesotho', 'Liberia', 'Libya', 'Liechtenstein', 'Lithuania',
                      'Luxembourg', 'Madagascar', 'Malawi', 'Malaysia', 'Maldives',
                      'Mali', 'Malta', 'Marshall Islands', 'Mauritania', 'Mauritius',
                      'Mexico', 'Micronesia', 'Moldova', 'Monaco', 'Mongolia',
                      'Montenegro', 'Morocco', 'Mozambique', 'Myanmar', 'Namibia',
                      'Nauru', 'Nepal', 'Netherlands', 'New Zealand', 'Nicaragua',
                      'Niger', 'Nigeria', 'North Korea', 'North Macedonia', 'Norway',
                      'Oman', 'Pakistan', 'Palau', 'Palestine', 'Panama',
                      'Papua New Guinea', 'Paraguay', 'Peru', 'Philippines', 'Poland', 'Portugal',
                      'Qatar', 'Romania', 'Russia', 'Rwanda', 'Saint Kitts and Nevis',
                      'Saint Lucia', 'Saint Vincent and the Grenadines', 'Samoa',
                      'San Marino', 'Sao Tome and Principe', 'Saudi Arabia', 'Senegal',
                      'Serbia', 'Seychelles', 'Sierra Leone', 'Singapore', 'Slovakia',
                      'Slovenia', 'Solomon Islands', 'Somalia', 'South Africa', 'South Korea',
                      'South Sudan', 'Spain', 'Sri Lanka', 'Sudan', 'Suriname',
                      'Sweden', 'Switzerland', 'Syria', 'Taiwan', 'Tajikistan',
                      'Tanzania', 'Thailand', 'Timor-Leste', 'Togo', 'Tonga',
                      'Trinidad and Tobago', 'Tunisia', 'Turkey', 'Turkmenistan', 'Tuvalu',
                      'Uganda', 'Ukraine', 'United Arab Emirates (UAE)', 'United Kingdom',
                      'United States of America (USA)', 'Uruguay', 'Uzbekistan',
                      'Vanuatu', 'Vatican City (Holy See)', 'Venezuela', 'Vietnam',
                      'Yemen', 'Zambia', 'Zimbabwe' ]
list_of_genders = ['Male', 'Female']
list_of_ages = ['0-16', '17-25', '26-35', '36-45', '46-55', '56-65', '66-75', '76+']
list_of_incomes = ['0-10k', '10k-20k', '20k-40k', '40k-60k', '60k-100k', '100k-150k', '150k-250k', '250k+']

cidr_dict = {}
used_cidrs = []

def fix_certs():
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS)
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    ssl_context.check_hostname = True
    ssl_context.load_default_certs()
    if platform.system().lower() == 'darwin':
        import certifi
        ssl_context.load_verify_locations(
            cafile=os.path.relpath(certifi.where()),
            capath=None,
            cadata=None)
    return ssl_context

def build_country_cidrs():
    for country in list_of_countries:
        # Select a number of CIDRs to associate with that country
        num_cidrs = random.randrange(1, 10)
        cnt = 0;
        country_cidrs = []
        while cnt < num_cidrs:
            next_cidr = random.randrange(1, 16000000)
            if not next_cidr in used_cidrs:
                used_cidrs.append(next_cidr)
                country_cidrs.append(next_cidr)
                cnt += 1
        cidr_dict[country] = country_cidrs

def select_country():
    idx = random.randrange(0, len(list_of_countries))
    return list_of_countries[idx]

def select_cidr(country):
    cidr_list = cidr_dict[country]
    idx = random.randrange(0, len(cidr_list))
    return cidr_list[idx]

def make_ip(cidr):
    octet4 = random.randrange(0, 255)
    octet3 = cidr % 255
    cidr = int(cidr / 255)
    octet2 = cidr % 255
    octet1 = int(cidr / 255)
    ip = str(octet1)+ '.' + str(octet2) + '.' + str(octet3) + '.' + str(octet4)
    return ip

def make_filename(bucketname, dirname, num_files):
    idx = random.randrange(0, num_files)
    name = bucketname
    if dirname != '':
        name += ('/' + dirname) 
    name += ('/' + str(idx) + '.html')
    return name

def get_list_item(lst):
    index = random.randrange(0, len(lst))
    return lst[index]

def build_headers(country, ip):
    headers = {}
    headers.update({'X-country':country})
    headers.update({'X-client-IP':ip})
    headers.update({'X-gender':get_list_item(list_of_genders)})
    headers.update({'X-age':get_list_item(list_of_ages)})
    headers.update({'X-income':get_list_item(list_of_incomes)})
    today = str(date.today()) 
    time_of_day = random.randrange(0,24)
    time_str = today + ' ' + format(time_of_day, '02d') + ':00:00'
    headers.update({'X-time':time_str})
    return headers

def make_request(domain, port, country, ip, filename, use_ssl, ssl_context, follow, verbose):
    if verbose:
        print("Requesting ", filename, " from ", domain, port)
    conn = None
    if use_ssl:
        conn = http.client.HTTPSConnection(domain, port, context=ssl_context)
    else:
        conn = http.client.HTTPConnection(domain, port)

    headers = build_headers(country, ip)
    conn.request("GET", filename, headers=headers)
    res = conn.getresponse()
    data = res.read()
    if verbose:
        print(res.status, res.reason)
        print(res.msg)
        print(data)
    if follow:
        location_header = res.getheader('location')
        if location_header is not None:
            filename = urljoin(filename, location_header)
            make_request(domain, port, country, ip, filename, use_ssl, ssl_context, follow, verbose)
    conn.close()
                 
        
def main():
    ssl_context = fix_certs()
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--domain", help="Domain to make requests to", type=str, default="www.python.org")
    parser.add_argument("-b", "--bucket", help="Cloud bucket containing your files.  Use none if running local", type=str, default="bu-cds533-kthanasi-bucket1")
    parser.add_argument("-w", "--webdir", help="Directory containing your files.  Use none if you did not make one", type=str, default="webdir")
    parser.add_argument("-n", "--num_requests", help="Number of requests to make", type=int, default=100000)
    parser.add_argument("-i", "--index", help="Maximum existing file index", type=int, default=100000)
    parser.add_argument("-p", "--port", help="Server Port", type=int, default=80)
    parser.add_argument("-f", "--follow", help="Follow Redirects", action="store_true")
    parser.add_argument("-s", "--ssl", help="Use HTTPS", action="store_true")
    parser.add_argument("-v", "--verbose", help="Print the responses from the server on stdout", action="store_true")
    parser.add_argument("-r", "--random", help="Initial random seed", type=int, default=0)
    args = parser.parse_args()
    if args.random != 0:
        random.seed(args.random)

    build_country_cidrs()

    if args.bucket == 'none':
        args.bucket = ''
    if args.webdir == 'none':
        args.webdir = ''
    # Make the requests
    for i in range(0,args.num_requests):
        country = select_country()
        cidr = select_cidr(country)
        ip = make_ip(cidr)
        filename = make_filename(args.bucket, args.webdir, args.index)
        # If using the default port but have enabled ssl change the default port to be that of SSL
        if args.ssl and args.port==80:
            args.port=443
        make_request(args.domain, args.port, country, ip, filename, args.ssl, ssl_context, args.follow, args.verbose)

if __name__ == "__main__":
    main()