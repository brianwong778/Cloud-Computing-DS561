#!env python3
import argparse
import random

def add_text(f):
  text = "Lorem ipsum dolor sit amet, \
consectetur adipiscing elit, sed do \
eiusmod tempor incididunt ut labore \
et dolore magna aliqua. Ut enim ad\n\
minim veniam, quis nostrud exercitation \
ullamco laboris nisi ut aliquip ex ea \
commodo consequat. Duis aute irure dolor \
in reprehenderit in voluptate velit esse\n\
cillum dolore eu fugiat nulla pariatur. \
Excepteur sint occaecat cupidatat non \
proident, sunt in culpa qui officia \
deserunt mollit anim id est laborum.\n<p>\n"
  f.write(text)

def add_headers(f):
  text = "<!DOCTYPE html>\n\
<html>\n\
<body>\n"
  f.write(text)


def add_footers(f):
  text = "</body>\n\
</html>\n"
  f.write(text)

def add_link(f, lnk):
  text = "<a HREF=\""
  f.write(text)
  text = str(lnk) + ".html\""
  f.write(text)
  text = "> This is a link </a>\n<p>\n"
  f.write(text)

def generate_file(idx, max_refs, num_files):
  fname = str(idx) + ".html"
  with open(fname, 'w', encoding="utf-8") as f:
    # how many references in this file
    add_headers(f)
    num_refs = random.randrange(0,max_refs)
    for i in range(0,num_refs):
      add_text(f)
      lnk = random.randrange(0,num_files)
      add_link(f, lnk)
    add_footers(f)
    f.close()

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('-n', '--num_files', help="Specify the number of files to generate", type=int, default=10000)
  parser.add_argument('-m', '--max_refs', type=int, help="Specify the maximum number of references per file", default=250)
  args = parser.parse_args()
  
  random.seed(0)

  print(args.num_files, args.max_refs)
  for i in range(0,args.num_files):
    generate_file(i, args.max_refs, args.num_files)

if __name__ == "__main__":
  main()