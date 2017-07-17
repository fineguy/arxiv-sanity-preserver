"""
Queries arxiv API and downloads papers (the query is a parameter).
The script is intended to enrich an existing database pickle (by default db.p),
so this file will be loaded first, and then new results will be added to it.
"""

import time
import pickle
import random
import argparse
import urllib.request
import feedparser

from utils import Config, safe_pickle_dump

RETRIES = 5  # maximum number of retries for sending a query
TIMEOUT = 600  # sleep time in case of a query failure
BASE_URL = 'http://export.arxiv.org/api/query?search_query=%s&' \
           'sortBy=lastUpdatedDate&start=%i&max_results=%i'  # base api query url


def encode_feedparser_dict(d):
  """
  Helper function to get rid of feedparser bs with a deep copy.
  I hate when libs wrap simple things in their own classes.
  """
  if isinstance(d, feedparser.FeedParserDict) or isinstance(d, dict):
    j = {}
    for k in d.keys():
      j[k] = encode_feedparser_dict(d[k])
    return j
  elif isinstance(d, list):
    l = []
    for k in d:
      l.append(encode_feedparser_dict(k))
    return l
  else:
    return d


def parse_arxiv_url(url):
  """
  Examples is http://arxiv.org/abs/1512.08756v2
  We want to extract the raw id and the version
  """
  idversion = url.rsplit('/', 1)[-1]  # extract just the id (and the version)
  parts = idversion.split('v')
  assert len(parts) == 2, 'error parsing url ' + url
  return parts[0], int(parts[1])


def send_request(query, index, rpi, retries=RETRIES, timeout=TIMEOUT):
  """
  Attempt new requests in case arxiv returns an empty list.
  """
  url_query = BASE_URL % (query, index, rpi)
  for i in range(1, retries + 1):
    with urllib.request.urlopen(url_query) as url:
      response = url.read()
    parse = feedparser.parse(response)
    if len(parse.entries) == 0 and i < retries:
      print("Request %d out of %d failed, retry in %.2f seconds" % (i, retries, timeout))
      time.sleep(timeout)
      continue
    return response, parse.entries


def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--search-query', type=str, default='cat:cs.CV+OR+cat:cs.AI+OR+cat:cs.LG+OR+'
                      'cat:cs.CL+OR+cat:cs.NE+OR+cat:stat.ML', help='query used for arxiv API. '
                      'See http://arxiv.org/help/api/user-manual#detailed_examples')
  parser.add_argument('--start-index', type=int, default=0, help='0 = most recent API result')
  parser.add_argument('--max-index', type=int, default=10000,
                      help='upper bound on paper index we will fetch')
  parser.add_argument('--results-per-iteration', type=int, default=100, help='passed to arxiv API')
  parser.add_argument('--wait-time', type=float, default=5.0,
                      help='lets be gentle to arxiv API (in number of seconds)')
  parser.add_argument('--no-break-on-no-added', action='store_false', help='no break out early '
                      'if all returned query papers are already in db')
  return parser.parse_args()


if __name__ == "__main__":
  args = parse_args()

  # misc hardcoded variables
  print('Searching arXiv for %s' % (args.search_query, ))

  # lets load the existing database to memory
  try:
    db = pickle.load(open(Config.db_path, 'rb'))
  except Exception as e:
    print('error loading existing database:')
    print(e)
    print('starting from an empty database')
    db = {}

  # -----------------------------------------------------------------------------
  # main loop where we fetch the new results
  print('database has %d entries at start' % (len(db), ))
  num_added_total = 0
  start_index = args.start_index
  for index in range(args.start_index, args.max_index, args.results_per_iteration):
    print("Results %i - %i" % (index, index + args.results_per_iteration))
    response, entries = send_request(args.search_query, index, args.results_per_iteration)
    num_added = 0
    num_skipped = 0

    for e in entries:
      j = encode_feedparser_dict(e)

      # extract just the raw arxiv id and version for this paper
      rawid, version = parse_arxiv_url(j['id'])
      j['_rawid'] = rawid
      j['_version'] = version

      # add to our database if we didn't have it before, or if this is a new version
      if rawid not in db or j['_version'] > db[rawid]['_version']:
        db[rawid] = j
        print('Updated %s added %s' % (j['updated'].encode('utf-8'), j['title'].encode('utf-8')))
        num_added += 1
      else:
        num_skipped += 1

    num_added_total += num_added

    # print some information
    print('Added %d papers, already had %d.' % (num_added, num_skipped))

    if len(entries) == 0:
      print('Received no results from arxiv. Rate limiting? Exiting. Restart later maybe.')
      print(response)
      break

    if num_added == 0 and args.no_break_on_no_added:
      print('No new papers were added. Assuming no new papers exist. Exiting.')
      break

    print('Sleeping for %.2f seconds' % (args.wait_time, ))
    time.sleep(args.wait_time + random.uniform(0, 3))

  # save the database before we quit, if we found anything new
  if num_added_total > 0:
    print('Saving database with %d papers to %s' % (len(db), Config.db_path))
    safe_pickle_dump(db, Config.db_path)
