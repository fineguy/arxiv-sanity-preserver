from multiprocessing.dummy import Pool
import os
import pickle
import time
from urllib.request import urlretrieve

from utils import Config

RETRIES = 5  # maximum number of retries for fetching a pdf
TIMEOUT = 10  # sleep time in case of a fetching failure
THREADS = 40  # number of downloading threads
if not os.path.exists(Config.pdf_dir):
  os.makedirs(Config.pdf_dir)
EXISTING_PAPERS = set(os.listdir(Config.pdf_dir))  # get list of all pdfs we already have
NUM_OK = 0
DB = pickle.load(open(Config.db_path, 'rb'))


def save_pdf(pid, j):
  pdfs = [x['href'] for x in j['links'] if x['type'] == 'application/pdf']
  assert len(pdfs) == 1
  pdf_url = pdfs[0] + '.pdf'
  basename = os.path.basename(pdf_url)
  fname = os.path.join(Config.pdf_dir, basename)

  def download(retries=RETRIES, timeout=TIMEOUT):
    success = False
    for i in range(RETRIES):
      try:
        urlretrieve(pdf_url, fname)
        success = True
        break
      except Exception:
        time.sleep(TIMEOUT)
    return success

  global NUM_OK
  try:
    if basename not in EXISTING_PAPERS:
      success = download()
      if success:
        print('fetched %s into %s' % (pdf_url, fname))
      else:
        print('error downloading: ', pdf_url)
    else:
      print('%s exists, skipping' % (fname, ))
    NUM_OK += 1
  except Exception as e:
    print('error downloading: ', pdf_url)
    print(e)


with Pool(THREADS) as pool:
  pool.starmap(save_pdf, DB.items())

print('final number of papers downloaded okay: %d/%d' % (NUM_OK, len(DB)))
