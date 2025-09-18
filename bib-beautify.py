
import bibtexparser
import glob
import os 
from fuzzywuzzy import fuzz
from bs4 import BeautifulSoup
import json
import requests
import sqlite3
import re
import time
import sys

# target_data = open("../23large/ref.bib")
# target_data = open("../geolis-paper/ref.bib")
# SRC_FILE = "../warbler-paper/ref.bib"
# SRC_FILE = "/Users/shuai/git/causalmesh-paper/paper.bib"
# SRC_FILE = "../nsf-23-serverless/ref.bib"
SRC_FILE = "../application/cv/ref.bib"
TGT_FILE = SRC_FILE

SCRIPT_DIR = os.path.dirname(os.path.realpath(sys.argv[0])) + '/'


n = len(sys.argv);
if n > 1: 
    SRC_FILE=sys.argv[1]
TGT_FILE = SRC_FILE.replace('.bib', '-beautified.bib')
if n > 2:
    TGT_FILE=sys.argv[2]


def search_doc(query):
    time.sleep(10)
    kvs = {}
    try:
        r = requests.get('https://dl.acm.org/action/doSearch', params={'AllField':query})
        parsed_html = BeautifulSoup(r.text, 'html.parser')
        t = parsed_html.body.find('div', attrs={'class':'issue-item__content'}).text
        res = re.findall('org.*', t)
        doi = res[0][4:]
        print(doi)
        r = requests.post('https://dl.acm.org/action/exportCiteProcCitation', data={
            'dois': doi,
            'targetFile': 'custom-bibtex',
            'format': 'bibTex'
        })
        j = json.loads(r.text)
        kvs = list(j['items'][0].items())[0][1]
    except:
        print("Error searching ACM")
        pass
    # print(kvs)
    return kvs

def json_to_bib(j):
    pass

# files = list(set(glob.glob(SCRIPT_DIR+'*.bib')) - set(glob.glob(SCRIPT_DIR+"title*.bib")))
# files.insert(0, SCRIPT_DIR+"title.bib")
files = list(set(glob.glob('*.bib')) - set(glob.glob("title*.bib")))
files.insert(0, "title.bib")

data = ""
for f in files:
    data += open(f).read()
# d1 = open("title_short.bib", "r").read()
# d2 = open("osdi.bib").read()
bib_database = bibtexparser.loads(data)
# print(bib_database.entries)

target_data = open(SRC_FILE)
# target_data = open("../application/cv/ref.bib")
target_bib = bibtexparser.load(target_data)
# print(target_bib.entries)

bib_list = []
for entry in target_bib.entries: 
    results = []
    result = entry
    r = 0
    title = ""
    if "title" in entry.keys():
        title = entry["title"]
        for e2 in bib_database.entries:
            r = fuzz.token_set_ratio(entry["title"], e2["title"]);
            if r == 100:
                results.append(e2);
                print("Found match ratio, " + str(r) + ": " + title)
    if len(results) > 0:
        result = results[0]
    else:
        print("Nothing found for: " + title)
        writer = bibtexparser.bwriter.BibTexWriter()
        db = bibtexparser.bibdatabase.BibDatabase()
        db.entries = [entry]
        print(writer.write(db))
        
        journal = "" 
        if "journal" in entry:
            journal = entry["journal"].lower()
        if "booktitle" in entry:
            journal = entry["booktitle"].lower()
        # if False:
        if (entry["ENTRYTYPE"] == "article" or entry["ENTRYTYPE"] == "inproceedings") and ("vldb" in journal or "sigmod" in journal):
            print("Search ACM database for it")
            acm_res = search_doc(title)
            result = {}
            result["ENTRYTYPE"] = entry["ENTRYTYPE"]
            result["ID"] = entry["ID"]
            result["title"] = entry["title"]
            result["author"] = entry["author"]
            if len(acm_res) > 0 and fuzz.token_set_ratio(acm_res['title'], title) == 100:
                print("Found on ACM")
                if acm_res["type"] == "PAPER_CONFERENCE":
                    result["ENTRYTYPE"] = "inproceedings"
                    result["booktitle"] = acm_res["container-title"]
                elif acm_res["type"] == "ARTICLE":
                    result["ENTRYTYPE"] = "article"
                    result["journal"] = acm_res["container-title"]
                    result["volume"] = acm_res["volume"]
                    result["number"] = acm_res["issue"]
                else:
                    print("Error")
                result["year"] = "{}".format(acm_res["issued"]["date-parts"][0][0])
                result["month"] = "{}".format(acm_res["issued"]["date-parts"][0][1])
                db.entries = [result]
                print(writer.write(db))
            else:
                print("Didn't find exact match on ACM")
                result = entry
    if len(results) > 1: 
        for e2 in results:
            if entry["ENTRYTYPE"] == e2["ENTRYTYPE"]:
                result = e2
    ids = []
    if "ids" in result.keys():
        ids+=result["ids"].split(",")
    ids.append(entry["ID"]) 
    if "ids" in entry.keys():
        ids += entry["ids"].split(",")
    ids = [*(set(ids)-set([result["ID"]]))]
    if len(ids) > 0:
        result["ids"] = ",".join(ids)
    else:
        result.pop('ids', None)
    result["ID"] = entry["ID"]
    bib_list.append(result) 


res_db = bibtexparser.bibdatabase.BibDatabase()
result_entries = []
[result_entries.append(x) for x in bib_list if x not in result_entries] 
res_db.entries = result_entries


with open(TGT_FILE, 'w') as bibtex_file:
    bibtexparser.dump(res_db, bibtex_file)

