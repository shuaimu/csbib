#!/usr/bin/env python3

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

# Get the directory where this script is located (csbib directory)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Known conference/journal abbreviations from our database
KNOWN_VENUES = [
    'osdi', 'sosp', 'eurosys', 'atc', 'nsdi', 'sigcomm', 'sigmod', 'vldb',
    'fast', 'hotos', 'pldi', 'popl', 'oopsla', 'isca', 'asplos', 'socc',
    'dsn', 'cidr', 'podc', 'disc', 'spaa', 'focs', 'stoc', 'soda',
    'tocs', 'tods', 'tkde', 'toplas', 'cacm', 'jacm', 'csur'
]

# Common special names/acronyms that should be wrapped in {}
SPECIAL_NAMES = {
    # Networking & Systems
    'RDMA', 'DPDK', 'TCP', 'UDP', 'IP', 'IPv4', 'IPv6', 'HTTP', 'HTTPS',
    'DNS', 'BGP', 'SDN', 'NFV', 'P4', 'QUIC', 'TLS', 'SSL', 'VPN',
    'CDN', 'DDoS', 'QoS', 'VLAN', 'MPLS', 'OSPF', 'RPC', 'gRPC',

    # Storage & File Systems
    'SSD', 'HDD', 'NVMe', 'RAID', 'ZFS', 'NFS', 'HDFS', 'GFS', 'CIFS',
    'SMB', 'iSCSI', 'SAN', 'NAS', 'LVM', 'FUSE', 'ext4', 'btrfs', 'XFS',

    # Databases
    'SQL', 'NoSQL', 'ACID', 'BASE', 'CAP', 'OLTP', 'OLAP', 'ETL', 'ORM',
    'MVCC', '2PC', '3PC', 'OCC', 'CRDT', 'WAL', 'LSM',

    # Hardware & Architecture
    'CPU', 'GPU', 'TPU', 'FPGA', 'ASIC', 'ARM', 'x86', 'x64', 'RISC',
    'CISC', 'SIMD', 'NUMA', 'SMP', 'DMA', 'PCIe', 'DDR', 'DRAM', 'SRAM',
    'TLB', 'MMU', 'IOMMU', 'PMU', 'ISA', 'AVX', 'SSE', 'NEON',

    # Operating Systems & Virtualization
    'OS', 'VM', 'VMM', 'KVM', 'QEMU', 'Xen', 'VMware', 'VirtualBox',
    'BSD', 'UNIX', 'POSIX', 'API', 'ABI', 'ELF', 'JIT', 'AOT', 'GC',
    'IPC', 'RCU', 'BPF', 'eBPF', 'XDP', 'DPDK',

    # Distributed Systems
    'P2P', 'DHT', 'MapReduce', 'MPI', 'RMA', 'RDMA', 'CXL',
    'Paxos', 'Raft', 'PBFT', 'BFT', 'CFT', 'FLP', 'ZAB', 'VR',

    # Cloud & Containers
    'AWS', 'EC2', 'S3', 'GCP', 'GCE', 'GCS', 'Azure', 'IaaS', 'PaaS',
    'SaaS', 'FaaS', 'K8s', 'Kubernetes', 'Docker', 'LXC', 'cgroups',

    # Security
    'RSA', 'AES', 'DES', 'SHA', 'MD5', 'PKI', 'CA', 'CRL', 'OCSP',
    'SGX', 'TEE', 'TPM', 'HSM', 'SEV', 'TDX', 'MPC', 'ZKP', 'PIR',

    # Machine Learning
    'ML', 'DL', 'AI', 'NN', 'CNN', 'RNN', 'LSTM', 'GRU', 'GAN', 'VAE',
    'NLP', 'CV', 'RL', 'DNN', 'MLP', 'SVM', 'LLM', 'GPT', 'BERT',

    # Programming & Languages
    'JVM', 'CLR', 'LLVM', 'GCC', 'JDK', 'SDK', 'IDE', 'DSL', 'AST',
    'IR', 'CFG', 'DFG', 'PDG', 'SSA', 'CPS', 'STM', 'HTM',

    # Benchmarks & Standards
    'SPEC', 'TPC', 'TPCC', 'TPCH', 'YCSB', 'IEEE', 'ISO', 'ANSI', 'RFC',

    # Companies & Products (when used as system names)
    'Linux', 'Windows', 'macOS', 'iOS', 'Android', 'Chrome', 'Firefox',
    'MySQL', 'PostgreSQL', 'MongoDB', 'Redis', 'Cassandra', 'HBase',
    'Spark', 'Hadoop', 'Flink', 'Kafka', 'RabbitMQ', 'ZooKeeper',
    'ElasticSearch', 'Lucene', 'Solr', 'Neo4j', 'DynamoDB', 'CosmosDB'
}

# Typical months for conferences (based on historical data)
VENUE_MONTHS = {
    'osdi': 'nov',      # November (sometimes October)
    'sosp': 'oct',      # October
    'eurosys': 'apr',   # April
    'atc': 'jul',       # July (USENIX ATC)
    'nsdi': 'apr',      # April
    'sigcomm': 'aug',   # August
    'sigmod': 'jun',    # June
    'vldb': 'aug',      # August
    'fast': 'feb',      # February
    'hotos': 'may',     # May (workshop, varies)
    'pldi': 'jun',      # June
    'popl': 'jan',      # January
    'oopsla': 'oct',    # October
    'isca': 'may',      # May (varies)
    'asplos': 'mar',    # March
    'socc': 'nov',      # October/November (varies)
    'dsn': 'jun',       # June
    'cidr': 'jan',      # January
    'podc': 'jul',      # July
    'disc': 'oct',      # October (distributed computing)
    'spaa': 'jul',      # July
    'focs': 'oct',      # October (Fall)
    'stoc': 'jun',      # June (Spring/Summer)
    'soda': 'jan',      # January
    # Journals don't have months typically
    'tocs': None,
    'tods': None,
    'tkde': None,
    'toplas': None,
    'cacm': None,
    'jacm': None,
    'csur': None
}

# Parse command line arguments
n = len(sys.argv)
if n < 2:
    print("Usage: python bib-beautify.py <input.bib> [output.bib]")
    print("  If output.bib is not specified, creates <input>-beautified.bib")
    sys.exit(1)

SRC_FILE = sys.argv[1]
TGT_FILE = SRC_FILE.replace('.bib', '-beautified.bib')
if n > 2:
    TGT_FILE = sys.argv[2]


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

def process_title(title):
    """Process title to wrap special names in curly braces"""
    if not title:
        return title

    # Check if title starts with a system name (word ending with colon)
    words = title.split()
    if words and words[0].endswith(':'):
        # Wrap the system name in braces
        system_name = words[0][:-1]  # Remove the colon
        title = '{' + system_name + '}: ' + ' '.join(words[1:]) if len(words) > 1 else '{' + system_name + '}:'
        words = title.split()  # Re-split for further processing

    # Process the rest of the title for special names
    # We need to be careful not to wrap words that are already in braces
    result_words = []
    for word in words:
        # Skip if already wrapped in braces
        if word.startswith('{') and word.endswith('}'):
            result_words.append(word)
            continue

        # Check if word (without punctuation) is a special name
        # Extract the core word without leading/trailing punctuation
        prefix = ''
        suffix = ''
        core_word = word

        # Extract leading punctuation
        while core_word and not core_word[0].isalnum():
            prefix += core_word[0]
            core_word = core_word[1:]

        # Extract trailing punctuation
        while core_word and not core_word[-1].isalnum():
            suffix = core_word[-1] + suffix
            core_word = core_word[:-1]

        # Check if core word is a special name (case-insensitive for matching)
        found = False
        if core_word.upper() in SPECIAL_NAMES:
            # Find the correctly-cased version from our set
            for special in SPECIAL_NAMES:
                if core_word.upper() == special.upper():
                    result_words.append(prefix + '{' + special + '}' + suffix)
                    found = True
                    break

        # Check for compound terms like TCP/IP, ext4, x86/x64, etc.
        if not found and ('/' in core_word or '-' in core_word):
            # Split by / or - and check each part
            parts = re.split(r'[/-]', core_word)
            all_special = all(part.upper() in SPECIAL_NAMES for part in parts if part)
            if all_special:
                # Rebuild with correct casing
                result_parts = []
                for part in parts:
                    for special in SPECIAL_NAMES:
                        if part.upper() == special.upper():
                            result_parts.append(special)
                            break
                separator = '/' if '/' in core_word else '-'
                result_words.append(prefix + '{' + separator.join(result_parts) + '}' + suffix)
                found = True

        if not found:
            result_words.append(word)

    return ' '.join(result_words)

def generate_cite_key(entry):
    """Generate citation key following convention: lastnameYYfirstword"""
    # Get first author's last name
    author_field = entry.get("author", "")
    if not author_field:
        return entry.get("ID", "unknown")

    # Parse first author - handle various formats
    authors = author_field.split(" and ")
    first_author = authors[0].strip()

    # Extract last name (handle "Last, First" and "First Last" formats)
    if "," in first_author:
        last_name = first_author.split(",")[0].strip()
        # Check if this is a prefix like "van Renesse"
        parts = last_name.split()
        if len(parts) > 1 and parts[0].lower() in ['van', 'von', 'de', 'der', 'den', 'del', 'da', 'le', 'la']:
            last_name = parts[-1]
    else:
        # Assume last word is last name
        parts = first_author.split()
        last_name = parts[-1].strip()
        # Check for prefixes in "First van Last" format
        if len(parts) > 1 and parts[-2].lower() in ['van', 'von', 'de', 'der', 'den', 'del', 'da', 'le', 'la']:
            last_name = parts[-1]

    # Get year (last 2 digits)
    year = entry.get("year", "00")
    year_short = str(year)[-2:] if year else "00"

    # Get first word of title (excluding special characters and articles)
    title = entry.get("title", "")
    if not title:
        return f"{last_name.lower()}{year_short}"

    # Remove LaTeX commands and special chars from title
    title_clean = re.sub(r'[{}\\]', '', title)
    title_clean = re.sub(r'[^\w\s]', ' ', title_clean)

    # Split into words and get first meaningful word (skip articles)
    words = title_clean.split()
    skip_words = ['a', 'an', 'the', 'on', 'in', 'at', 'for', 'to', 'of', 'with']
    first_word = ""
    for word in words:
        if word.lower() not in skip_words:
            first_word = word.lower()
            break

    if not first_word:
        first_word = "paper"

    return f"{last_name.lower()}{year_short}{first_word}"

def detect_known_venue(entry):
    """Detect if entry is from a known conference/journal"""
    venue_name = ""
    if "journal" in entry:
        venue_name = entry["journal"].lower()
    if "booktitle" in entry:
        venue_name = entry["booktitle"].lower()

    for venue in KNOWN_VENUES:
        if venue in venue_name:
            return venue
    return None

def get_venue_template(venue_abbr):
    """Get a template entry from the venue's database file"""
    venue_file = os.path.join(SCRIPT_DIR, f"{venue_abbr}.bib")
    if not os.path.exists(venue_file):
        return None

    try:
        with open(venue_file, 'r') as f:
            venue_data = f.read()
        venue_db = bibtexparser.loads(venue_data)
        if venue_db.entries:
            # Return the first entry as template
            return venue_db.entries[0]
    except:
        return None
    return None

def beautify_with_template(entry, template, venue_abbr):
    """Beautify entry using template from known venue"""
    beautified = entry.copy()

    # Copy formatting style from template
    if template:
        if "booktitle" in template:
            # Use the same format as in the template
            beautified["booktitle"] = template["booktitle"]
            beautified["ENTRYTYPE"] = "inproceedings"
        elif "journal" in template:
            beautified["journal"] = template["journal"]
            beautified["ENTRYTYPE"] = "article"
    else:
        # Default based on original entry type, using string reference format
        if entry.get("ENTRYTYPE") == "inproceedings":
            beautified["booktitle"] = venue_abbr
        elif entry.get("ENTRYTYPE") == "article":
            beautified["journal"] = venue_abbr

    # Ensure required fields are present
    if "year" not in beautified:
        import datetime
        beautified["year"] = str(datetime.datetime.now().year)

    # Keep author from original
    if "author" in entry:
        beautified["author"] = entry["author"]

    # Process and beautify the title
    if "title" in entry:
        beautified["title"] = process_title(entry["title"])

    # Add month information if available for this venue
    if venue_abbr in VENUE_MONTHS and VENUE_MONTHS[venue_abbr]:
        beautified["month"] = VENUE_MONTHS[venue_abbr]

    # Remove pages information to follow convention
    if "pages" in beautified:
        del beautified["pages"]

    # Generate citation key following convention
    beautified["ID"] = generate_cite_key(beautified)

    return beautified

def prompt_add_to_database(entry, venue_abbr):
    """Prompt user to add entry to database"""
    writer = bibtexparser.bwriter.BibTexWriter()
    db = bibtexparser.bibdatabase.BibDatabase()
    db.entries = [entry]

    print(f"\nBeautified entry for {venue_abbr.upper()}:")
    print(writer.write(db))

    response = input(f"Add this entry to the {venue_abbr}.bib database? (y/n): ")
    return response.lower() == 'y'

def insert_entry_chronologically(entry, venue_abbr):
    """Insert entry into venue database file in chronological order"""
    venue_file = os.path.join(SCRIPT_DIR, f"{venue_abbr}.bib")

    # Read the existing file
    with open(venue_file, 'r') as f:
        content = f.read()

    # Get the year of the new entry
    new_year = int(entry.get('year', 0))

    # Find the position to insert
    lines = content.split('\n')
    insert_line = None

    # Track the current entry being processed
    in_entry = False
    current_year = None
    entry_start = -1

    for i, line in enumerate(lines):
        line_stripped = line.strip()

        # Check if we're starting a new entry
        if line_stripped.startswith('@'):
            in_entry = True
            entry_start = i
            current_year = None

        # Look for year field
        if in_entry and 'year' in line.lower():
            # Extract year value
            match = re.search(r'year\s*=\s*[{"]?(\d{4})[}"]?', line, re.IGNORECASE)
            if match:
                current_year = int(match.group(1))

        # Check if we're ending an entry
        if in_entry and line_stripped == '}':
            in_entry = False
            if current_year is not None and current_year > new_year:
                # Found the first entry with year > new_year
                # Insert before this entry
                insert_line = entry_start
                # Add a blank line before if previous line is not blank
                if entry_start > 0 and lines[entry_start - 1].strip():
                    insert_line = entry_start
                break
            elif current_year is not None and current_year == new_year:
                # Found an entry with the same year, insert after this entry
                # Continue to find all entries with the same year
                continue

    # If we didn't find a position (all entries are older or same year), append at the end
    if insert_line is None:
        insert_line = len(lines)

    # Format the new entry
    entry_str = f"\n@{entry['ENTRYTYPE']}{{{entry['ID']},\n"

    # Format fields
    for key, value in entry.items():
        if key not in ['ENTRYTYPE', 'ID']:
            if key == 'booktitle' and value == venue_abbr:
                # Use string reference format without quotes
                entry_str += f"  {key}={value},\n"
            else:
                # Use quoted format for other fields
                entry_str += f"  {key}={{{value}}},\n"

    entry_str = entry_str.rstrip(",\n") + "\n}\n"

    # Insert the entry at the determined position
    lines.insert(insert_line, entry_str.rstrip('\n'))

    # Write back the modified content
    with open(venue_file, 'w') as f:
        f.write('\n'.join(lines))

def append_to_venue_database(entry, venue_abbr):
    """Append entry to venue database file (fallback function)"""
    venue_file = os.path.join(SCRIPT_DIR, f"{venue_abbr}.bib")

    # Manually format the entry to use string reference style for booktitle
    entry_str = f"\n@{entry['ENTRYTYPE']}{{{entry['ID']},\n"

    # Format fields
    for key, value in entry.items():
        if key not in ['ENTRYTYPE', 'ID']:
            if key == 'booktitle' and value == venue_abbr:
                # Use string reference format without quotes
                entry_str += f"  {key}={value},\n"
            else:
                # Use quoted format for other fields
                entry_str += f"  {key}={{{value}}},\n"

    entry_str = entry_str.rstrip(",\n") + "\n}\n"

    with open(venue_file, 'a') as f:
        f.write(entry_str)

# Load all BibTeX database files from the csbib directory
bib_pattern = os.path.join(SCRIPT_DIR, '*.bib')
title_pattern = os.path.join(SCRIPT_DIR, 'title*.bib')
files = list(set(glob.glob(bib_pattern)) - set(glob.glob(title_pattern)))
files.insert(0, os.path.join(SCRIPT_DIR, "title.bib"))

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

        # Check if this is from a known conference/journal
        venue_abbr = detect_known_venue(entry)
        if venue_abbr:
            print(f"Detected known venue: {venue_abbr.upper()}")
            template = get_venue_template(venue_abbr)

            # Beautify the entry using the template (or defaults if no template)
            beautified = beautify_with_template(entry, template, venue_abbr)

            # Ask user if they want to add it to the database
            if prompt_add_to_database(beautified, venue_abbr):
                insert_entry_chronologically(beautified, venue_abbr)
                print(f"Entry added to {venue_abbr}.bib database in chronological order")
                # Add the new entry to the in-memory database so it won't be matched again
                bib_database.entries.append(beautified)

            result = beautified
        else:
            # Try ACM lookup for VLDB/SIGMOD as before
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
            else:
                result = entry
    if len(results) > 1: 
        for e2 in results:
            if entry["ENTRYTYPE"] == e2["ENTRYTYPE"]:
                result = e2
    ids = []
    if "ids" in result.keys():
        ids+=result["ids"].split(",")

    # For beautified entries, preserve the original ID in the ids field
    # but keep the new generated citation key as the main ID
    if result.get("ID") != entry["ID"]:
        # This is a beautified entry with a new citation key
        ids.append(entry["ID"])
    else:
        # Not beautified, keep original ID
        ids.append(entry["ID"])
        result["ID"] = entry["ID"]

    if "ids" in entry.keys():
        ids += entry["ids"].split(",")
    ids = [*(set(ids)-set([result["ID"]]))]
    if len(ids) > 0:
        result["ids"] = ",".join(ids)
    else:
        result.pop('ids', None)
    bib_list.append(result) 


res_db = bibtexparser.bibdatabase.BibDatabase()
result_entries = []
[result_entries.append(x) for x in bib_list if x not in result_entries] 
res_db.entries = result_entries


with open(TGT_FILE, 'w') as bibtex_file:
    bibtexparser.dump(res_db, bibtex_file)

