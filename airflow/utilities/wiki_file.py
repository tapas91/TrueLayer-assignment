""" Creates a Filtered csv file from gz file with 3 required coulmns."""

import gzip
import xml.etree.cElementTree as ET
import codecs
import csv


def extract_wiki_file(*args, **kwargs):
    wiki_file = "/usr/local/app/downloads/enwiki-latest-abstract.xml.gz"
    filtered_wiki_csv = "/usr/local/app/downloads/filtered-wiki.csv"

    filtered_columns = ["title", "abstract", "url"]

    wiki_entry_flag = False
    wiki_entry_dict = {}

    dump_data = gzip.open(wiki_file, 'rb')

    with codecs.open(filtered_wiki_csv, "w", "utf-8") as filtered_csv:

        csv_writer = csv.writer(filtered_csv, quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(filtered_columns)

        for event, elem in ET.iterparse(dump_data, events=['start', 'end']):
            if event == "start" and elem.tag == "doc":
                wiki_entry_flag = True
                wiki_entry_dict = {}

            #Fetch data for filtered_columns
            if wiki_entry_flag:
                if elem.tag in filtered_columns:
                    if elem.tag == "title" and elem.text:

                        new_title = elem.text.split("Wikipedia:")[-1].split("(")[0]
                        elem.text = new_title.strip()
                    wiki_entry_dict[elem.tag] = elem.text

            if wiki_entry_flag and event == "end" and elem.tag == "doc":
                csv_writer.writerow([wiki_entry_dict['title'], wiki_entry_dict['abstract'], wiki_entry_dict['url']])

        elem.clear()

    print('Extraction from given gz file completed!!!')