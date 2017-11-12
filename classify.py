#!/usr/bin/env python
# -*- coding: utf-8 -*- 

import csv
import logging
import os
import re
import shutil
from sets import Set
import sys
import time

# Setup logger
LOG_LEVEL = logging.INFO
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)
ch_formatter = logging.Formatter('%(asctime)-15s %(levelname)-8s%(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(LOG_LEVEL)
console_handler.setFormatter(ch_formatter)
logger.addHandler(console_handler)

fl_formatter = logging.Formatter('%(asctime)-15s %(levelname)-8s%(message)s')
filelog_handler = logging.FileHandler("trace.log", encoding="utf-8")
filelog_handler.setLevel(LOG_LEVEL)
filelog_handler.setFormatter(fl_formatter)
logger.addHandler(filelog_handler)

KEY_WORDS = [
       "sample"
]

SINGLE_KEY = Set(["single"])

OUTPUT_DIR = "out"
HEADER_CSV = "header.csv"
DATA_DIR = "data_"
ARCHIVED_TWEETS = os.path.join("archived", "tweets")
TEXT_COL = 311 # "KY"
ID_COL = 18 # "R"
MAX_ROWS = 5000

class WordItem:    
    def __init__(self, name, csvfile, idfile, ids):
        self.name = name
        self.csvfile = csvfile
        self.idfile = idfile
        self.ids = ids
        self.rows = []

    def add(self, row):
        if len(self.rows) == MAX_ROWS:
            self.flush()
            self.rows = []
        
        id = row[ID_COL-1]
        if id not in self.ids:
            self.ids.add(id)
            self.rows.append(row)
            
    def flush(self):
        with open(self.csvfile, 'a') as cf:
            writer = csv.writer(cf, quoting=csv.QUOTE_ALL)
            for row in self.rows:
                writer.writerow(row)

    def saveids(self):
        with open(self.idfile, 'w') as f:
            for id in self.ids:
                f.write(id + "\n")
        
def load(idfile):
    ids = Set()
    with open(idfile) as f:
        content = f.readlines()
        for line in content:
            ids.add(line.strip())
    return ids
       
def findWord(w):
    return re.compile(r'\b({0})\b'.format(w), flags=re.IGNORECASE).search       

def main():
    start_time = time.time()
    logger.info("Start time: %s" % start_time)
    
    if not os.path.isdir(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    keyword_dict={}
    for word in KEY_WORDS:
        word = word.replace('/', '')
        word_list = word.split(" ")
        name = "_".join(word_list)
        csvfile = os.path.join(OUTPUT_DIR, name + ".csv")        
        idfile = os.path.join(OUTPUT_DIR, name + ".id")
        ids = Set()
        if not os.path.isfile(csvfile):            
            shutil.copyfile(HEADER_CSV, csvfile)
        if os.path.isfile(idfile):
            ids = load(idfile)
            
        keyword_dict[word] = WordItem(name, csvfile, idfile, ids)            
            
    for data_dir in os.listdir("."):
        if DATA_DIR in data_dir:
            logger.info("scanning %s ..." % data_dir)
            tweet_path = os.path.join(data_dir, ARCHIVED_TWEETS)                        
            for tweet_dir in os.listdir(tweet_path):
                csv_path = os.path.join(tweet_path, tweet_dir)
                if os.path.isdir(csv_path):
                    logger.info("processing csv path %s ..." % csv_path)
                    for file in os.listdir(csv_path):                        
                        if file.endswith(".csv"):
                            csv_datafile = os.path.join(csv_path, file)
                            logger.info("reading csv file %s ..." % csv_datafile)
                            with open(csv_datafile, 'r') as cf:
                                reader = csv.reader(cf, quoting=csv.QUOTE_ALL)
                                for row in reader:
                                    if len(row) >= TEXT_COL:                                        
                                        for key, value in keyword_dict.iteritems():
                                                if key in SINGLE_KEY:
                                                    if findWord(key)(row[TEXT_COL-1]) != None:                                                                                
                                                        value.add(row)
                                                else:
                                                    if re.search(key, row[TEXT_COL-1], re.IGNORECASE):                                                                                
                                                        value.add(row)
                                                        
    for value in keyword_dict.itervalues():
        value.saveids()
        value.flush() 
    
    end_time = time.time()
    logger.info("%s seconds elapsed." % (end_time - start_time))
    
if __name__ == "__main__":
    main()
