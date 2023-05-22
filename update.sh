#!/bin/bash

LOGFILE=graz_new.txt

cd /home/gjankowiak/tmp/webscraper_sophie
source .venv/bin/activate

env $(grep -v '^#' .env | xargs) scrapy crawl willhaben 2>&1 | grep '^ > ' >> $LOGFILE.tmp

if [ -s $LOGFILE.txt ];
then
    date >> $LOGFILE
    cat $LOGFILE.tmp >> $LOGFILE
    echo >> $LOGFILE

    scp $LOGFILE nuiton:/var/www/oknaj.eu/p/
fi

rm $LOGFILE.tmp

