#!/usr/bin/env bash

WIKIDATA={WIKIDATA:-wikidata-20181112-all.json.bz2}

lbzip2 -cd ${INPUT} | head -n -1 | tail -n +2 | \
    sed 's/.$//' | jq -c .sitelinks | jq -c 'map({(.site):(.title)}) | add' | tee wikidata-flatten.json