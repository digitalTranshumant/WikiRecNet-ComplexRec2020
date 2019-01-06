import mwxml
import gzip
import dateutil
import glob
import sys

def map_(pages, path):
    for page in pages:
        for rev in page:
            if rev.page.namespace != 0:
                continue

            if not rev.user or not rev.page:
                continue

            yield (rev.user.id, rev.user.text, rev.page.id,
                   rev.page.redirect or rev.page.title, rev.minor, rev.comment, rev.bytes, rev.timestamp.unix())

def parse(src, dst):
    files = glob.glob(src)

    with open(dst, 'w', buffering=100) as f:
        for r in mwxml.map(map_, files):
            f.write('\t'.join(map(str, r)) + '\n')


if __name__ == '__main__':
    if len(sys.argv[1:]) != 2:
        exit("Usage python parse-revisions.py enwiki-20181120-stub-meta-history[1-9]*.xml.gz en-revisions.tsv")

    parse(*sys.argv[1:])
