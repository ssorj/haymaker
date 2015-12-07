.PHONY: devel import clean

# python3-requests, python3-tornado

devel:
	haystack

import:
	scripts/fetch-data
	rm -f data/data.sqlite
	scripts/import-data
