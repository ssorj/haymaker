.PHONY: devel import

# python3-requests, python3-tornado

devel:
	haymaker

import:
	scripts/fetch-data
	rm -f data/data.sqlite
	scripts/import-data
