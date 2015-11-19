.PHONY: devel import

# python3-requests, python3-tornado

devel:
	brbn --app haymaker

import:
	scripts/fetch-data
	rm -f data/data.sqlite
	scripts/import-data
