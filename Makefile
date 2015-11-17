.PHONY: devel

# python-requests

devel:
#	scripts/fetch-data
	rm -f data/data.sqlite
	scripts/import-data
#	sqlite3 data/data.sqlite
