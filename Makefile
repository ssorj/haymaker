.PHONY: devel import clean

# python3-requests, python3-tornado

devel:
	haystack

import:
	scripts/fetch-data
	scripts/import-data

.PHONY: import-%
update-%:
	curl "https://raw.githubusercontent.com/ssorj/$*/master/python/$*.py" -o python/$*.py
