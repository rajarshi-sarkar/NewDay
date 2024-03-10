all: unit-test build

unit-test:
	pytest test

build:
	rmdir /s .\dist && mkdir .\dist
	copy .\main.py .\dist
	copy .\configuration.json .\dist
	tar -cvzf dist\jobs.zip jobs
	tar -cvzf dist\extract.zip extract
	tar -cvzf dist\transform.zip transform
	tar -cvzf dist\load.zip load