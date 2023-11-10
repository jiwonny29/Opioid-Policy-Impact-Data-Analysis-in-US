install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt
		
test:
	python -m pytest -vv --cov=main --cov=lib test_*.py

format:	
	black src/*.py

lint:
	pylint  --disable=R,C src/*.py