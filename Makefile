lint:
	# stop the build if there are Python syntax errors or undefined names
	docker-compose -f docker-compose.test.yml exec -T pagai flake8 pagai test --count --select=E9,F63,F7,F82 --show-source --statistics
	# TODO: fix python code
	# flake8 pagai test --count --max-complexity=10 --max-line-length=100 --statistics

tests:
	docker-compose -f docker-compose.test.yml exec -T pagai python -m pytest -svv test/

docker-base:
	docker build -t arkhn/python-base:latest -f Dockerfile.base .

docker-setup-tests: docker-base
	docker-compose -f docker-compose.test.yml build
	docker-compose -f docker-compose.test.yml up -d
