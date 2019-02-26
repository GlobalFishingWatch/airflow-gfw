# airflow-gfw

[Airflow](https://airflow.apache.org/) extension for GFW pipeline.

## Installation

You just need [docker](https://www.docker.com/) and
[docker-compose](https://docs.docker.com/compose/) in your machine to run the
pipeline. No other dependency is required.

## Usage

Run unit tests
  Quick run
  `docker-compose run tests`

  Run with all tests including ones that hit some GCP API
  `docker-compose run tests --runslow`

Re-build the docker environment (needed if you modify setup.py or other environmental change)
  `docker-compose build -t gfw/airflow-gfw:<version> .`

You can run the unit tests outside of docker like this
  ` py.test tests`
which may be convenient when debugging stuff.  If you do this then you will need
to clear out the `__pycache__` with
    `rm -rf tests/__pycache__/`

## Development

To setup your development environment you have 2 options. The recommended way is through [docker](https://www.docker.com/), although you can also use a local [python](https://www.python.org/) installation if you prefer so.


### Local python

You need to have [python](https://www.python.org/) 2.7. [Virtualenv](https://virtualenv.pypa.io/en/stable/) is recommended as well. Just run the following commands to run the tests, and you are ready to start hacking around:

```cconsole
virtualenv venv
source venv/bin/activate
pip install -e .
py.test tests
```
