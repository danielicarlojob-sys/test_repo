# Makefile for formatting, linting, testing, and coverage

.PHONY: setup format lint test coverage all full

# Create virtual environment and install dependencies from requirements.txt
setup:
	@echo "🌱 Setting up virtual environment..."
	@python -m venv venv
	@venv/bin/pip install --upgrade pip
	@venv/bin/pip install -r requirements.txt
	@echo "✅ Environment setup complete."

# Format code using autopep8
format:
	@echo "🔧 Formatting code in src/ and tests/..."
	@venv/bin/autopep8 --in-place --recursive --aggressive --aggressive src/
	@venv/bin/autopep8 --in-place --recursive --aggressive --aggressive tests/
	@echo "✅ Formatting done."

# Lint code using flake8 (ignore line length for now)
lint:
	@echo "🔍 Linting code with flake8..."
	@venv/bin/flake8 --ignore=E501,W503,W504 src/*.py tests/*.py
	@echo "✅ Linting complete."

# Run tests with pytest and testdox output, with PYTHONPATH set so src/ is importable
test:
	@echo "🧪 Running tests..."
	@PYTHONPATH=src venv/bin/pytest --testdox
	@echo "✅ Tests complete."

# Run coverage report
coverage:
	@echo "📊 Running coverage analysis..."
	@PYTHONPATH=src venv/bin/coverage run -m pytest
	@venv/bin/coverage report -m
	@venv/bin/coverage html
	@echo "✅ Coverage report generated (see htmlcov/index.html)."

# Format checks
form: format lint 

# Run everything except setup
all: format lint test coverage

# Run everything including setup
full: setup all
