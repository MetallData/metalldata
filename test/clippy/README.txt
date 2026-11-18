The build sets up a Python venv for these tests when tests are enabled:

1) configure with tests on and build:
     cmake -DMETALLDATA_BUILD_TESTS=ON ..
     make
2) run the tests (from the build directory):
     make pytest

To run pytest by hand instead:

     source <build>/metalldata-venv/bin/activate
     cd test/clippy
     DATA_DIR=../../data/metall_graph pytest tests/

If you configured with METALLDATA_PYTHON_VENV_ROOT, the build leaves that venv
alone: install requirements.txt and requirements-dev.txt into it yourself and
set PYTHONPATH=<build>/py.
