#!/bin/bash
if [ "$PYPROFILE" != "" ]; then
    PYTHON="python2.4 -m profile $PYPROFILE"
else
    PYTHON="python2.4"
fi
PYTHONPATH=.:$PYTHONPATH $PYTHON run_simulator.py $*
