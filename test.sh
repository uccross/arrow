
#!/bin/bash
set -ex

g++ main.cc -larrow -larrow_dataset -o main
export LD_LIBRARY_PATH=/usr/local/lib
time ./main
