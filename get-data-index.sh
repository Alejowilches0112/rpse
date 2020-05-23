#!/bin/bash

BASE_FOLDER=../cc-output/

test -d input || mkdir input

if [ -e input/warc_t.txt ]; then
	echo "Archivo input/warc_all.txt ya existe"
	echo "Por favor borre todo el contenido de input para generar de nuevo los archivos"
	exit 1
fi

for f in out/t/*; do
    if [ -e "$f" ]; then
        ##find $f -maxdepth 1 -name "*.warc" | xargs realpath >>input/warc_$(basename $f).txt
        find $f -maxdepth 1 -name "*.warc" | 
        xargs -0 realpath >> input/warc_t.txt
    fi
done
