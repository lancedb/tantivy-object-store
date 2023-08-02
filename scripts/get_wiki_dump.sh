#!/bin/bash

wget https://dumps.wikimedia.org/simplewiki/20230720/simplewiki-20230720-pages-articles-multistream.xml.bz2
bzip2 -d simplewiki-20230720-pages-articles-multistream.xml.bz2

wget https://raw.githubusercontent.com/powerlanguage/word-lists/master/1000-most-common-words.txt