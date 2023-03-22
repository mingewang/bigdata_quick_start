#!/bin/bash

java -jar ./avro-tools-1.11.1.jar fromjson --schema-file ./book.avsc ./books.txt > books.asvo
java -jar ./avro-tools-1.11.1.jar fromjson --schema-file ./book.avsc ./books2.txt > books2.asvo
