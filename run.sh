#!/bin/bash

icsv="./sample.d/input.csv"
mpq="./sample.d/temp.parquet"
opq="./sample.d/output.parquet"

geninput(){
	echo generating input file...

	mkdir -p ./sample.d

	exec 11>&1
	exec 1>"${icsv}"

	echo timestamp,severity,status,method,uri,body
	echo 2025-09-10T02:25:20.012345Z,INFO,200,GET,/index.html,helo wrld
	echo 2025-09-10T02:25:21.012345Z,INFO,200,GET,/index.html,hello

	exec 1>&11
	exec 11>&-
}

gentmp(){
	echo generating temp file...

	which rs-csv2parquet | fgrep -q rs-csv2parquet || exec sh -c '
		echo rs-csv2parquet missing.
		echo you can install it using cargo install.
		exit 1
	'

	rs-csv2parquet \
		--input-csv-filename "${icsv}" \
		--output-parquet-filename "${mpq}" \
		--has-header \
		--same-column-count
}

test -f "${icsv}" || geninput
test -f "${mpq}" || gentmp

echo
echo converting the parquet...
./rs-parquet2parquet \
	--input-parquet-filename "${mpq}" \
	--output-parquet-filename "${opq}" \
	--fsync-after-write nop \
	--writer-version PARQUET_2_0 \
	--data-page-size-limit 1048576 \
	--data-page-row-count-limit 20000 \
	--write-batch-size 1024 \
	--max-row-group-size 1048576 \
	--created-by parquet-rs \
	--compression 'zstd(1)' \
	--dictionary-page-size-limit 1048576 \
	--statistics-enabled none

echo showing the parquet using rsql...
which rsql | fgrep -q rsql || exec sh -c '
	echo the rsql command not installed.
	echo you can install it using cargo.
	exit 1
'

rsql --url "parquet://${mpq}" -- "SELECT * FROM temp"
rsql --url "parquet://${opq}" -- "SELECT * FROM output"

ls -lSh ./sample.d/*.parquet
