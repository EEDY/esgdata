#!/bin/bash

if [ $# -ne 1 ]
then
	echo "File name must be passed"
	echo "$0 file_name"
	exit 1
fi

file_path=$1
if [ ! -f "${file_path}" ]
then
    #echo "File: '${file_path}' does not exist"
    exit 0
fi

FROM_ENCODING=$(file -bi "${file_path}" |sed -e "s/.*[ ]charset=//")
FROM_ENCODING="${FROM_ENCODING^^}"
TO_ENCODING=UTF-8

#ASCII is a subset of UTF-8, no need to convert it
IGNORE_ENCODING="US-ASCII"

if [ "${FROM_ENCODING}" != "${TO_ENCODING}" ] && [ "${FROM_ENCODING}" != "${IGNORE_ENCODING}" ]
then
	# Convert
	mv "${file_path}" "${file_path}.bak"
	iconv -f ${FROM_ENCODING} -t ${TO_ENCODING} "${file_path}.bak" -o "${file_path}"
	exit_code=$?
	if [ ${exit_code} -eq 0 ]
	then
		rm "${file_path}.bak"
	else
		mv "${file_path}.bak" "${file_path}"
	fi
fi

exit ${exit_code}

