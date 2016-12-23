#!/bin/bash
#turns all file* folders of spark result into one result csv 
echo "start"
num=0
bool=true
for i in $(ls); do
	if [[ $i =~ ^fil.* ]];
	then
		bool=false
		files[$num]=`cat $i/part-00000`
		num=$((num+1))
	fi
done
if $bool;
then
	echo no results found [directories starting with file]
	exit
fi
printf "%s\n" "${files[@]}" > "$(date +%s%3N)".csv
echo result file created
find -type d -name "fil*" -exec rm -rf {} +;
echo old result folders removed
echo end

