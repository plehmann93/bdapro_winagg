#!/bin/bash
#turns all file* folders of spark result into one result csv 

num=0
#name=da
bool=true
for i in $(ls); do
	if [[ $i =~ ^fil.* ]];
	then
		bool=false
		files[$num]=`cat $i/part-00000`
		num=$((num+1))
		declare name=$i
		#echo $i
#name=${name
		
	fi
done
if $bool;
then
#	echo no results found [directories starting with file]
	exit
fi

#printf "%s\n" "${files[@]}" > "$(date +%s%3N)".csv
printf "%s\n" "${files[@]}" > "${name##*_}".csv

find -type d -name "fil*" -exec rm -rf {} +;

