#!/bin/bash
input="/home/ubuntu/spark-1.2.0.2.2.0.0-82-bin-2.6.0.2.2.0.0-2041/save.csv"
# Set "," as the field separator using $IFS
# and read line by line using while read combo 
#shopt -s extglob
counter=0
while IFS=',' read -r f1 f2 f3 f4 f5 f6 f7 f8 f9
do
  #echo ${f3}
  f3=$( echo "${f3}"| sed -e 's/^ *//' -e 's/ *$//' );
  f3="${f3##*( )}"
  f3="${f3%%*( )}"
  #echo ${f3}
  illcommand="./bin/spark-submit --master local[4] my_code8.py ${f3}"
  echo $illcommand 
  #eval $illcommand 
  #counter=$((counter+1))
  #echo $counter
  #if [ $counter -eq 5]
  #then
  #    break
  #fi
  #f3_WHITESPACE="$(echo -e "${f3}" | tr -d '[[:space:]]')"
  #echo -e "f3_WHITESPACE='${f3_WHITESPACE}'"
  #echo "$f3_WHITESPACE"
done < "$input"
#echo $illcommand

#shopt -u extglob
