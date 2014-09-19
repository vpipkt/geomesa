mvn dependency:tree | grep "provided" | grep jar | sed 's/^\[INFO\] //' | sed 's/:provided$//' | sed 's/^\+\- \(.*\)/\1\tDIRECT/' | sed 's/^[| ][\\ |+-]*\(.*\)/\1\tTRANSITIVE/' | sed 's/\(.*\):/\1\t/' | sort | uniq > provided-deps
while read line; do
  dep=$(echo $line | awk '{ print $1 }')
  all=$(grep "$dep" all-deps | wc -l)
  if [ $all -eq 0 ]; then
    echo "$line" >> tmp
  fi
done < provided-deps
mv tmp provided-deps
