mvn dependency:tree | grep 'test$' | grep jar | sed 's/^\[INFO\] //' | sed 's/:test$//' | sed 's/^\+\- \(.*\)/\1\tDIRECT/' | sed 's/^[| ][\\ |+-]*\(.*\)/\1\tTRANSITIVE/' | sed 's/\(.*\):/\1\t/' | sort | uniq > test-deps
while read line; do
  dep=$(echo $line | awk '{ print $1 }')
  all=$(grep "$dep" all-deps | wc -l)
  prov=$(grep "$dep" provided-deps | wc -l)
  if [ $all -eq 0 -a $prov -eq 0 ]; then
    echo "$line" >> tmp
  fi
done < test-deps
mv tmp test-deps
