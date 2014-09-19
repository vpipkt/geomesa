grep -v geomesa shaded-deps > all-deps; grep -v geomesa assemble-deps >> all-deps; sort all-deps | uniq > tmp; mv tmp all-deps
while read line; do
  shaded=$(grep "$line" shaded-deps | wc -l)
  assemble=$(grep "$line" assemble-deps | wc -l)
  if [ $shaded -gt 0 -a $assemble -gt 0 ]; then
    echo "${line}	BOTH" >> tmp
  elif [ $shaded -gt 0 ]; then
    echo "${line}	SHADED" >> tmp
  else
    echo "${line}	ASSEMBLE" >> tmp
  fi
done < all-deps
mv tmp all-deps
