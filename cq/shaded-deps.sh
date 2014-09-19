mvn clean package -Dmaven.test.skip=true | grep "Including" | sed 's/^\[INFO\] Including //' | sed 's/ in the shaded jar\.$//' | sed 's/\(.*\):/\1\t/' | sort | uniq > shaded-deps
