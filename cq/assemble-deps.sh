mvn -pl geomesa-assemble dependency:tree | grep compile | sed 's/:compile$//' | sed 's/^\[INFO\][\\ |+-]*//' | sed 's/\(.*\):/\1\t/' | sort | uniq > assemble-deps
