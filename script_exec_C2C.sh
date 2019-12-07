if [ -z "$1" ]
	then
	echo "smell(ser) files directory is missing is missing (#1)"
	exit 0
fi
if [ -z "$2" ]
	then
	echo "cluster(rsf) files directory is missing is missing is missing (#2)"
	exit 0
fi
if [ -z "$3" ]
	then
	echo "deps(rsf) files director is missing is missing is missing (#3)"
	exit 0
fi
if [ -z "$4" ]
	then
	echo "output file is missing is missing is missing (#4)"
	exit 0
fi

smell=$1
cluster=$2
deps=$3
out=$4

 echo "running C2C"
python /Users/pooyan/git/arcadepy/src/arc/simevolanalyzer.py --inputdir $cluster > $out
