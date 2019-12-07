if [ -z "$1" ]
	then
	echo "source directory is missing (#1)"
	exit 0
fi
if [ -z "$2" ]
	then
	echo "file (partial) name missing is missing (#2)"
	exit 0
fi


sourceFolder="$1"
fileName="$2"

dstFolder="$3"

for i in $(find "$sourceFolder" -name "*$fileName*" -type f); do
rm $i
done
for i in $(find "$sourceFolder" -name "*$fileName*" -type d); do
rm -r $i
done
