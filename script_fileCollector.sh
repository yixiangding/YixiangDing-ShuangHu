if [ -z "$1" ]
	then
	echo "extension is missing is missing (#1)"
	exit 0
fi
if [ -z "$2" ]
	then
	echo "source Folder is missing is missing is missing (#2)"
	exit 0
fi
if [ -z "$3" ]
	then
	echo "dest Folder is missing is missing is missing (#3)"
	exit 0
fi



fileExtension="$1"
sourceFolder="$2"
dstFolder="$3"

for i in $(find "$sourceFolder" -name "*$fileExtension" -type f); do
stringZ=$i
match=*$sourceFolder
repl=
temp1=${stringZ/$match/$repl}  
match="/"
newFileName=${temp1//$match/$repl}  
cp $i $dstFolder/$newFileName
done
echo "number of $fileExtension files:"
ls -1 $dstFolder | wc -l