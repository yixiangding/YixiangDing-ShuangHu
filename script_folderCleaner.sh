if [ -z "$1" ]
	then
	echo "Folder Name is missing is missing (#1)"
	exit 0
fi
if [ -z "$2" ]
	then
	echo "source Folder is missing is missing is missing (#2)"
	exit 0
fi


FolderName="$1"
sourceFolder="$2"

for i in $(find "$sourceFolder" -name "*$FolderName*" -type d); do
rm -r $i 
done

