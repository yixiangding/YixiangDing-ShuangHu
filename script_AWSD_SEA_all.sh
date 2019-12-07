# ACDC WITH SMELL DETECTION + SMELL EVOLo

if [ -z "$1" ]
	then
	echo "Binary folder  is missing (#1)"
	exit 0
fi

if [ -z "$2" ]
	then
	echo "Application name  is missing (#2)"
	exit 0
fi

sourceFolder="$1"
applicationName="$2"

for i in $(ls  "$sourceFolder/$applicationName"); do
#$i is just a number 
./script_AWSD_SEA.sh $applicationName $i
done
exit 0

echo "number of $fileExtension files:"
ls -1 $dstFolder | wc -l