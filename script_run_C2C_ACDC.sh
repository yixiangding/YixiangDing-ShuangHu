# by running this script, the C2C analysis between the results will be done
if [ -z "$1" ]
	then
	echo "Application's name is missing (#1)"
	exit 0
fi
./script_generic.sh $1 C2C AWSD_SEA