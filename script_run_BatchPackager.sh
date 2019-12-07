if [ -z "$1" ]
	then
	echo "Application's name is missing (#1)"
	exit 0
fi
./script_generic.sh $1 BatchPackager BCE