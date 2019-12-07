for i in $(find .) ;do
string=$i
key=ACDC
replace=ARC
mv $i ${string//$key/$replace}
done
