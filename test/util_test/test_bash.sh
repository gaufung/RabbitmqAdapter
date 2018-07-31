# !/bin/bash
echo "This is test bash for asynchronous process"
echo "This message will print from stderr" >&2
if [ $2 > 0 ];  then sleep $2; fi
exit $1