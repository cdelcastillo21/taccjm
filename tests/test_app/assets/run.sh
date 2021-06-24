#### BEGIN SCRIPT LOGIC
set -x
ls -lat ${data} > test1.txt
sleep 60
ls -lat ${data} > test2.txt
${command} ${command_opts} > test.log 2>&1
set +x
