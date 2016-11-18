#!/bin/sh

#restart single and unique process

psid=0

checkpid() {
  echo "checkpid $1"
  psid=0
  aaps=`ps -A | grep -w $1`
  
  
  if [ -n "$aaps" ]; then
  psid=`echo $aaps | awk '{print $1}'`
  else
  psid=0
  fi
  
  echo "checkpided: $psid"
}

dt=`date +%Y%m%d-%H%M%S`;
destFolder=backup/$dt/;
mkdir -p $destFolder;
tar cvfz $destFolder/bin.tar.gz bin/;
mv swap/* bin/ -f

checkpid "python"
echo "kill $psid ..."
kill $psid
sleep 1s
mv logs/nohup.out logs/nohup.out.$dt
cd bin
nohup python server.py > ../logs/nohup.out &
checkpid "python"
echo "new pid: $psid ..."
