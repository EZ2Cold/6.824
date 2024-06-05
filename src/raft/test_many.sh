#!/bin/bash  

rm out
for((i=1;i<=100;i++));  
do   
    go test -run 3A >> out
done