#!/bin/bash

# 定义测试次数
num_tests=100

# 创建 logs 目录以存储输出文件
mkdir -p logs

# 清理之前的日志文件
rm -f logs/out*
rm -f logs/time

# 输出测试开始时间
start_time=$(date)
echo "测试开始时间: $start_time" > logs/time

# 生成任务列表并并行执行测试
seq 1 $num_tests | parallel -j 10 'rm -f logs/out{} && ./test> logs/out{}'

# 合并所有输出文件
cat logs/out* > logs/combined_output.log

rm -f logs/out*

# 输出测试结束时间
end_time=$(date)
echo "测试结束时间: $end_time" >> logs/time