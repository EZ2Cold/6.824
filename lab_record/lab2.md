# Lab 2: Key/Value Server
本实验实现一个Key/Value Server，client与Server通过RPC通信。

client可以进行如下操作：
* Get：获取一个Key的值
* Put：设置一个Key的值
* Append：追加一个Key的值，返回值为旧值

在不可靠的网络上可能发生消息丢失（如丢失RPC请求或响应），此时，client会重新发出请求直到成功。本实验要求重复请求不会被执行两次，具体要求如下：
* Get：每次Get请求都获取最新值，无论是不是重复请求
* Put：如果为重复请求，不更改Value
* Append：如果为重复请求，返回执行第一个请求获得的返回值

为了过滤重复请求，每个请求都携带一个`ReqId`，取值为{0, 1}。发出新请求时翻转当前值，发出重复请求时值保持不变。

Put和Append处理新请求时需要记录新的`ReqId`到一个`history`变量中。Put无返回值，只需记录`ReqId`，而Append还需要额外记录请求返回值。收到一个请求，如果无`history`或者和`history`中的`ReqId`不一致，则为新请求，否则为重复请求，Append需要返回`history`中保存的返回值。

每次执行Get函数都可以选择更新`history`中的`ReqId`，或者直接删除`history`。为了通过`memory use get`测试，应该采用后者。


问题：
```
go test -race // 可以正常进行测试且通过
go test // 显示info: linearizability check timed out, assuming history is ok，且进程被killed
```