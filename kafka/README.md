## bootstrap.servers

指定了要连接到的broker,可以用,分隔写入多个，连接broker cluster时不需要写入全部的broker地址，broker本身会同步其他broker的信息，但最少放两个以免写一个对应的broker挂了。

在实际使用中，可能会出现org.apache.kafka.common.errors.TimeoutException: Failed to update metadata after 60000 ms.这个错误，请求本身是发送成功的，检查zk topic也建立了，但就是获取不到metadata。

这是因为填入的broker服务器和kafka在zk上注册的host不一致导致的(在zk上可以通过get /brokers/ids/{id}查看)，可以通过修改kafka的server.properties的advertised.host.name和advertised.port改正。

但是要注意hosts文件会影响，比如配置了自己ip到一个域名上导致失败，注意代码在运行时log打出的实际的值。
