GMS是一个分布式的消息系统，支持集群和多副本，支持可靠的消息传递。

### 0. 特性
- 支持集群架构，容易做横向扩展.
- 支持多副本. 多副本之间保持强一致性.
- 使用etcd保存集群数据和集群控制器选举.
- 客户端和服务器之间使用二进制协议通信.
- 客户端发送消息时可以自由选择可靠性等级.
- 客户端发送消息时使用writev系统调用，相比write或send可以将多个数据包为一个做到减少延迟，还能减少write或send系统调用的开销。
- 创建topic时可以选择topic的可靠性级别，做到根据使用场景灵活调整.
- 使用内存映射文件，在读取和写入文件时获取更高性能。特别是在读取时，使用内存映射文件加上sendfile系统调用，可以节省一次从用户态到内核态的系统调用和数据拷贝开销。


### 1. 概念
- topic: 消息服务器中某个主题.
- 分区和副本：一个topic可以在物理上分为n个分区，所有分区功能相同。每个分区也可以用n个副本，n个副本中有一个是leader副本，其他是follower副本。follower副本向leader副本同步数据。
- log和segment: 分区在磁盘上表现为n个segment，每个segment是一个log文件和index文件。index文件将通过内存映射文件的方式载入，它提供了查找log文件的索引。log和index文件都是固定大小。只有写入消息的segment是以读写方式打开，其他的segment是以只读方式打开。
- ISR: In Sync Replica的缩写，指的是多个follower副本中和leader同步的副本列表。处于同步状态的要求是: 10秒中内获取过数据，并且从开始获取到返回响应的时间不超过200毫秒。发送消息时，producer可以根据ACK参数指定需要同步的副本数量，待足够数量的副本同步了此消息后，server才返回完成状态给producer。
- HW: 高水位，指的是producer发送消息给server后，server将这个消息写入到磁盘，offset增加。然后server等待所有follower都同步了这个offset以后，在内存中更新这个分区的HW，leader将HW写入到etcd。下次leader给follower返回数据时会带上当前分区的HW，follower收到后，也会写入到内存和etcd. 最后当consumer在leader副本上获取数据时，会将请求的offset和当前的HW对比，小于等于HW的数据才是可靠的。
- ACK: producer发送消息时，可以在参数中指定ACK，代表producer希望这个消息获得怎么样的确认。ACK为0是不确认，producer发送消息给server后不用等待server的响应。ACK为1是只用等待leader副本一个节点的响应即可。如果ACK是-1,  且leader维护的ISR列表的数量大于全局ISR或者分区设置的ISR，则以全局ISR或者分区ISR参数作为需要等待同步的数量；如果leader维护的ISR列表的数量小于全局ISR或者分区设置的ISR，则以当前ISR列表的数量为准。所以默认最少要等待leader自己写入到磁盘，客户端才能认为消息写入成功。

### 2. 架构
架构图

### 3. 工作过程

#### 3.1 初始化数据目录

节点启动的第一件事就是初始化数据目录，类似与`/data/mytopic/mytopic-0/`这样的3级目录。第一层是最上层的数据目录，第二层是topic的名称，第三层是分区的序号。一个分区目录下不会只有一个消息文件，因为那样会变得很大，载入内存和操纵它都会很麻烦。所以将数据组织为多个segment组成，每个segment分为一个log文件和一个index文件。log文件负责存储真正的消息， index文件负责记录消息的索引。目录结构如下图：

![2019-05-05_23-19](https://github.com/shelmesky/gms/raw/master/images/2019-05-06_14-58.png)

文件名代表了当前log或index文件保存的消息的起始offset，长度是64位。consumer读取消息时，会在内存中将排序好的文件名列表和consumer请求的offset做二分查找，找到对应的文件读取。

多个segment之中，offset最大的那个segment是active segment，它是以读写方式打开，其他segment以只读方式打开。在使用所有segment之前都使用内存映射方式打开文件，但是对于active segment，除了使用内存映射文件打开，还使用了`syscall.MAP_POPULATE`这个flag值传递给mmap系统调用，这个flag的作用是提前做预取，即第一将文件的所有内容从磁盘读取到物理内存页面作为cache，第二为进程建立好页表，这样不论后续的读取还是写入都会极大的提高速度。 (使用内存映射的方式写入，需要进程的页表中存在文件的映射，所以还是需要从磁盘读取文件内容到物理内存，这点不同与write系统调用，它是直接通过内核文件系统的write函数写入) 还有一种方式是通过syscall.Madvice()，传递`syscall.MADV_SEQUENTIAL|syscall.MADV_WILLNEED`flag给这个函数，告诉内核提前做预读，但是这样要看内核的预读策略，而且不会读取文件的所有内容，和普通的read调用效果差不多。

#### 3.2 注册当前节点：

在etcd的`/brokers/ids/xxx`注册lease机制的key，保存当前节点的信息：`{"ip_address":"127.0.0.1","port":50051,"rpc_port":50052,"node_id":"node-0","start_time":1557046846}`。lease节点不同于普通的key，注册成功后如果在指定时间不续约，则lease节点会被删除。这里需要启动一个goroutine定期续约。

#### 3.3 尝试注册成为controller
在`/controller-election/`目录下注册，并尝试成为controler。每个节点都会在该目录下成功创建key， 但只有当前节点观察到key的revision是最小的时候，才会认为自己成为了controller，否则会阻塞并等待。这样就带来了一个好处，这种形式类似与FIFO，最先注册的节点就会最早成为controller，如果观察到当前创建key的revision是最小的，则认为自己是controller.

具体的过程是这样的，节点首先在`/controller-election/key`下注册key,  key是当前session的lease id.  注册key采用的是etcd事务的方式，类似与if-then-else。
事务首先判断key的createRevision是否为0, 为0则创建key并写入value，value可以是任意值。

创建完毕key后，获取创建时的revision，然后将`revision-1`作为一个新的值，调用函数`etcdv3/concurrency/key.go`中的函数waitDeletes。
这个函数不停的循环Get是否有比当前revision更小的key, 如果有则watch这个key，等到DELETE事件发生返回nil，在下一个循环中继续Get是否有比当前revision更小的key，
如果没有则函数waitDeletes返回，当前节点成为controller. 否则会阻塞在watch函数上。代码如下：

```golang
// waitDeletes efficiently waits until all keys matching the prefix and no greater
// than the create revision.
func waitDeletes(ctx context.Context, client *v3.Client, pfx string, maxCreateRev int64) (*pb.ResponseHeader, error) {
	getOpts := append(v3.WithLastCreate(), v3.WithMaxCreateRev(maxCreateRev))
	for {
		resp, err := client.Get(ctx, pfx, getOpts...)
		if err != nil {
			return nil, err
		}
		if len(resp.Kvs) == 0 {
			return resp.Header, nil
		}
		lastKey := string(resp.Kvs[0].Key)
		if err = waitDelete(ctx, client, lastKey, resp.Header.Revision); err != nil {
			return nil, err
		}
	}
}

func waitDelete(ctx context.Context, client *v3.Client, key string, rev int64) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wr v3.WatchResponse
	wch := client.Watch(cctx, key, v3.WithRev(rev))
	for wr = range wch {
		for _, ev := range wr.Events {
			if ev.Type == mvccpb.DELETE {
				return nil
			}
		}
	}
	if err := wr.Err(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("lost watcher waiting for delete")
}
```

#### 3.4 成功注册为controller

如果当前节点成功注册成为controller，则启动3个goroutine:

```golang
    go HandleLeaderChange(leaderChan)

	go WatchBrokers()
	go WatchTopics()
```

第一个非常重要，用来监视当前节点是否是controller，当自己成为controller和失去controller身份时，都需要做对应的处理动作。

第二个监视节点离线或者重新上线，当节点离线时，需要查看节点当前负责哪些分区副本，这些分区副本当中如果有leader副本则需要重新为这些其他失去leader副本的follower寻找leader.
首先在

第三个监视在etcd中topic的创建，如果有新的topic创建则收到topic的分区数量和每个分区的副本数量，按照全排列的算法分配到现有所有节点上。
接着将需要创建的topic名称和分区的数量通过RPC发送给各个节点。每个结点在收到后调用topics包下的CreatePartition在磁盘上创建和初始化topic以及分区目录。
然后在etcd的`/topics-brokers/`目录下创建每个topic->partition->replica的对应关系，例如在`/topics-brokers/testtopic/partition-0/replica-0`路径下保存的内容为:

```json
{
    "node_index":0,
    "node_id":"node-0",
    "topic_name":"testtopic",
    "partition_index":0,
    "replica_index":0,
    "is_leader":true,
    "hw":0,
    "is_isr": true
}
```

上面的路径保存的是以topic为维度的分区副本信息，还需要一个以节点为维度的分区副本信息。
所以在etcd的`/brokers-topics/`目录下保存node->topic->partition->replica的对应关系，例如在`/brokers-topics/node-0/testtopic-partition0-replica0`路径下保存的内容为：

```json
{
    "node_index":0,
    "node_id":"node-0",
    "topic_name":"testtopic",
    "partition_index":0,
    "replica_index":0,
    "is_leader":true,
    "hw":0,
    "is_isr":false
}
```

在创建分区时，默认指定序号号为0的副本是leader副本。

当节点启动完毕，在etcd注册了所有监听路径，并创建完成topic后，etcd中的存储内容如下：

![2019-05-06_14-44](https://raw.githubusercontent.com/shelmesky/gms/master/images/2019-05-06_14-44.png)

#### 3.5 follower开始同步leader的数据

- 当确认topic和其分区创建完毕后，就可以开始向follower发送指令，让follower向它们的副本leader开始同步数据。
-  首先client发送startSync指令给server，收到命令后server首先查询`/topics-brokers/`目录下的topic信息，然后循环该目录下的所有分区和副本。
-  如果发现是leader副本，就调用RPC Client发送SYNC_MANAGER命令给leader副本所在的节点，告诉leader在其FollowerManager中提前加入follower的信息，方便在同步开始后leader维护follower的信息。
-  如果发现是follower副本，就调用RPC Client发送SYNC_SET命令给follower副本所在的节点，告诉follower接下来需要与哪个leader节点同步数据。
-  follower收到SYNC_SET指令后，开启Syncer goroutine向leader节点同步。

#### 3.6 producer写入数据

-  producer从API获取消息内容、topic名称、分区序号、ACK这个4个参数，前3个都很好理解，ACK是producer指定当前消息的需要获得怎样的确认。
-  producer连接到任何一个节点询问当前controller节点的IP地址，接着连接controller节点根据需要写入的topic名称和分区序号，获取分区的leader副本所在的节点IP地址。
-  producer连接到leader副本，将消息写入到leader节点。
-  leader节点判断消息中是否指定了分区序号，如果没有指定则随机写入到某个分区。如果指定了分区序号，再判断消息是否有key，有则将key做hash和分区的数量取模，选择所在分区。
- leader节点根据消息中的ACK参数，如果是1, 则只需要leader节点写入数据到磁盘既可返回响应给producer. 如果ACK是-1，则需要根据全局的ISR参数或者当前分区的ISR参数值，再结合当前leader维护的ISR列表长度，选择两者中的最小值，作为leader需要等待同步的follower副本数量。

#### 3.7 消息写入到磁盘

- leader或follower结点收到消息后，会将数据顺序的追加的active segment中，并在index中记录文件。
- 每个分区都会启动一个goroutine，定时的调用`msync`系统调用将内存映射的文件同步到持久化存储中。

#### 3.8 consumer读取数据

- consumer首先指定topic名称、分区序号、offset和消息数量参数，然后连接到controller节点，询问controller指定分区的leader副本的节点，得到响应后，再连接到leader副本节点。
- leader副本根据consumer请求的参数在本地磁盘中寻找相应的index和数据文件，找到segment文件后从指定的offset开始读取，读取到了指定的数量后就使用sendfile系统调用将消息发送给consumer.
