(此为早期设计，已过时)
# 介绍服务端消息流转机制
## 1 接收消息:
```
    在完成消息体解码后 获得一个ProducerToServerMessage 从中提取一个ProducerToServerMessageUnit
    
    根据从中提取ProducerToServerMessageUnit的信息生成一个MessageId
    
    然后构建获得一个 PersistentMessage 将其进行持久化，
    
    持久化完成后 生成 TransmissionMessage对象 用于描述这条消息，以及其中的内容。  
   
    在MessageMetaInfo的receivedMessages中存入这个MessageId，用于告诉producer自己已收到消息了
    
    把TransmissionMessage 放到对应的topic中(putMessage)
```
## 2 发送消息:
```
    Topic中存在多个MessageQueue 每个MessageQueue有一个专门的线程不断进行take()操作 (没有消息时会阻塞)
    拿出一条消息(TransmissionMessage)后要进行投递，对每一个ConsumerGroup调用nextConsumer()获取一个Consumer
    调用netty进行消息的发送 调用Consumer的 putMessage()进行发送。可能不是及时发送 取决于设置和实现。

```

## 3 消息确认:
```
    producer需要确认服务端收到了消息，服务端也需要确认consumer接收到了消息

```