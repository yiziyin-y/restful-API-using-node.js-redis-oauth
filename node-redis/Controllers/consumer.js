
/**
 * 定义消费参数
 */

exports.kafkaConsume = function () {
  const kafka = require('kafka-node');
  const client = new kafka.KafkaClient({ kafkaHost: 'localhost:9092' });
  var consumerOption = {
    groupId: "group-test",
    autoCommit: true
  };
  /**
   * 定义consumer，指定从分区0获取数据
   */
  const consumer = new kafka.Consumer(client, [
    { topic: 'topic-test-one', partition: 0 }
  ], consumerOption);
  /**
   * 监听消费数据
   */
  consumer.on('message', function (message) {
    var info = message.value;
    console.log("receive info from kafka:" + info, new Date());
  });
  consumer.on('error', function (message) {
    console.log('kafka连接错误,message:' + message);
  });
};