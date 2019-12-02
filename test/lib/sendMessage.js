'use strict';
const kafka = require('kafka-node');
const uuid = require('uuid');

/**
 * kafka publisher
 *
 * @class Publisher
 */
class Publisher {
  /**
   *Creates an instance of Publisher.
   * @param {String} url   - broker url
   * @param {String} topic - kafka topic
   * @memberof Publisher
   */
  constructor(url, topic) {
    this.url = url;
    this.topic = topic;
    this.sendCount = 0;
  }

  /**
   * start functiuon
   *
   * @param {Number} time - message send frequency
   * @param {function} callback - callback
   * @memberof Publisher
   * @return {undefined}
   */
  start(time, callback) {
    const client = new kafka.KafkaClient({
      kafkaHost: this.url,
    });
    this.producer = new kafka.HighLevelProducer(client, {
      requireAcks: 1,
      partitionerType: 3,
    });
    this.client = client;

    client.on('connect', () => {
      this.producer.once('ready', () => {
        setInterval(() => {
          const message = new Array(10).join(Math.random().toString(36));
          this.sendMessage(message, callback);
        }, time);
      });

      this.producer.on('error', error => {
        console.log(error);
      });
    });
  }

  /**
   * 发送消息
   *
   * @param {String} message - kafka message
   * @param {Function} done  - callback
   * @memberof Publisher
   * @return {undefined}
   */
  sendMessage(message, done) {
    const _this = this;
    this.sendCount += 1;
    const topic = this.topic;
    this.producer.send(
      [{
        topic,
        messages: message,
        key: uuid(),
        attributes: 0,
      }],
      function(error, res) {
        if (error) {
          done(error);
        } else {
          res.count = _this.sendCount;
          done(null, res);
        }
      }
    );
  }
}

const publiser = new Publisher('172.19.3.186:9092', 'thing_paas_mqtts_upstream');

publiser.start(1, (error, res) => {
  const partion = Object.keys(res.thing_paas_mqtts_upstream)[0];
  const offset = res.thing_paas_mqtts_upstream[partion];
  if (error) console.log(error);
  else console.log(`[publisher01] 发送第${res.count}条数据, partion：${partion}, offset: ${offset}`);
});

publiser.start(1, (error, res) => {
  const partion = Object.keys(res.thing_paas_mqtts_upstream)[0];
  const offset = res.thing_paas_mqtts_upstream[partion];
  if (error) console.log(error);
  else console.log(`[publisher02] 发送第${res.count}条数据, partion：${partion}, offset: ${offset}`);
});

publiser.start(1, (error, res) => {
  const partion = Object.keys(res.thing_paas_mqtts_upstream)[0];
  const offset = res.thing_paas_mqtts_upstream[partion];
  if (error) console.log(error);
  else console.log(`[publisher03] 发送第${res.count}条数据, partion：${partion}, offset: ${offset}`);
});

publiser.start(1, (error, res) => {
  const partion = Object.keys(res.thing_paas_mqtts_upstream)[0];
  const offset = res.thing_paas_mqtts_upstream[partion];
  if (error) console.log(error);
  else console.log(`[publisher04] 发送第${res.count}条数据, partion：${partion}, offset: ${offset}`);
});

publiser.start(1, (error, res) => {
  const partion = Object.keys(res.thing_paas_mqtts_upstream)[0];
  const offset = res.thing_paas_mqtts_upstream[partion];
  if (error) console.log(error);
  else console.log(`[publisher05] 发送第${res.count}条数据, partion：${partion}, offset: ${offset}`);
});
