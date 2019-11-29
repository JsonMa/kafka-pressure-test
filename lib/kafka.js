'use strict';

const Kafka = require('kafka-node');
const uuid = require('uuid');
const assert = require('assert');
const _ = require('underscore');
const async = require('async')
/**
 * Kafka Worker类
 *
 * @class KafkaWorker
 */
class KafkaWorker {
  /**
   * Creates an instance of KafkaWorker.
   *
   * @param {object} application     - Egg agent/app application
   * @param {object} options         - Kafka Client 配置文件
   * @memberof KafkaWorker
   */
  constructor(application, options = {}) {
    this.application = application;
    this.kafkaConfig = application.config.kafka;
    this.initProducer = true;
    this.initConsumer = true;
    this.consumers = []; // kafka consumers
    this.producers = {
      totalCount: 0,
      availables: {}, // 可用的producers
    }; // kafka producers pool
    Object.assign(this, options);
  }

  /**
   * 消息logger
   *
   * @readonly
   * @memberof KafkaWorker
   */
  get logger() {
    const handler = {
      get(target, attrName) {
        if (attrName in target) {
          return target[attrName].bind(target, '[Kafka]');
        }
      },
    };
    return new Proxy(this.application.logger, handler);
  }

  /**
   * 获取config中的topic信息
   *
   * @param {Bollean} onlyTopics    - 是否只需要topcs数组
   * @memberof KafkaWorker
   * @return {Array} topcs
   */
  topics(onlyTopics = false) {
    const {
      kafkaTopics,
    } = this.kafkaConfig;
    assert(kafkaTopics && typeof kafkaTopics === 'object', '[topics] - topics类型错误');
    assert(typeof onlyTopics === 'boolean', '[topics] - onlyTopics类型错误');

    return Object.keys(kafkaTopics).map(item => {
      return onlyTopics ? kafkaTopics[item].topic : kafkaTopics[item];
    });
  }

  /**
   * 验证topics信息
   *
   * @memberof KafkaWorker
   * @return {undefined}
   */
  async validateTopics() {
    const {
      kafkaTopics,
    } = this.kafkaConfig;
    const incorrectTopics = [];

    // 获取topics信息
    const [broker, zookeeper] = await this.topicList();
    this.logger.info(`[validateTopics] - kafka broker number: ${Object.keys(broker).length}`);
    const {
      metadata,
    } = zookeeper;
    const existingTopics = Object.keys(metadata); // 已存在的topics

    // 过滤topic出不存在的topic及配置不一致的topic
    _.values(kafkaTopics).forEach(topicPara => {
      const isExisted = existingTopics.includes(topicPara.topic); // 指定topic是否存在
      let isParaCorrect = true;
      let isRpCorrect = true;
      if (isExisted) {
        isParaCorrect = topicPara.partitions === Object.keys(metadata[topicPara.topic]).length;
      } // 分区数是否正确
      if (isExisted) {
        isRpCorrect = topicPara.replicationFactor === metadata[topicPara.topic][0].isr.length;
      } // 分区数是否正确
      if (!isExisted || !isParaCorrect || !isRpCorrect) incorrectTopics.push(topicPara);
    });

    incorrectTopics.forEach(item => {
      this.logger.error(
        `[validateTopics] - topic error, topic:${item.topic}, partitions: ${item.partitions}, replicationFactor: ${item.replicationFactor}`
      );
    });
  }

  /**
   * ConsumerGroup实例化
   *
   * @param {object} options       - 实例化配置
   * @param {array|string} topics  -consumerGroup监听的topic
   * @memberof KafkaWorker
   * @return {object} consumerGroup对象
   */
  async createConsumerGroup(options, topics = 'kafka_test_topic1') {
    assert(topics && Array.isArray(topics), '[consumerGroup] - consumer group topics需为Array类型');
    if (options) {
      assert(typeof options === 'object', '[consumerGroup] - consumer group options需为Object类型');
    }

    const consumer = await new Kafka.ConsumerGroup(options, topics);
    return new Promise((resolve, reject) => {
      consumer.once('connect', () => {
        consumer.cid = uuid();
        consumer.cache = {}; // 消息队列，maxLength = 10000
        this.logger.info(`[createConsumerGroup] - cosumer ready, id: ${consumer.cid}`);
        resolve(consumer);
      });

      consumer.once('error', err => {
        reject(err);
      });
    });
  }

  /**
   *Consumer绑定消息处理函数
   *
   * @param {function} handler  - 消息处理函数
   * @return {undefined}
   */
  consumerHandler(handler) {
    this.consumers.forEach(consumer => {
      consumer.asyncQueue = async.queue(async(task) =>{
        try {
          await handler(task)
          this.logger.info(`resolved message, cache: ${cacheLength}, partition: ${message.partition}, offset: ${message.offset}`);
        } catch (err) {
          this.logger.error(`[consumerHandler] [${message.traceId}] - consumer handler error,`, err, '\nMessage:\n', message);
        }
      });

      consumer.asyncQueue.drain(()=>{
        consumer.resume() // 继续消费
      })

      consumer.on('message', message => {
        if(!consumer.paused) consumer.pause(); // 暂停消费
        message.traceId = uuid.v1();
        consumer.asyncQueue.push(message, () => {
          this.logger.info(`received message, cache: ${cacheLength}, partition: ${message.partition}, offset: ${message.offset}`);
        })
      });

      consumer.on('error', err => {
        this.logger.error('[consumerHandler] - consumer error,\nError:', err);
      });
    })
  }

  /**
   * Admin-列出kafka所有groups
   *
   * @return {object} groups promsie
   * @memberof KafkaWorker
   */
  async listGroups() {
    const {
      kafkaHost,
    } = this.kafkaConfig;
    const client = new Kafka.KafkaClient({
      kafkaHost,
    });
    const admin = new Kafka.Admin(client); // client must be KafkaClient

    return new Promise((resolve, reject) => {
      admin.listGroups((err, res) => {
        if (err) reject(err);
        client.close(() => {
          this.logger.info('[listGroups] - client closed');
        });
        resolve(res);
      });
    });
  }

  /**
   * 获取kafka consumerGroup信息
   *
   * @param {array} consumerGroups  - group名
   * @return {object} groupInfo promise
   * @memberof KafkaWorker
   */
  async describeGroups(consumerGroups) {
    const {
      kafkaHost,
    } = this.kafkaConfig;
    const client = new Kafka.KafkaClient({
      kafkaHost,
    });
    const admin = new Kafka.Admin(client);

    return new Promise((resolve, reject) => {
      admin.describeGroups(consumerGroups, (err, res) => {
        if (err) reject(err);
        client.close(() => {
          this.logger.info('[describeGroups] - client closed');
          resolve(res);
        });
      });
    });
  }

  /**
   * 验证consumer group数量及id
   *
   * @param {Array} consumerGroups - consumer group name
   * @return {undefined}
   */
  async validateGroups(consumerGroups) {
    const {
      consumerOptions,
    } = this.kafkaConfig;

    const resp = await this.describeGroups(consumerGroups);
    const groupLength = resp[consumerGroups[0]].members.length;
    const groupId = resp[consumerGroups[0]].groupId;

    assert(
      groupId === consumerOptions.groupId,
      `[validateGroups] - groupId错误，定义：${groupId}, 实际：${consumerOptions.groupId}`
    );
    this.logger.info(
      `[validateGroups] - consumerGroupId: ${groupId}, consumerGroupLength: ${groupLength}, newAdded: ${this.consumers.length}`
    );
  }

  /**
   * Producer实例化
   *
   * @return {object} Producer 实例
   * @memberof KafkaWorker
   */
  producer() {
    const {
      kafkaHost,
    } = this.kafkaConfig;
    const Producer = Kafka.HighLevelProducer;
    const client = new Kafka.KafkaClient({
      kafkaHost,
    });

    client.refreshMetadata(this.topics(true), err => {
      if (err) throw err;
    });
    const producer = new Producer(client, {
      partitionerType: 3, // 指定分区类型 keyed
    });
    producer.producerId = uuid();
    this.logger.info(`[producer] - producer creating, id: ${producer.producerId}`);
    return producer;
  }
  /**
   * producer池初始化
   *
   * @memberof KafkaWorker
   * @return {undefined}
   */
  async producerGroupInit() {
    const {
      application,
    } = this;
    const {
      count,
    } = application.config.kafka.producerOptions;

    const createGroup = async () => {
      const producer = this.producer();

      // error event propagates from internal client
      producer.on('error', err => {
        this.logger.error(`[producerGroupInit] - producer error, ${err}`);
      });

      return new Promise(resolve => {
        producer.once('ready', () => {
          this.logger.info(`[producerGroupInit] - producer ready, Id: ${producer.producerId}`);
          this.producers.available[producer.producerId] = producer;
          this.producers.totalCount += 1;
          resolve();
        });
      });
    };

    // 启动指定数量的producer
    for (let i = 0; i < count; i++) {
      await createGroup();
    }
  }

  /**
   * Producer发送消息
   *
   * @param {string} payloads - 消息
   * @param {string} traceId  - trace id
   * @memberof KafkaWorker
   * @return {object} promise对象
   */
  sendMsg(payloads, traceId) {
    const {
      producers,
    } = this;
    assert(producers.totalCount !== 0, '[sendMsg] - 没有可用的producer');

    const keysArray = Object.keys(producers.available); // 获取producer keys
    const key = keysArray[parseInt(Math.random() * keysArray.length)]; // Random获取key
    const producer = producers.available[key]; // 获取producer

    return new Promise((resolve, reject) => {
      producer.send(payloads, (err, data) => {
        if (err) reject(err);
        payloads.forEach(payload => {
          this.logger.info(
            `[sendMsg] [${traceId}] - topic: ${payload.topic}, key: ${payload.key}, partition: ${payload.partition}, result: success`
          );
        });
        resolve(data);
      });
    });
  }

  /**
   * 获取topics列表
   *
   * @memberof KafkaWorker
   * @return {array} topics
   */
  async topicList() {
    const {
      application,
    } = this;
    const client = new Kafka.KafkaClient({
      kafkaHost: application.config.kafka.kafkaHost,
    });

    return new Promise((resolve, reject) => {
      client.once('connect', () => {
        client.loadMetadataForTopics([], (error, results) => {
          if (error) reject(error);
          client.close(() => {
            this.logger.info('[topicList] - client closed');
            resolve(results);
          });
        });
      });
    });
  }

  /**
   * Adamin方式创建Topic
   *
   * @param {array} topics - topics
   * @memberof KafkaWorker
   * @return {object} topic promise
   */
  adminCreateTopics(topics) {
    assert(Array.isArray(topics), '[adminCreateTopics] - topics需为数组');

    let rawTopics = topics.map(item => {
      return item.topic;
    });
    const {
      kafkaHost,
    } = this.kafkaConfig;
    const client = new Kafka.KafkaClient({
      kafkaHost,
    });
    const admin = new Kafka.Admin(client); // client must be KafkaClient

    return new Promise((resolve, reject) => {
      admin.createTopics(topics, (err, res) => {
        if (err) reject(err);
        if (res) rawTopics = _.without(rawTopics, res); // delete failed topics
        this.logger.info(`[adminCreateTopics] - topics: ${rawTopics.toString()}, result: success`);
        resolve(rawTopics);
      });
    });
  }

  /**
   * 关闭consumergroup
   *
   * @memberof KafkaWorker
   * @return {undefined}
   */
  async closeConsumerGroup() {
    const {
      consumers,
    } = this;

    if (consumers.length) {
      consumers.forEach(consumer => {
        this.logger.info(`[closeConsumerGroup] - consumer: ${consumer.cid} closing`);
        consumer.close(() => {
          this.logger.info(`[closeConsumerGroup] - consumer: ${consumer.cid} closed`);
        });
      });
    }
  }

  // healthCheack(consumers, time) {
  //   setInterval(() => {
  //     consumers.queue.length
  //   }, time)
  // }

  /**
   * 初始化
   *
   * @memberof KafkaWorker
   * @return {undefined}
   */
  async init() {
    const {
      consumerOptions,
      kafkaHost,
      consumerCount,
      kafkaTopics,
    } = this.kafkaConfig;

    await this.validateTopics();

    if (this.initConsumer) {
      assert(this.handler && typeof this.handler === 'function', '[init] - handler未指定或类型错误');
      // 创建consumer group并指定message handler
      for (let i = 0; i < consumerCount; i++) {
        const consumerIns = await this.createConsumerGroup(
          Object.assign(consumerOptions, {
            kafkaHost,
          }),
          [kafkaTopics.thingPaaSMqttUpstream.topic]
        );
        this.consumers.push(consumerIns);
      }
      await this.validateGroups([consumerOptions.groupId]);
      this.consumerHandler(this.handler);
    }

    if (this.initProducer) await this.producerGroupInit();
    this.logger.info('[init] - kafka worker init success');
  }
}

module.exports = KafkaWorker;
