/* eslint valid-jsdoc: "off" */

'use strict';

/**
 * @param {Egg.EggAppInfo} appInfo app info
 */
module.exports = appInfo => {
  /**
   * built-in config
   * @type {Egg.EggAppConfig}
   **/
  const config = (exports = {});

  // use for cookie sign key, should change to your own and keep security
  config.keys = appInfo.name + '_1574921647098_6098';

  // add your middleware config here
  config.middleware = [];

  config.kafka = {
    kafkaHost: '172.19.3.186:9092',
    consumerCount: 1,
    consumerOptions: {
      groupId: 'pressTestConsumerGroup',
      sessionTimeout: 15000,
      protocol: ['roundrobin'],
      fromOffset: 'latest',
      commitOffsetsOnFirstJoin: true,
      outOfRangeOffset: 'earliest',
      migrateHLC: false,
      migrateRolling: true,
      encoding: 'buffer',
    },
    kafkaTopics: {
      // saasDeviceUpstream: {
      //   topic: 'saas_device_upstream',
      //   partitions: 10,
      //   replicationFactor: 1,
      // },
      thingPaasMqttsUpstream: {
        topic: 'thing_paas_mqtts_upstream',
        partitions: 10,
        replicationFactor: 1,
      },
    },
    producerOptions: {
      producerGroupId: 'follower-producer-group',
      count: 1,
    },
  };

  // add your user config here
  const userConfig = {
    // myAppName: 'egg',
  };

  return {
    ...config,
    ...userConfig,
  };
};
