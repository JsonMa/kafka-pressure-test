'use strict';

const kafkaWorker = require('./lib/kafka');
const os = require('os');
module.exports = async app => {
  const kafkaWorkerIns = new kafkaWorker(app, {
    initProducer: true,
    async handler() {
      await wait(1000); // mock 异步调用
    },
  });

  /**
   * 等待时间
   *
   * @param {Number} time - 等待时间
   * @return {Promise}  - promise
   */
  function wait(time) {
    return new Promise(resolve => {
      resolve(time);
    }, time);
  }

  setInterval(() => {
    kafkaWorkerIns.logger.debug(`free memery: ${os.freemem()} bytes`);
    kafkaWorkerIns.logger.debug(`total memery: ${os.totalmem()} bytes`);
  }, 2000);

  Reflect.defineProperty(app, 'kafka', {
    value: kafkaWorkerIns,
  });

  app.beforeStart(async () => {
    try {
      await kafkaWorkerIns.init();
    } catch (error) {
      kafkaWorkerIns.logger.error(error);
    }
  });

  app.beforeClose(async () => {
    await kafkaWorkerIns.closeConsumerGroup();
  });

  process.on('uncaughtException', err => {
    kafkaWorkerIns.logger.error(err);
  });
};
