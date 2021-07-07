const amqp = require('amqplib/callback_api');
const debug = require('debug')('rabbitmq');

class RabbitMQ {
  constructor(host) {
    // record start time, skip message before start
    this.startTime = new Date().getTime();
    this.host = host;
    /** @type {import('amqplib/callback_api').Connection} */
    this.connection = null;
    /** @type {import('amqplib/callback_api').Channel} */
    this.channel = null;
  }

  async connect() {
    return new Promise((resolve, reject) => {
      amqp.connect(this.host, {
        timeout: 5000,
      }, (error, connection) => {
        if (error) {
          reject(error);
        } else {
          this.connection = connection;
          this.connection.on('error', (connectionError) => {
            debug('[AMQP] error', { error: connectionError });
          });
          this.connection.on('close', () => {
            debug('[AMQP] close');
            setTimeout(this.connect.bind(this), 1000);
          });
          debug('[AMQP] rabbitmq connected');
          resolve(connection);
        }
      });
    });
  }

  async createChannel() {
    if (!this.connection) {
      await this.connect();
    }
    return new Promise((resolve, reject) => {
      this.connection.createChannel((error, channel) => {
        if (error) {
          reject(error);
        } else {
          this.channel = channel;
          resolve(channel);
          debug('[AMQP] rabbitmq channel created');
        }
      });
    });
  }

  async recoverChannel() {
    return new Promise((resolve) => {
      // this.channel.nackAll(false)
      this.channel.recover(resolve);
    });
  }

  /**
   * @returns {Promise<import('amqplib/callback_api').Channel>}
   */
  async getChannel() {
    if (!this.channel) {
      await this.createChannel();
      await this.recoverChannel();
    }
    return this.channel;
  }

  async purgeQueue(queueName) {
    const channel = await this.getChannel();
    return new Promise((resolve, reject) => {
      channel.purgeQueue(queueName, (error, ok) => {
        if (error) {
          debug(`[AMQP/${queueName}] purgeQueue error`, { error });
          reject(error);
          return;
        }
        if (ok) {
          debug(`[AMQP/${queueName}] purgeQueue ok`, { ok });
          resolve();
        }
      });
    });
  }

  /**
   * @param {string} queueName
   * @param {Message} message
   * @param {import('amqplib/callback_api').Options.AssertQueue} queueOptions
   * @param {import('amqplib/callback_api').Options.Publish} sendOptions
   */
  async sendToQueue(
    queueName,
    message,
    queueOptions = { durable: true },
    sendOptions = { persistent: true },
  ) {
    try {
      const channel = await this.getChannel();

      const payload = {
        ...message,
        time: new Date().getTime(),
      };
      channel.assertQueue(queueName, queueOptions);
      channel.sendToQueue(queueName, Buffer.from(JSON.stringify(payload)), sendOptions);
      debug(`[AMQP/${queueName}/${message.type}] sended`, { message });
    } catch (error) {
      debug(`[AMQP/${queueName}/${message.type}] sendToQueue error`, { error });
    }
  }

  /**
   * @param {string} queueName
   * @param {(message: Message) => Promise<any>} callback
   * @param {number} prefetchCount
   * @param {import('amqplib/callback_api').Options.AssertQueue} queueOptions
   */
  async listenToQueue(
    queueName,
    callback,
    prefetchCount = 1,
    queueOptions = { durable: true },
    consumeOptions = { noAck: false },
  ) {
    const channel = await this.getChannel();
    channel.assertQueue(queueName, queueOptions);
    channel.prefetch(prefetchCount);

    await this.purgeQueue(queueName);

    channel.consume(queueName, async (msg) => {
      try {
        /** @type {Message} */
        const message = JSON.parse(msg.content.toString());
        debug(`[AMQP/${queueName}/${message.type}] onMessage`, { message });

        if (message.time < this.startTime) {
          // skip message before start time
          debug(`[AMQP/${queueName}/${message.type}] nack (skip old message)`);
          channel.nack(msg, false, false);
          return;
        }

        await callback(message);
        debug(`[AMQP/${queueName}/${message.type}] ack (${message.type})`);
        channel.ack(msg);
      } catch (error) {
        debug(`[AMQP/${queueName}] nack`, { error });
        channel.nack(msg, false, false);
      }
    }, consumeOptions);
    debug(`[AMQP/${queueName}] start listen`);
  }
}

module.exports = new RabbitMQ();
