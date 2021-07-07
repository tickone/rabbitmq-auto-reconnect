const amqp = require('amqplib/callback_api');
const debug = require('debug')('rabbitmq');

class RabbitMQ {
  constructor(host) {
    this.startTime = new Date().getTime();
    this.host = host;
    this.connection = null;
    this.channel = {};
  }

  async connect() {
    return new Promise((resolve, reject) => {
      amqp.connect(this.host, 'heartbeat=30', (error, connection) => {
        if (error) {
          reject(error);
        } else {
          this.channel = {};
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

  async createChannel(type, queueName) {
    if (!this.connection) {
      await this.connect();
    }

    if (this.channel?.[type]?.[queueName]) {
      return this.channel?.[type]?.[queueName];
    }

    return new Promise((resolve, reject) => {
      this.connection.createChannel((error, channel) => {
        if (error) {
          reject(error);
        } else {
          this.channel[type] = this.channel[type] ?? {};
          this.channel[type][queueName] = channel;
          resolve(channel);
          debug('[AMQP] rabbitmq channel created');
        }
      });
    });
  }

  async recoverChannel(type, queueName) {
    return new Promise((resolve) => {
      // this.channel.nackAll(false)
      this.channel?.[type]?.[queueName].recover(resolve);
    });
  }

  /**
   * @returns {Promise<import('amqplib/callback_api').Channel>}
   */
  async getChannel(type, queueName) {
    const channel = await this.createChannel(type, queueName);
    await this.recoverChannel(type, queueName);

    return channel;
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
      const channel = await this.getChannel('send', queueName);

      channel.assertQueue(queueName, queueOptions);
      channel.sendToQueue(queueName, Buffer.from(JSON.stringify(message)), sendOptions);
      debug(`[AMQP/${queueName}] sended`, { message });
    } catch (error) {
      debug(`[AMQP/${queueName}] sendToQueue error`, { error });
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
    const channel = await this.getChannel('listen', queueName);
    channel.assertQueue(queueName, queueOptions);
    channel.prefetch(prefetchCount);

    channel.consume(queueName, async (msg) => {
      try {
        /** @type {Message} */
        const message = JSON.parse(msg.content.toString());
        debug(`[AMQP/${queueName}] onMessage`, { message });

        await callback(message);
        debug(`[AMQP/${queueName}] ack (${message.type})`);
        channel.ack(msg);
      } catch (error) {
        debug(`[AMQP/${queueName}] nack`, { error });
        channel.nack(msg, false, false);
      }
    }, consumeOptions);
    debug(`[AMQP/${queueName}] start listen`);
  }
}

module.exports = RabbitMQ;
