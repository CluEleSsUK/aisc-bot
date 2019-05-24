import defaultKafka from 'kafka-node'

export default class KafkaAdapter {
  constructor (options, kafka = defaultKafka) {
    this.kafka = kafka
    this.client = new kafka.KafkaClient(options)
  }

  /**
   * Returns a Producer instance.
   *
   * @see {@link https://github.com/SOHU-Co/kafka-node#producer}
   */
  getProducer (options) {
    return new this.kafka.Producer(this.client, options)
  }

  /**
   * Returns a Consumer instance.
   *
   * @see {@link https://github.com/SOHU-Co/kafka-node#consumer}
   */
  getConsumer (options) {
    if (!this.consumer) {
      this.consumer = this.createConsumer(options)
    }

    return this.consumer
  }

  createConsumer (options) {
    return new this.kafka.Consumer(this.client, options)
  }

  /**
   * Publish one or more messages
   *
   * @param {Object[]} payloads
   *
   * @returns {Promise}
   *
   * @see {@link https://github.com/SOHU-Co/kafka-node#sendpayloads-cb}
   */
  publish (payloads) {
    return new Promise((resolve, reject) => {
      this.getProducer().send(payloads, (err, data) => {
        return err ? reject(err) : resolve(data)
      })
    })
  }

  /**
   * Subscribe to one or more topics.
   *
   * @param {String[]} topics
   * @param {Function} callback
   */
  subscribe (topics, callback) {
    topics = topics instanceof Array ? topics : [topics]

    const consumer = this.getConsumer(this.topicsToArrayOfFetchRequest(topics))

    consumer.addTopics(this.topicsToArrayOfFetchRequest(topics))
    consumer.on('message', (message) => callback(null, message))
    consumer.on('error', (err) => callback(err))
  }

  /**
   * @see {@link https://github.com/SOHU-Co/kafka-node#consumerclient-payloads-options}
   */
  topicsToArrayOfFetchRequest (topics) {
    return topics.map((topic) => ({ topic }))
  }
}
