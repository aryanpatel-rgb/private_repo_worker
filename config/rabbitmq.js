/**
 * RabbitMQ Configuration for sengine-workers
 * Handles message queue connections for processing inbox messages
 *
 * @module config/rabbitmq
 */

const amqp = require('amqplib');
const { logger } = require('../services/logger.service');
const CONFIG = require('./config');

let connection = null;
let channel = null;

// Queue names (same as main sengine)
const QUEUES = {
    SEND_MESSAGE: 'inbox.send.message',
    INBOUND_MESSAGE: 'inbox.inbound.message',
    STATUS_UPDATE: 'inbox.status.update',
    NOTIFICATION: 'inbox.notification',
    // High-scale drip queues
    DRIP_MESSAGES: 'drip.messages.queue',
    DRIP_DEAD_LETTER: 'drip.dead.letter'
};

// Exchange names
const EXCHANGES = {
    INBOX: 'inbox.exchange',
    INBOX_DLX: 'inbox.dlx',
    // High-scale drip exchanges
    DRIP: 'drip.exchange',
    DRIP_DLX: 'drip.dlx'
};

// Routing keys
const ROUTING_KEYS = {
    SEND: 'send',
    INBOUND: 'inbound',
    STATUS: 'status',
    NOTIFY: 'notify',
    FAILED: 'failed',
    // Drip routing keys
    DRIP_SEND: 'drip.send',
    DRIP_FAILED: 'drip.failed'
};

/**
 * Connect to RabbitMQ and initialize queues
 * @returns {Promise<{connection, channel}>}
 */
const connect = async () => {
    try {
        const url = CONFIG.RABBITMQ.URL;

        console.log('[Workers:RabbitMQ] Connecting to:', url.replace(/:[^:@]*@/, ':****@'));

        connection = await amqp.connect(url);
        channel = await connection.createChannel();

        // Set prefetch for fair dispatch
        await channel.prefetch(CONFIG.MESSAGE_WORKER.PREFETCH || 10);

        // Create exchanges
        await channel.assertExchange(EXCHANGES.INBOX, 'direct', { durable: true });
        await channel.assertExchange(EXCHANGES.INBOX_DLX, 'direct', { durable: true });

        // Queue options with dead letter configuration
        const queueOptions = {
            durable: true,
            deadLetterExchange: EXCHANGES.INBOX_DLX,
            deadLetterRoutingKey: ROUTING_KEYS.FAILED,
            arguments: {
                'x-message-ttl': 86400000 // 24 hours TTL
            }
        };

        // Create main queues
        await channel.assertQueue(QUEUES.SEND_MESSAGE, queueOptions);
        await channel.assertQueue(QUEUES.INBOUND_MESSAGE, queueOptions);
        await channel.assertQueue(QUEUES.STATUS_UPDATE, queueOptions);
        await channel.assertQueue(QUEUES.NOTIFICATION, { durable: true });

        // Dead letter queue
        await channel.assertQueue('inbox.failed', {
            durable: true,
            arguments: {
                'x-message-ttl': 604800000 // 7 days TTL
            }
        });
        await channel.bindQueue('inbox.failed', EXCHANGES.INBOX_DLX, ROUTING_KEYS.FAILED);

        // Bind queues to exchange
        await channel.bindQueue(QUEUES.SEND_MESSAGE, EXCHANGES.INBOX, ROUTING_KEYS.SEND);
        await channel.bindQueue(QUEUES.INBOUND_MESSAGE, EXCHANGES.INBOX, ROUTING_KEYS.INBOUND);
        await channel.bindQueue(QUEUES.STATUS_UPDATE, EXCHANGES.INBOX, ROUTING_KEYS.STATUS);
        await channel.bindQueue(QUEUES.NOTIFICATION, EXCHANGES.INBOX, ROUTING_KEYS.NOTIFY);

        // ============================================
        // HIGH-SCALE DRIP QUEUES
        // ============================================
        await channel.assertExchange(EXCHANGES.DRIP, 'direct', { durable: true });
        await channel.assertExchange(EXCHANGES.DRIP_DLX, 'direct', { durable: true });

        // Drip messages queue
        await channel.assertQueue(QUEUES.DRIP_MESSAGES, {
            durable: true,
            deadLetterExchange: EXCHANGES.DRIP_DLX,
            deadLetterRoutingKey: ROUTING_KEYS.DRIP_FAILED,
            arguments: {
                'x-message-ttl': 3600000 // 1 hour TTL
            }
        });

        // Drip dead letter queue
        await channel.assertQueue(QUEUES.DRIP_DEAD_LETTER, {
            durable: true,
            arguments: {
                'x-message-ttl': 604800000 // 7 days TTL
            }
        });

        // Bind drip queues
        await channel.bindQueue(QUEUES.DRIP_MESSAGES, EXCHANGES.DRIP, ROUTING_KEYS.DRIP_SEND);
        await channel.bindQueue(QUEUES.DRIP_DEAD_LETTER, EXCHANGES.DRIP_DLX, ROUTING_KEYS.DRIP_FAILED);

        console.log('[Workers:RabbitMQ] Connected and queues initialized (including drip queues)');
        logger.info('[Workers:RabbitMQ] Connected and queues initialized');

        // Handle connection errors
        connection.on('error', (err) => {
            console.error('[Workers:RabbitMQ] Connection error:', err.message);
            logger.error('[Workers:RabbitMQ] Connection error:', err);
            reconnect();
        });

        connection.on('close', () => {
            console.warn('[Workers:RabbitMQ] Connection closed');
            logger.warn('[Workers:RabbitMQ] Connection closed');
            reconnect();
        });

        return { connection, channel };

    } catch (error) {
        console.error('[Workers:RabbitMQ] Failed to connect:', error.message);
        logger.error('[Workers:RabbitMQ] Failed to connect:', error);
        reconnect();
        return null;
    }
};

/**
 * Reconnect with exponential backoff
 */
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 10;

const reconnect = () => {
    if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
        console.error('[Workers:RabbitMQ] Max reconnection attempts reached');
        logger.error('[Workers:RabbitMQ] Max reconnection attempts reached');
        return;
    }

    reconnectAttempts++;
    const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), 30000);

    console.log(`[Workers:RabbitMQ] Reconnecting in ${delay}ms (attempt ${reconnectAttempts})`);

    setTimeout(async () => {
        try {
            await connect();
            reconnectAttempts = 0;
        } catch (error) {
            console.error('[Workers:RabbitMQ] Reconnection failed:', error.message);
        }
    }, delay);
};

/**
 * Get the channel instance
 */
const getChannel = () => {
    if (!channel) {
        throw new Error('RabbitMQ channel not initialized. Call connect() first.');
    }
    return channel;
};

/**
 * Get the connection instance
 */
const getConnection = () => {
    if (!connection) {
        throw new Error('RabbitMQ connection not initialized. Call connect() first.');
    }
    return connection;
};

/**
 * Check if RabbitMQ is connected
 */
const isConnected = () => {
    return connection !== null && channel !== null;
};

/**
 * Publish message to exchange
 */
const publish = async (routingKey, data, options = {}) => {
    try {
        const ch = getChannel();
        const message = Buffer.from(JSON.stringify(data));

        const publishOptions = {
            persistent: true,
            timestamp: Date.now(),
            contentType: 'application/json',
            ...options
        };

        const success = ch.publish(EXCHANGES.INBOX, routingKey, message, publishOptions);

        if (success) {
            console.log(`[Workers:RabbitMQ] Published to ${routingKey}:`, data.type || 'message');
        }

        return success;
    } catch (error) {
        console.error('[Workers:RabbitMQ] Publish error:', error.message);
        logger.error('[Workers:RabbitMQ] Publish error:', error);
        return false;
    }
};

/**
 * Consume messages from a queue
 * @param {string} queueName - Queue to consume from
 * @param {Function} handler - Message handler function
 */
const consume = async (queueName, handler) => {
    try {
        const ch = getChannel();

        console.log(`[Workers:RabbitMQ] Starting consumer for queue: ${queueName}`);

        await ch.consume(queueName, async (msg) => {
            if (!msg) return;

            try {
                const content = JSON.parse(msg.content.toString());
                console.log(`[Workers:RabbitMQ] Received message from ${queueName}:`, content.type);

                await handler(content, msg);

                // Acknowledge message after successful processing
                ch.ack(msg);
                console.log(`[Workers:RabbitMQ] Message acknowledged: ${queueName}`);

            } catch (error) {
                console.error(`[Workers:RabbitMQ] Error processing message from ${queueName}:`, error.message);
                logger.error(`[Workers:RabbitMQ] Error processing message:`, error);

                // Reject and requeue (or send to DLX after max retries)
                const retryCount = (msg.properties.headers?.['x-retry-count'] || 0) + 1;
                if (retryCount >= 3) {
                    ch.nack(msg, false, false); // Send to DLX
                    console.log(`[Workers:RabbitMQ] Message sent to DLX after ${retryCount} retries`);
                } else {
                    ch.nack(msg, false, true); // Requeue
                    console.log(`[Workers:RabbitMQ] Message requeued (retry ${retryCount})`);
                }
            }
        }, { noAck: false });

        console.log(`[Workers:RabbitMQ] Consumer started for: ${queueName}`);
        logger.info(`[Workers:RabbitMQ] Consumer started for: ${queueName}`);

    } catch (error) {
        console.error(`[Workers:RabbitMQ] Failed to start consumer for ${queueName}:`, error.message);
        logger.error(`[Workers:RabbitMQ] Failed to start consumer:`, error);
        throw error;
    }
};

/**
 * Get queue statistics
 */
const getQueueStats = async (queueName) => {
    try {
        const ch = getChannel();
        const stats = await ch.checkQueue(queueName);
        return {
            name: queueName,
            messageCount: stats.messageCount,
            consumerCount: stats.consumerCount
        };
    } catch (error) {
        console.error('[Workers:RabbitMQ] GetQueueStats error:', error.message);
        return null;
    }
};

/**
 * Get all queue statistics
 */
const getAllQueueStats = async () => {
    const stats = await Promise.all([
        getQueueStats(QUEUES.SEND_MESSAGE),
        getQueueStats(QUEUES.INBOUND_MESSAGE),
        getQueueStats(QUEUES.STATUS_UPDATE),
        getQueueStats(QUEUES.NOTIFICATION),
        getQueueStats('inbox.failed')
    ]);
    return stats.filter(Boolean);
};

/**
 * Close connection gracefully
 */
const close = async () => {
    try {
        if (channel) {
            await channel.close();
            channel = null;
        }
        if (connection) {
            await connection.close();
            connection = null;
        }
        console.log('[Workers:RabbitMQ] Connection closed gracefully');
        logger.info('[Workers:RabbitMQ] Connection closed gracefully');
    } catch (error) {
        console.error('[Workers:RabbitMQ] Error closing connection:', error.message);
        logger.error('[Workers:RabbitMQ] Error closing connection:', error);
    }
};

module.exports = {
    connect,
    getChannel,
    getConnection,
    isConnected,
    publish,
    consume,
    getQueueStats,
    getAllQueueStats,
    close,
    QUEUES,
    EXCHANGES,
    ROUTING_KEYS
};
