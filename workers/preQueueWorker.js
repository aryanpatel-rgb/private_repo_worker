/**
 * Pre-Queue Worker for sengine-workers (HIGH-SCALE MODE)
 *
 * This worker is part of the high-scale drip processing pipeline:
 * scheduled_messages (DB) → PreQueueWorker → RabbitMQ → MessageConsumer → Twilio
 *
 * Features:
 * - Fetches pending scheduled_messages from database
 * - Pushes to RabbitMQ 10-15 minutes before send time
 * - Batch processing for high throughput (2000+ per cycle)
 * - Marks messages as QUEUED in database
 *
 * Run standalone: node workers/preQueueWorker.js
 * Or as part of main app: require('./workers/preQueueWorker').start()
 *
 * @module workers/preQueueWorker
 */

const path = require('path');

// Load environment variables
require('dotenv').config({ path: path.join(__dirname, '../.env') });

const { logger } = require('../services/logger.service');
const scheduledMessageService = require('../services/drip/scheduledMessage.service');
const rabbitmq = require('../config/rabbitmq');
const CONFIG = require('../config/config');

// Configuration
const HIGH_SCALE_CONFIG = CONFIG.HIGH_SCALE_DRIP;
const INTERVAL_MS = HIGH_SCALE_CONFIG.PRE_QUEUE_WORKER_INTERVAL;
const PRE_QUEUE_MINUTES = HIGH_SCALE_CONFIG.PRE_QUEUE_MINUTES;
const BATCH_SIZE = HIGH_SCALE_CONFIG.PRE_QUEUE_BATCH;

let isRunning = false;
let intervalId = null;

/**
 * Push messages to RabbitMQ queue
 */
const pushMessagesToQueue = async (messages) => {
    const result = {
        queued: 0,
        failed: 0,
        errors: []
    };

    if (!messages || messages.length === 0) {
        return result;
    }

    if (!rabbitmq.isConnected()) {
        console.log('[PreQueueWorker] RabbitMQ not connected, skipping...');
        result.failed = messages.length;
        result.errors.push('RabbitMQ not connected');
        return result;
    }

    const queuedIds = [];
    const channel = rabbitmq.getChannel();

    for (const msg of messages) {
        try {
            // Create message payload
            const payload = {
                scheduledMessageId: msg.id,
                dripContactId: msg.drip_contact_id,  // Link to drip_contact for status update
                userId: msg.user_id,
                workspaceId: msg.workspace_id,
                contactId: msg.contact_id,
                dripId: msg.drip_id,
                campaignId: msg.campaign_id,
                fromNumber: msg.from_number,
                toNumber: msg.to_number,
                sid: msg.sid,
                message: msg.message,
                mediaUrl: msg.media_url,
                scheduledAt: msg.scheduled_at,
                queuedAt: new Date().toISOString()
            };

            // Publish to drip exchange
            const published = channel.publish(
                rabbitmq.EXCHANGES.DRIP,
                rabbitmq.ROUTING_KEYS.DRIP_SEND,
                Buffer.from(JSON.stringify(payload)),
                {
                    persistent: true,
                    contentType: 'application/json',
                    messageId: msg.uid || String(msg.id),
                    timestamp: Date.now()
                }
            );

            if (published) {
                queuedIds.push(msg.id);
                result.queued++;
            } else {
                result.failed++;
                result.errors.push({ id: msg.id, error: 'Publish returned false (buffer full)' });
            }

        } catch (error) {
            result.failed++;
            result.errors.push({ id: msg.id, error: error.message });
        }
    }

    // Mark successfully queued messages in database
    if (queuedIds.length > 0) {
        await scheduledMessageService.markMessagesAsQueued(queuedIds);
    }

    return result;
};

/**
 * Main worker function - runs on interval
 */
const runWorkerCycle = async () => {
    if (isRunning) {
        console.log('[PreQueueWorker] Previous cycle still running, skipping...');
        return;
    }

    isRunning = true;
    const startTime = Date.now();

    console.log('[PreQueueWorker] ========================================');
    console.log('[PreQueueWorker] Starting pre-queue cycle');
    console.log('[PreQueueWorker] Time:', new Date().toISOString());
    console.log('[PreQueueWorker] ========================================');

    try {
        if (!rabbitmq.isConnected()) {
            console.log('[PreQueueWorker] RabbitMQ not connected, skipping cycle');
            return;
        }

        // Get messages ready for queue
        const messages = await scheduledMessageService.getMessagesReadyForQueue(
            PRE_QUEUE_MINUTES,
            BATCH_SIZE
        );

        if (messages.length === 0) {
            console.log('[PreQueueWorker] No messages to queue');
            return;
        }

        console.log('[PreQueueWorker] Found messages to queue:', messages.length);

        // Push to RabbitMQ
        const result = await pushMessagesToQueue(messages);

        const duration = Date.now() - startTime;
        console.log('[PreQueueWorker] Cycle complete:');
        console.log(`  - Queued: ${result.queued}`);
        console.log(`  - Failed: ${result.failed}`);
        console.log(`  - Duration: ${duration}ms`);

        if (result.errors.length > 0) {
            console.log('[PreQueueWorker] Errors:', JSON.stringify(result.errors.slice(0, 5), null, 2));
            logger.warn('[PreQueueWorker] Queue errors:', result.errors);
        }

    } catch (error) {
        console.error('[PreQueueWorker] Error in cycle:', error);
        logger.error('[PreQueueWorker] Worker cycle error:', error);
    } finally {
        isRunning = false;
    }
};

/**
 * Start the Pre-Queue Worker
 */
const start = async () => {
    console.log('[PreQueueWorker] ========================================');
    console.log('[PreQueueWorker] Starting Pre-Queue Worker');
    console.log('[PreQueueWorker] Interval:', INTERVAL_MS, 'ms');
    console.log('[PreQueueWorker] Pre-Queue Minutes:', PRE_QUEUE_MINUTES);
    console.log('[PreQueueWorker] Batch Size:', BATCH_SIZE);
    console.log('[PreQueueWorker] ========================================');

    // Connect to RabbitMQ if not connected
    if (!rabbitmq.isConnected()) {
        console.log('[PreQueueWorker] Connecting to RabbitMQ...');
        await rabbitmq.connect();
    }

    // Process immediately on start
    await runWorkerCycle();

    // Then process on interval
    intervalId = setInterval(runWorkerCycle, INTERVAL_MS);

    console.log('[PreQueueWorker] Worker started successfully');
};

/**
 * Stop the Pre-Queue Worker
 */
const stop = async () => {
    console.log('[PreQueueWorker] Stopping worker...');

    if (intervalId) {
        clearInterval(intervalId);
        intervalId = null;
    }

    isRunning = false;
    console.log('[PreQueueWorker] Worker stopped');
};

/**
 * Get worker status
 */
const getStatus = () => {
    return {
        running: !!intervalId,
        isProcessing: isRunning,
        rabbitMQConnected: rabbitmq.isConnected(),
        config: {
            intervalMs: INTERVAL_MS,
            preQueueMinutes: PRE_QUEUE_MINUTES,
            batchSize: BATCH_SIZE
        }
    };
};

// Export for use in main app
module.exports = {
    start,
    stop,
    getStatus,
    runWorkerCycle
};

// If running as standalone script
if (require.main === module) {
    console.log('[PreQueueWorker] Running as standalone process');

    // Handle graceful shutdown
    process.on('SIGINT', async () => {
        console.log('\n[PreQueueWorker] Received SIGINT, shutting down...');
        await stop();
        await rabbitmq.close();
        process.exit(0);
    });

    process.on('SIGTERM', async () => {
        console.log('\n[PreQueueWorker] Received SIGTERM, shutting down...');
        await stop();
        await rabbitmq.close();
        process.exit(0);
    });

    // Start the worker
    start();
}
