/**
 * Webhook Worker for sengine-workers
 * Consumes webhook events from RabbitMQ and dispatches them to user URLs
 *
 * This worker:
 * - Consumes messages from WEBHOOK queue
 * - Sends HTTP POST to user-configured webhook URLs
 * - Updates delivery status in database
 * - Handles retries for failed deliveries
 *
 * @module workers/webhookWorker
 */

const path = require('path');

// Load environment variables
require('dotenv').config({ path: path.join(__dirname, '../.env') });

const axios = require('axios');
const crypto = require('crypto');
const { logger } = require('../services/logger.service');
const rabbitmq = require('../config/rabbitmq');
const { dbWriter, dbReader } = require('../config/database');
const { to } = require('../services/util.service');
const CONFIG = require('../config/config');

let isRunning = false;

// Webhook dispatch timeout (10 seconds)
const WEBHOOK_TIMEOUT = 10000;

/**
 * Generate HMAC signature for webhook payload
 * @param {Object} payload - Payload to sign
 * @param {string} secret - Webhook secret
 * @returns {string} HMAC signature
 */
const generateSignature = (payload, secret) => {
    if (!secret) return null;
    return 'sha256=' + crypto
        .createHmac('sha256', secret)
        .update(JSON.stringify(payload))
        .digest('hex');
};

/**
 * Update delivery status in database
 * @param {number} deliveryId - Delivery ID
 * @param {Object} result - Delivery result
 */
const updateDeliveryStatus = async (deliveryId, result) => {
    const { success, response_status, response_body, error_message, duration_ms, webhook_id } = result;

    try {
        // Update delivery record
        await dbWriter('webhook_deliveries')
            .where({ id: deliveryId })
            .update({
                status: success ? 'success' : 'failed',
                response_status,
                response_body: typeof response_body === 'object' ? JSON.stringify(response_body) : (response_body || '').substring(0, 5000),
                error_message,
                duration_ms,
                attempted_at: new Date()
            });

        // Update webhook stats
        if (success) {
            await dbWriter('webhooks')
                .where({ id: webhook_id })
                .update({
                    failure_count: 0,
                    last_triggered_at: new Date(),
                    updated_at: new Date()
                });
        } else {
            await dbWriter('webhooks')
                .where({ id: webhook_id })
                .increment('failure_count', 1);
        }

        console.log('[WebhookWorker] Delivery status updated:', {
            deliveryId,
            success,
            response_status
        });

    } catch (error) {
        console.error('[WebhookWorker] Error updating delivery status:', error.message);
        logger.error('[WebhookWorker] Error updating delivery status:', error);
    }
};

/**
 * Dispatch webhook to user URL
 * @param {Object} webhookData - Webhook dispatch data
 * @returns {Promise<Object>} Dispatch result
 */
const dispatchWebhook = async (webhookData) => {
    const { deliveryId, webhookId, url, secret, eventId, eventType, payload } = webhookData;

    const startTime = Date.now();

    // Build webhook payload
    const webhookPayload = {
        event_id: eventId,
        event: eventType,
        timestamp: new Date().toISOString(),
        data: payload
    };

    // Generate signature
    const signature = generateSignature(webhookPayload, secret);

    // Build headers
    const headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'Sengine-Webhook/1.0',
        'X-Webhook-Event': eventType,
        'X-Webhook-Delivery': eventId
    };

    if (signature) {
        headers['X-Webhook-Signature'] = signature;
    }

    try {
        console.log('[WebhookWorker] Dispatching webhook:', {
            deliveryId,
            webhookId,
            eventType,
            url: url.substring(0, 50) + '...'
        });

        const response = await axios.post(url, webhookPayload, {
            headers,
            timeout: WEBHOOK_TIMEOUT,
            maxRedirects: 3,
            validateStatus: () => true // Accept any status code
        });

        const duration_ms = Date.now() - startTime;
        const success = response.status >= 200 && response.status < 300;

        const result = {
            success,
            response_status: response.status,
            response_body: response.data,
            error_message: success ? null : `HTTP ${response.status}: ${response.statusText}`,
            duration_ms,
            webhook_id: webhookId
        };

        // Update delivery status
        await updateDeliveryStatus(deliveryId, result);

        console.log('[WebhookWorker] Webhook dispatched:', {
            deliveryId,
            success,
            status: response.status,
            duration: duration_ms + 'ms'
        });

        return result;

    } catch (error) {
        const duration_ms = Date.now() - startTime;

        let errorMessage = error.message;
        if (error.code === 'ECONNABORTED') {
            errorMessage = 'Request timeout after ' + WEBHOOK_TIMEOUT + 'ms';
        } else if (error.code === 'ENOTFOUND') {
            errorMessage = 'DNS lookup failed: ' + error.hostname;
        } else if (error.code === 'ECONNREFUSED') {
            errorMessage = 'Connection refused';
        }

        const result = {
            success: false,
            response_status: error.response?.status || null,
            response_body: error.response?.data || null,
            error_message: errorMessage,
            duration_ms,
            webhook_id: webhookId
        };

        // Update delivery status
        await updateDeliveryStatus(deliveryId, result);

        console.error('[WebhookWorker] Webhook dispatch failed:', {
            deliveryId,
            error: errorMessage,
            duration: duration_ms + 'ms'
        });

        return result;
    }
};

/**
 * Handle webhook retry
 * @param {Object} retryData - Retry data
 */
const handleRetry = async (retryData) => {
    const { deliveryId, webhookId } = retryData;

    console.log('[WebhookWorker] Processing retry:', { deliveryId, webhookId });

    try {
        // Get delivery record
        const [err, delivery] = await to(
            dbReader('webhook_deliveries')
                .where({ id: deliveryId })
                .first()
        );

        if (err || !delivery) {
            console.error('[WebhookWorker] Delivery not found for retry:', deliveryId);
            return;
        }

        // Get webhook record
        const [webhookErr, webhook] = await to(
            dbReader('webhooks')
                .where({ id: delivery.webhook_id })
                .first()
        );

        if (webhookErr || !webhook) {
            console.error('[WebhookWorker] Webhook not found for retry:', delivery.webhook_id);
            return;
        }

        // Check if webhook is still active
        if (webhook.status !== 'active') {
            console.log('[WebhookWorker] Webhook inactive, skipping retry:', webhook.id);
            return;
        }

        // Dispatch the webhook
        await dispatchWebhook({
            deliveryId: delivery.id,
            webhookId: webhook.id,
            url: webhook.url,
            secret: webhook.secret,
            eventId: delivery.event_id,
            eventType: delivery.event_type,
            payload: delivery.payload
        });

    } catch (error) {
        console.error('[WebhookWorker] Error handling retry:', error);
        logger.error('[WebhookWorker] Error handling retry:', error);
    }
};

/**
 * Handle a single webhook message
 * @param {Object} payload - Message payload
 */
const handleWebhookMessage = async (payload) => {
    const { type, deliveryId, webhookId } = payload;

    console.log('[WebhookWorker] Processing message:', { type, deliveryId, webhookId });

    try {
        if (type === 'WEBHOOK_DISPATCH') {
            // New webhook dispatch
            await dispatchWebhook(payload);

        } else if (type === 'WEBHOOK_RETRY') {
            // Retry failed webhook
            await handleRetry(payload);

        } else {
            console.warn('[WebhookWorker] Unknown message type:', type);
        }

    } catch (error) {
        console.error('[WebhookWorker] Error processing message:', error);
        logger.error('[WebhookWorker] Error processing message:', error);
        throw error;
    }
};

/**
 * Start consuming messages from RabbitMQ
 */
const startConsumer = async () => {
    try {
        console.log('[WebhookWorker] Starting consumer...');

        const channel = rabbitmq.getChannel();

        channel.consume(rabbitmq.QUEUES.WEBHOOK, async (msg) => {
            if (!msg) return;

            let payload;
            try {
                payload = JSON.parse(msg.content.toString());
            } catch (parseError) {
                console.error('[WebhookWorker] Failed to parse message:', parseError);
                channel.nack(msg, false, false);
                return;
            }

            try {
                await handleWebhookMessage(payload);
                channel.ack(msg);
            } catch (error) {
                console.error('[WebhookWorker] Error handling message:', error);
                // ACK anyway - webhook failures are logged in the database
                // User can manually retry from the UI
                channel.ack(msg);
            }
        }, { noAck: false });

        console.log('[WebhookWorker] Consumer started');
        isRunning = true;

    } catch (error) {
        logger.error('[WebhookWorker] Failed to start consumer:', error);
        throw error;
    }
};

/**
 * Start the webhook worker
 */
const start = async () => {
    console.log('[WebhookWorker] ========================================');
    console.log('[WebhookWorker] Starting Webhook Worker');
    console.log('[WebhookWorker] ========================================');

    try {
        // Connect to RabbitMQ if not already connected
        if (!rabbitmq.isConnected()) {
            await rabbitmq.connect();
        }

        // Start consuming
        await startConsumer();

        console.log('[WebhookWorker] Worker started successfully');

    } catch (error) {
        logger.error('[WebhookWorker] Failed to start:', error);
        throw error;
    }
};

/**
 * Stop the webhook worker
 */
const stop = async () => {
    console.log('[WebhookWorker] Stopping worker...');
    isRunning = false;
    console.log('[WebhookWorker] Worker stopped');
};

/**
 * Check if worker is running
 */
const isWorkerRunning = () => isRunning;

// Export for use in main app
module.exports = {
    start,
    stop,
    isWorkerRunning,
    dispatchWebhook,
    handleWebhookMessage
};

// If running as standalone script
if (require.main === module) {
    console.log('[WebhookWorker] Running as standalone process');

    // Handle graceful shutdown
    process.on('SIGINT', async () => {
        console.log('\n[WebhookWorker] Received SIGINT, shutting down...');
        await stop();
        process.exit(0);
    });

    process.on('SIGTERM', async () => {
        console.log('\n[WebhookWorker] Received SIGTERM, shutting down...');
        await stop();
        process.exit(0);
    });

    // Start the worker
    rabbitmq.connect().then(() => {
        start().catch((error) => {
            console.error('[WebhookWorker] Fatal error:', error);
            process.exit(1);
        });
    });
}
