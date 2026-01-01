/**
 * Delivery Report Worker for sengine-workers
 * Processes delivery status updates from Twilio webhooks
 *
 * This worker:
 * - Consumes messages from STATUS_UPDATE queue
 * - Updates message delivery_status in database
 * - Publishes WebSocket notifications for real-time UI updates
 *
 * @module workers/deliveryReportWorker
 */

const path = require('path');

// Load environment variables
require('dotenv').config({ path: path.join(__dirname, '../.env') });

const { logger } = require('../services/logger.service');
const rabbitmq = require('../config/rabbitmq');
const { dbReader, dbWriter } = require('../config/database');
const { to } = require('../services/util.service');
const webhookProducer = require('../services/webhook/webhookProducer.service');
const CONFIG = require('../config/config');

let isRunning = false;

/**
 * Status mapping from Twilio to our format
 */
const STATUS_MAP = {
    'queued': { status: '0', delivery_status: 'queued' },
    'sending': { status: '1', delivery_status: 'sending' },
    'sent': { status: '1', delivery_status: 'sent' },
    'delivered': { status: '2', delivery_status: 'delivered' },
    'undelivered': { status: '4', delivery_status: 'undelivered' },
    'failed': { status: '3', delivery_status: 'failed' },
    'read': { status: '2', delivery_status: 'read' }
};

/**
 * Update message delivery status in database
 */
const updateDeliveryStatus = async (bRef, status, messageSid, errorCode = null) => {
    console.log('[DeliveryReportWorker] Updating delivery status:', { bRef, status, messageSid });

    try {
        // Find message by b_ref or msg_id
        const [findErr, message] = await to(
            dbReader('messages')
                .select('id', 'user_id', 'contact_id', 'workspace_id', 'delivery_status')
                .where(function() {
                    this.where('b_ref', bRef)
                        .orWhere('msg_id', messageSid);
                })
                .first()
        );

        if (!message) {
            console.warn('[DeliveryReportWorker] Message not found:', { bRef, messageSid });
            return { success: false, error: 'Message not found' };
        }

        // Get status mapping
        const statusData = STATUS_MAP[status] || { status: message.status, delivery_status: status };

        // Build update object (only include fields that exist in the table)
        const updateData = {
            ...statusData,
            updated_at: new Date()
        };

        // Only add msg_id if provided
        if (messageSid) {
            updateData.msg_id = messageSid;
        }

        // Update message
        const [updateErr] = await to(
            dbWriter('messages')
                .where({ id: message.id })
                .update(updateData)
        );

        if (updateErr) {
            console.error('[DeliveryReportWorker] Error updating message:', updateErr);
            logger.error('[DeliveryReportWorker] Error updating message:', updateErr);
            return { success: false, error: updateErr.message };
        }

        console.log('[DeliveryReportWorker] Status updated:', {
            messageId: message.id,
            oldStatus: message.delivery_status,
            newStatus: status
        });

        return {
            success: true,
            message: {
                id: message.id,
                user_id: message.user_id,
                contact_id: message.contact_id,
                workspace_id: message.workspace_id,
                delivery_status: status
            }
        };

    } catch (error) {
        console.error('[DeliveryReportWorker] updateDeliveryStatus error:', error);
        logger.error('[DeliveryReportWorker] updateDeliveryStatus error:', error);
        return { success: false, error: error.message };
    }
};

/**
 * Publish notification to RabbitMQ for WebSocket delivery
 */
const publishNotification = async (channel, data) => {
    try {
        await rabbitmq.publish('notify', {
            type: 'WEBSOCKET_NOTIFICATION',
            channel: channel,
            data: data,
            timestamp: Date.now()
        });
        console.log('[DeliveryReportWorker] Published notification:', channel);
    } catch (error) {
        console.error('[DeliveryReportWorker] Failed to publish notification:', error.message);
    }
};

/**
 * Handle a single status update
 */
const handleStatusUpdate = async (payload) => {
    const { data } = payload;

    console.log('[DeliveryReportWorker] Processing status update:', {
        messageSid: data.messageSid,
        status: data.status,
        bRef: data.bRef
    });

    try {
        // Update delivery status in database
        const result = await updateDeliveryStatus(
            data.bRef,
            data.status,
            data.messageSid,
            data.errorCode
        );

        if (result.success && result.message) {
            // Publish real-time status update notification
            await publishNotification('message:status', {
                userId: result.message.user_id,
                messageId: result.message.id,
                contactId: result.message.contact_id,
                status: data.status,
                errorCode: data.errorCode,
                errorMessage: data.errorMessage
            });

            // If message failed, send failure notification
            if (data.status === 'failed' || data.status === 'undelivered') {
                await publishNotification('message:delivery_failed', {
                    userId: result.message.user_id,
                    messageId: result.message.id,
                    contactId: result.message.contact_id,
                    errorCode: data.errorCode,
                    errorMessage: data.errorMessage || 'Message delivery failed'
                });

                // Trigger webhook for message failed (non-blocking)
                webhookProducer.queueMessageFailedEvent({
                    userId: result.message.user_id,
                    workspaceId: result.message.workspace_id,
                    messageId: result.message.id,
                    contactId: result.message.contact_id,
                    errorCode: data.errorCode,
                    errorMessage: data.errorMessage || 'Message delivery failed'
                }).catch(err => {
                    console.error('[DeliveryReportWorker] Webhook error:', err.message);
                });
            }

            // Trigger webhook for message delivered (non-blocking)
            if (data.status === 'delivered') {
                webhookProducer.queueMessageDeliveredEvent({
                    userId: result.message.user_id,
                    workspaceId: result.message.workspace_id,
                    messageId: result.message.id,
                    contactId: result.message.contact_id,
                    status: data.status
                }).catch(err => {
                    console.error('[DeliveryReportWorker] Webhook error:', err.message);
                });
            }

            console.log('[DeliveryReportWorker] Status update processed:', {
                messageId: result.message.id,
                status: data.status
            });

            return { success: true };

        } else {
            console.warn('[DeliveryReportWorker] Status update failed:', result.error);
            return { success: false, error: result.error };
        }

    } catch (error) {
        console.error('[DeliveryReportWorker] Error processing status update:', error);
        logger.error('[DeliveryReportWorker] Error:', error);
        throw error;
    }
};

/**
 * Start consuming messages from RabbitMQ
 */
const startConsumer = async () => {
    try {
        console.log('[DeliveryReportWorker] Starting consumer...');

        const channel = rabbitmq.getChannel();

        channel.consume(rabbitmq.QUEUES.STATUS_UPDATE, async (msg) => {
            if (!msg) return;

            let payload;
            try {
                payload = JSON.parse(msg.content.toString());
            } catch (parseError) {
                console.error('[DeliveryReportWorker] Failed to parse message:', parseError);
                channel.nack(msg, false, false);
                return;
            }

            try {
                await handleStatusUpdate(payload);
                // Always ACK status updates (don't retry - Twilio sends multiple)
                channel.ack(msg);
            } catch (error) {
                console.error('[DeliveryReportWorker] Error handling status update:', error);
                // ACK anyway - status updates are not critical enough to retry
                // Twilio will send multiple status updates, so missing one is not fatal
                channel.ack(msg);
            }
        }, { noAck: false });

        console.log('[DeliveryReportWorker] Consumer started');
        isRunning = true;

    } catch (error) {
        logger.error('[DeliveryReportWorker] Failed to start consumer:', error);
        throw error;
    }
};

/**
 * Start the status worker
 */
const start = async () => {
    console.log('[DeliveryReportWorker] ========================================');
    console.log('[DeliveryReportWorker] Starting Delivery Report Worker');
    console.log('[DeliveryReportWorker] ========================================');

    try {
        // Connect to RabbitMQ if not already connected
        if (!rabbitmq.isConnected()) {
            await rabbitmq.connect();
        }

        // Start consuming
        await startConsumer();

        console.log('[DeliveryReportWorker] Worker started successfully');

    } catch (error) {
        logger.error('[DeliveryReportWorker] Failed to start:', error);
        throw error;
    }
};

/**
 * Stop the status worker
 */
const stop = async () => {
    console.log('[DeliveryReportWorker] Stopping worker...');
    isRunning = false;
    console.log('[DeliveryReportWorker] Worker stopped');
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
    handleStatusUpdate,
    updateDeliveryStatus
};

// If running as standalone script
if (require.main === module) {
    console.log('[DeliveryReportWorker] Running as standalone process');

    // Handle graceful shutdown
    process.on('SIGINT', async () => {
        console.log('\n[DeliveryReportWorker] Received SIGINT, shutting down...');
        await stop();
        process.exit(0);
    });

    process.on('SIGTERM', async () => {
        console.log('\n[DeliveryReportWorker] Received SIGTERM, shutting down...');
        await stop();
        process.exit(0);
    });

    // Start the worker
    rabbitmq.connect().then(() => {
        start().catch((error) => {
            console.error('[DeliveryReportWorker] Fatal error:', error);
            process.exit(1);
        });
    });
}
