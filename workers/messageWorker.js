/**
 * Message Worker for sengine-workers
 * Consumes messages from RabbitMQ and sends them via Twilio
 *
 * This worker handles the SEND_MESSAGE queue and processes
 * messages that were queued by the main sengine API.
 *
 * @module workers/messageWorker
 */

const path = require('path');

// Load environment variables
require('dotenv').config({ path: path.join(__dirname, '../.env') });

const { logger } = require('../services/logger.service');
const rabbitmq = require('../config/rabbitmq');
const twilioService = require('../services/sms/twilio.service');
const { dbWriter } = require('../config/database');
const { to } = require('../services/util.service');
const CONFIG = require('../config/config');

let isRunning = false;

/**
 * Handle a send message job from the queue
 */
const handleSendMessage = async (payload, msg) => {
    const { data } = payload;

    console.log('[MessageWorker] Processing message:', {
        messageId: data.messageId,
        bRef: data.bRef,
        to: data.toNumber,
        retryCount: payload.retryCount || 0
    });

    try {
        // ========== LOAD TEST CHECK ==========
        // Skip load test messages (don't actually send via Twilio)
        if (data.isLoadTest) {
            console.log('[MessageWorker] LOAD TEST message - skipping Twilio:', data.bRef);
            // Simulate processing delay (50-200ms)
            await new Promise(resolve => setTimeout(resolve, 50 + Math.random() * 150));
            return { success: true, loadTest: true };
        }
        // =====================================

        // ========== DUPLICATE CHECK ==========
        // Check if message was already sent (crash recovery scenario)
        // This prevents sending the same message twice if worker crashed
        // after Twilio send but before ACK
        const [checkErr, existingMessage] = await to(
            dbWriter('messages')
                .select('id', 'msg_id', 'delivery_status')
                .where({ id: data.messageId })
                .first()
        );

        if (existingMessage?.msg_id) {
            // Message already has Twilio SID - it was already sent!
            console.log('[MessageWorker] DUPLICATE DETECTED - Already sent:', {
                messageId: data.messageId,
                bRef: data.bRef,
                existingTwilioSid: existingMessage.msg_id,
                status: existingMessage.delivery_status
            });
            // Return success to ACK the message without resending
            return { success: true, duplicate: true };
        }
        // =====================================

        // Send via Twilio
        const twilioResult = await twilioService.sendSMS({
            from: data.fromNumber,
            to: data.toNumber,
            body: data.message,
            mediaUrl: data.mediaUrl,
            statusCallback: data.statusCallbackUrl,
            credentials: data.twilioCredentials
        });

        // Update message record in database
        const updateData = {
            msg_id: twilioResult.sid,
            status: twilioResult.success ? '1' : '3',
            delivery_status: twilioResult.success ? twilioResult.status : 'failed',
            response: JSON.stringify(twilioResult),
            updated_at: new Date()
        };

        await to(
            dbWriter('messages')
                .where({ id: data.messageId })
                .update(updateData)
        );

        // Update contact
        if (twilioResult.success) {
            const now = new Date();
            await to(
                dbWriter('contacts')
                    .where({ id: data.contactId })
                    .update({
                        last_message: now,
                        open_chat: 1,
                        archive: 0,
                        updated_at: now
                    })
            );
        }

        console.log('[MessageWorker] Message processed:', {
            messageId: data.messageId,
            bRef: data.bRef,
            success: twilioResult.success,
            twilioSid: twilioResult.sid
        });

        return { success: twilioResult.success };

    } catch (error) {
        logger.error('[MessageWorker] Error processing message:', error);

        // Update message as failed
        await to(
            dbWriter('messages')
                .where({ id: data.messageId })
                .update({
                    status: '3',
                    delivery_status: 'failed',
                    response: JSON.stringify({ error: error.message }),
                    updated_at: new Date()
                })
        );

        throw error;
    }
};

/**
 * Start consuming messages from RabbitMQ
 */
const startConsumer = async () => {
    try {
        console.log('[MessageWorker] Starting consumer...');

        // Consume from SEND_MESSAGE queue
        await rabbitmq.consume(rabbitmq.QUEUES.SEND_MESSAGE, async (payload, msg) => {
            if (payload.type === 'SEND_SMS') {
                await handleSendMessage(payload, msg);
            } else {
                console.log('[MessageWorker] Unknown message type:', payload.type);
            }
        });

        console.log('[MessageWorker] Consumer started');
        isRunning = true;

    } catch (error) {
        logger.error('[MessageWorker] Failed to start consumer:', error);
        throw error;
    }
};

/**
 * Start the message worker
 */
const start = async () => {
    console.log('[MessageWorker] ========================================');
    console.log('[MessageWorker] Starting Message Worker');
    console.log('[MessageWorker] Prefetch:', CONFIG.MESSAGE_WORKER.PREFETCH);
    console.log('[MessageWorker] ========================================');

    try {
        // Connect to RabbitMQ
        await rabbitmq.connect();

        // Start consuming
        await startConsumer();

        console.log('[MessageWorker] Worker started successfully');

    } catch (error) {
        logger.error('[MessageWorker] Failed to start:', error);
        throw error;
    }
};

/**
 * Stop the message worker
 */
const stop = async () => {
    console.log('[MessageWorker] Stopping worker...');

    try {
        await rabbitmq.close();
        isRunning = false;
        console.log('[MessageWorker] Worker stopped');
    } catch (error) {
        logger.error('[MessageWorker] Error stopping worker:', error);
    }
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
    handleSendMessage
};

// If running as standalone script
if (require.main === module) {
    console.log('[MessageWorker] Running as standalone process');

    // Handle graceful shutdown
    process.on('SIGINT', async () => {
        console.log('\n[MessageWorker] Received SIGINT, shutting down...');
        await stop();
        process.exit(0);
    });

    process.on('SIGTERM', async () => {
        console.log('\n[MessageWorker] Received SIGTERM, shutting down...');
        await stop();
        process.exit(0);
    });

    // Start the worker
    start().catch((error) => {
        console.error('[MessageWorker] Fatal error:', error);
        process.exit(1);
    });
}
