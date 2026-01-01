/**
 * Outbound Message Worker for sengine-workers
 * Consumes messages from RabbitMQ and sends them via Twilio
 *
 * This worker handles the SEND_MESSAGE queue and processes
 * messages that were queued by the main sengine API.
 *
 * OPTIMIZED: Added rate limiting to prevent exceeding Twilio API limits
 *
 * @module workers/outboundMessageWorker
 */

const path = require('path');

// Load environment variables
require('dotenv').config({ path: path.join(__dirname, '../.env') });

const { logger } = require('../services/logger.service');
const rabbitmq = require('../config/rabbitmq');
const twilioService = require('../services/sms/twilio.service');
const { dbWriter, dbReader } = require('../config/database');
const { to } = require('../services/util.service');
const CONFIG = require('../config/config');
const webhookProducer = require('../services/webhook/webhookProducer.service');

let isRunning = false;

// ============================================================================
// RATE LIMITER - Token Bucket Implementation
// ============================================================================
// Twilio limits: ~1 message/second for regular numbers, ~10/second for toll-free
// We use a conservative limit to prevent hitting API limits

class RateLimiter {
    constructor(options = {}) {
        this.maxTokens = options.maxTokens || 10;           // Max burst capacity
        this.refillRate = options.refillRate || 5;          // Tokens per second
        this.tokens = this.maxTokens;
        this.lastRefill = Date.now();
        this.waitQueue = [];
        this.isProcessingQueue = false;
    }

    /**
     * Refill tokens based on elapsed time
     */
    refill() {
        const now = Date.now();
        const elapsed = (now - this.lastRefill) / 1000; // seconds
        const tokensToAdd = elapsed * this.refillRate;
        this.tokens = Math.min(this.maxTokens, this.tokens + tokensToAdd);
        this.lastRefill = now;
    }

    /**
     * Try to acquire a token, returns true if successful
     */
    tryAcquire() {
        this.refill();
        if (this.tokens >= 1) {
            this.tokens -= 1;
            return true;
        }
        return false;
    }

    /**
     * Wait until a token is available
     * @returns {Promise<void>}
     */
    async acquire() {
        if (this.tryAcquire()) {
            return;
        }

        // Calculate wait time until next token is available
        const tokensNeeded = 1 - this.tokens;
        const waitTime = Math.ceil((tokensNeeded / this.refillRate) * 1000);

        logger.debug('[RateLimiter] Waiting for token:', { waitTime, tokens: this.tokens });

        return new Promise((resolve) => {
            setTimeout(() => {
                this.refill();
                this.tokens -= 1;
                resolve();
            }, waitTime);
        });
    }

    /**
     * Get current status
     */
    getStatus() {
        this.refill();
        return {
            tokens: Math.floor(this.tokens * 100) / 100,
            maxTokens: this.maxTokens,
            refillRate: this.refillRate
        };
    }
}

// Create rate limiter instance
// Default: 5 messages/second with burst of 10
const rateLimiter = new RateLimiter({
    maxTokens: parseInt(process.env.TWILIO_RATE_LIMIT_BURST || '10', 10),
    refillRate: parseInt(process.env.TWILIO_RATE_LIMIT_PER_SEC || '5', 10)
});

// Stats tracking
const stats = {
    processed: 0,
    failed: 0,
    rateLimited: 0,
    startTime: Date.now()
};

/**
 * Get worker statistics
 */
const getStats = () => {
    const uptime = (Date.now() - stats.startTime) / 1000;
    return {
        ...stats,
        uptime,
        messagesPerSecond: stats.processed / uptime,
        rateLimiterStatus: rateLimiter.getStatus()
    };
};

/**
 * Refund credits for failed message
 * @param {string} userId - User ID
 * @param {number} creditCost - Amount to refund
 * @param {string} reason - Refund reason
 * @param {number} messageId - Message ID for reference
 */
const refundCredits = async (userId, creditCost, reason, messageId) => {
    if (!creditCost || creditCost <= 0) return;

    try {
        // Check if credits service exists
        let creditsService;
        try {
            creditsService = require('../services/credits');
        } catch (e) {
            logger.warn('[OutboundMessageWorker] Credits service not available for refund');
            return;
        }

        await creditsService.refundCredits(userId, creditCost, {
            description: `Refund: ${reason}`,
            referenceType: 'sms_refund',
            referenceId: messageId,
        });

        logger.info('[OutboundMessageWorker] Credit refunded:', { userId, amount: creditCost, reason });
    } catch (error) {
        logger.error('[OutboundMessageWorker] Failed to refund credit:', error);
    }
};

/**
 * Handle a send message job from the queue
 * OPTIMIZED: Uses rate limiting and supports credit refunds
 */
const handleSendMessage = async (payload, msg) => {
    const { data } = payload;

    logger.info('[OutboundMessageWorker] Processing message:', {
        messageId: data.messageId,
        bRef: data.bRef,
        to: data.toNumber,
        retryCount: payload.retryCount || 0
    });

    try {
        // ========== LOAD TEST CHECK ==========
        if (data.isLoadTest) {
            logger.debug('[OutboundMessageWorker] LOAD TEST message - skipping Twilio:', data.bRef);
            await new Promise(resolve => setTimeout(resolve, 50 + Math.random() * 150));
            stats.processed++;
            return { success: true, loadTest: true };
        }
        // =====================================

        // ========== DUPLICATE CHECK ==========
        const [checkErr, existingMessage] = await to(
            dbReader('messages')
                .select('id', 'msg_id', 'delivery_status')
                .where({ id: data.messageId })
                .first()
        );

        if (existingMessage?.msg_id) {
            logger.warn('[OutboundMessageWorker] DUPLICATE DETECTED - Already sent:', {
                messageId: data.messageId,
                bRef: data.bRef,
                existingTwilioSid: existingMessage.msg_id,
                status: existingMessage.delivery_status
            });
            stats.processed++;
            return { success: true, duplicate: true };
        }
        // =====================================

        // ========== RATE LIMITING ==========
        // Wait for rate limiter token before sending
        const rateLimitStart = Date.now();
        await rateLimiter.acquire();
        const rateLimitWait = Date.now() - rateLimitStart;

        if (rateLimitWait > 100) {
            stats.rateLimited++;
            logger.debug('[OutboundMessageWorker] Rate limited, waited:', rateLimitWait, 'ms');
        }
        // ===================================

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
            is_charged: twilioResult.success ? 1 : 0,
            updated_at: new Date()
        };

        await to(
            dbWriter('messages')
                .where({ id: data.messageId })
                .update(updateData)
        );

        // Handle success/failure
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
            stats.processed++;

            // ========== TRIGGER WEBHOOK FOR OUTBOUND MESSAGE ==========
            webhookProducer.queueOutboundMessageEvent({
                userId: data.userId,
                workspaceId: data.workspaceId,
                messageId: data.messageId,
                contactId: data.contactId,
                toNumber: data.toNumber,
                fromNumber: data.fromNumber,
                message: data.message,
                twilioSid: twilioResult.sid
            }).then(result => {
                if (result.queued > 0) {
                    console.log('[OutboundMessageWorker] Webhook events queued:', result.queued);
                }
            }).catch(err => {
                console.error('[OutboundMessageWorker] Webhook error:', err.message);
            });
            // ===========================================================
        } else {
            // ========== REFUND CREDITS ON FAILURE ==========
            // Credits were deducted upfront for queued messages
            if (data.creditCost && data.userId) {
                await refundCredits(
                    data.userId,
                    data.creditCost,
                    `Message send failed to ${data.toNumber}`,
                    data.messageId
                );
            }
            stats.failed++;
        }

        logger.info('[OutboundMessageWorker] Message processed:', {
            messageId: data.messageId,
            bRef: data.bRef,
            success: twilioResult.success,
            twilioSid: twilioResult.sid
        });

        return { success: twilioResult.success };

    } catch (error) {
        logger.error('[OutboundMessageWorker] Error processing message:', error);

        // Update message as failed
        await to(
            dbWriter('messages')
                .where({ id: data.messageId })
                .update({
                    status: '3',
                    delivery_status: 'failed',
                    response: JSON.stringify({ error: error.message }),
                    is_charged: 0,
                    updated_at: new Date()
                })
        );

        // ========== REFUND CREDITS ON ERROR ==========
        if (data.creditCost && data.userId) {
            await refundCredits(
                data.userId,
                data.creditCost,
                `Message error: ${error.message}`,
                data.messageId
            );
        }

        stats.failed++;
        throw error;
    }
};

/**
 * Start consuming messages from RabbitMQ
 */
const startConsumer = async () => {
    try {
        logger.info('[OutboundMessageWorker] Starting consumer...');

        // Consume from SEND_MESSAGE queue
        await rabbitmq.consume(rabbitmq.QUEUES.SEND_MESSAGE, async (payload, msg) => {
            if (payload.type === 'SEND_SMS') {
                await handleSendMessage(payload, msg);
            } else {
                logger.warn('[OutboundMessageWorker] Unknown message type:', payload.type);
            }
        });

        logger.info('[OutboundMessageWorker] Consumer started');
        isRunning = true;

    } catch (error) {
        logger.error('[OutboundMessageWorker] Failed to start consumer:', error);
        throw error;
    }
};

/**
 * Start the message worker
 */
const start = async () => {
    logger.info('[OutboundMessageWorker] ========================================');
    logger.info('[OutboundMessageWorker] Starting Message Worker');
    logger.info('[OutboundMessageWorker] Prefetch:', CONFIG.MESSAGE_WORKER?.PREFETCH || 'default');
    logger.info('[OutboundMessageWorker] Rate Limit:', {
        burst: rateLimiter.maxTokens,
        perSecond: rateLimiter.refillRate
    });
    logger.info('[OutboundMessageWorker] ========================================');

    try {
        // Connect to RabbitMQ
        await rabbitmq.connect();

        // Start consuming
        await startConsumer();

        logger.info('[OutboundMessageWorker] Worker started successfully');

        // Log stats periodically (every 60 seconds)
        setInterval(() => {
            const currentStats = getStats();
            logger.info('[OutboundMessageWorker] Stats:', currentStats);
        }, 60000);

    } catch (error) {
        logger.error('[OutboundMessageWorker] Failed to start:', error);
        throw error;
    }
};

/**
 * Stop the message worker
 */
const stop = async () => {
    logger.info('[OutboundMessageWorker] Stopping worker...');
    logger.info('[OutboundMessageWorker] Final stats:', getStats());

    try {
        await rabbitmq.close();
        isRunning = false;
        logger.info('[OutboundMessageWorker] Worker stopped');
    } catch (error) {
        logger.error('[OutboundMessageWorker] Error stopping worker:', error);
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
    handleSendMessage,
    getStats,
    rateLimiter
};

// If running as standalone script
if (require.main === module) {
    logger.info('[OutboundMessageWorker] Running as standalone process');

    // Handle graceful shutdown
    process.on('SIGINT', async () => {
        logger.info('\n[OutboundMessageWorker] Received SIGINT, shutting down...');
        await stop();
        process.exit(0);
    });

    process.on('SIGTERM', async () => {
        logger.info('\n[OutboundMessageWorker] Received SIGTERM, shutting down...');
        await stop();
        process.exit(0);
    });

    // Start the worker
    start().catch((error) => {
        logger.error('[OutboundMessageWorker] Fatal error:', error);
        process.exit(1);
    });
}
