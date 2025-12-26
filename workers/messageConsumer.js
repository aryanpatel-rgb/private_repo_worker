/**
 * Message Consumer Worker for sengine-workers
 * Consumes drip messages from RabbitMQ and sends via Twilio
 *
 * Run standalone: node workers/messageConsumer.js
 * Or as part of main app: require('./workers/messageConsumer').start()
 *
 * @module workers/messageConsumer
 */

const path = require('path');

// Load environment variables
require('dotenv').config({ path: path.join(__dirname, '../.env') });

const { to } = require('../services/util.service');
const { dbReader, dbWriter } = require('../config/database');
const { logger } = require('../services/logger.service');
const { v4: uuidv4 } = require('uuid');
const twilioService = require('../services/sms/twilio.service');
const scheduledMessageService = require('../services/drip/scheduledMessage.service');
const creditsService = require('../services/credits.service');
const rabbitmq = require('../config/rabbitmq');
const CONFIG = require('../config/config');

// Credit cost per SMS
const SMS_CREDIT_COST = 1;

// Configuration
const HIGH_SCALE_CONFIG = CONFIG.HIGH_SCALE_DRIP;
const PREFETCH_COUNT = HIGH_SCALE_CONFIG.CONSUMER_PREFETCH;
const RATE_LIMIT_MS = HIGH_SCALE_CONFIG.RATE_LIMIT_MS;
const CONSUMER_TAG = `drip-consumer-${process.pid}`;

let isRunning = false;
let consumerTag = null;
let processedCount = 0;
let failedCount = 0;
let startTime = null;

/**
 * Generate unique b_ref for message tracking
 */
const generateBRef = () => {
    const timestamp = Date.now();
    const random = Math.floor(Math.random() * 900000) + 100000;
    return `DM-${timestamp}-${random}`;
};

/**
 * Normalize phone number
 */
const normalizePhone = (phone) => {
    if (!phone) return '';
    return phone.replace(/\D/g, '');
};

/**
 * Process a single message from the queue
 */
const processMessage = async (msgData) => {
    console.log('[MessageConsumer] Processing message:', {
        scheduledMessageId: msgData.scheduledMessageId,
        to: msgData.toNumber,
        scheduledAt: msgData.scheduledAt
    });

    try {
        // Rate limiting
        if (RATE_LIMIT_MS > 0) {
            await new Promise(resolve => setTimeout(resolve, RATE_LIMIT_MS));
        }

        // Check if contact is still valid
        const [contactErr, contact] = await to(
            dbReader('contacts')
                .where('id', msgData.contactId)
                .whereNull('deleted_at')
                .first()
        );

        if (contactErr || !contact) {
            console.log('[MessageConsumer] Contact not found:', msgData.contactId);
            await scheduledMessageService.markMessageAsFailed(
                msgData.scheduledMessageId,
                'Contact not found or deleted'
            );
            return { success: false, error: 'Contact not found' };
        }

        // Check if opted out or blocked
        if (contact.opted_out || contact.is_block) {
            console.log('[MessageConsumer] Contact opted out or blocked:', msgData.contactId);
            await scheduledMessageService.markMessageAsFailed(
                msgData.scheduledMessageId,
                'Contact opted out or blocked'
            );
            return { success: false, error: 'Contact opted out' };
        }

        // Get user for Twilio credentials
        const [userErr, user] = await to(
            dbReader('users')
                .select('id', 'twilio_sid', 'twilio_token', 'messaging_status')
                .where('id', msgData.userId)
                .first()
        );

        if (userErr || !user) {
            console.log('[MessageConsumer] User not found:', msgData.userId);
            await scheduledMessageService.markMessageAsFailed(
                msgData.scheduledMessageId,
                'User not found'
            );
            return { success: false, error: 'User not found' };
        }

        if (user.messaging_status !== 1) {
            console.log('[MessageConsumer] User messaging disabled:', msgData.userId);
            await scheduledMessageService.markMessageAsFailed(
                msgData.scheduledMessageId,
                'User messaging disabled'
            );
            return { success: false, error: 'Messaging disabled' };
        }

        // ====== CREDITS CHECK AND DEDUCTION ======
        console.log('[MessageConsumer] Checking credits for user:', msgData.userId);

        const hasCredits = await creditsService.hasEnoughCredits(msgData.userId, SMS_CREDIT_COST);
        if (!hasCredits) {
            console.log('[MessageConsumer] Insufficient credits for user:', msgData.userId);
            await scheduledMessageService.markMessageAsFailed(
                msgData.scheduledMessageId,
                'Insufficient credits'
            );

            // Update drip_contact status to FAILED
            if (msgData.dripContactId) {
                await to(
                    dbWriter('drip_contact')
                        .where('id', msgData.dripContactId)
                        .update({
                            status: 3, // FAILED
                            error_message: 'Insufficient credits',
                            updated_at: new Date()
                        })
                );
            }

            return { success: false, error: 'Insufficient credits' };
        }

        // Deduct credit before sending
        try {
            const deductResult = await creditsService.deductCredits(msgData.userId, SMS_CREDIT_COST, {
                description: `Drip SMS sent to ${msgData.toNumber}`,
                referenceType: 'drip_sms',
                referenceId: msgData.dripId || null,
            });
            console.log('[MessageConsumer] Credit deducted. New balance:', deductResult?.credits?.balance);
        } catch (creditErr) {
            console.log('[MessageConsumer] Failed to deduct credit:', creditErr.message);
            await scheduledMessageService.markMessageAsFailed(
                msgData.scheduledMessageId,
                creditErr.message || 'Failed to deduct credits'
            );

            // Update drip_contact status to FAILED
            if (msgData.dripContactId) {
                await to(
                    dbWriter('drip_contact')
                        .where('id', msgData.dripContactId)
                        .update({
                            status: 3, // FAILED
                            error_message: creditErr.message || 'Failed to deduct credits',
                            updated_at: new Date()
                        })
                );
            }

            return { success: false, error: creditErr.message };
        }
        // ====== END CREDITS CHECK ======

        // Generate tracking IDs
        const bRef = generateBRef();
        const uid = uuidv4();
        const now = new Date();

        // Build status callback URL
        const statusCallbackUrl = CONFIG.TWILIO.STATUS_CALLBACK_URL
            ? `${CONFIG.TWILIO.STATUS_CALLBACK_URL}?b_ref=${bRef}`
            : null;

        // Get Twilio credentials
        const twilioCredentials = user.twilio_sid && user.twilio_token
            ? { accountSid: user.twilio_sid, authToken: user.twilio_token }
            : null;

        console.log('[MessageConsumer] Sending via Twilio:', {
            from: msgData.fromNumber,
            to: msgData.toNumber,
            bRef
        });

        // Send via Twilio
        const twilioResult = await twilioService.sendSMS({
            to: msgData.toNumber,
            from: msgData.fromNumber,
            body: msgData.message,
            mediaUrl: msgData.mediaUrl || null,
            statusCallback: statusCallbackUrl,
            credentials: twilioCredentials
        });

        if (twilioResult.success) {
            console.log('[MessageConsumer] Twilio send success:', {
                sid: twilioResult.sid,
                bRef
            });

            // Create message record
            const messageData = {
                uid: uid,
                sid: msgData.sid,
                from_number: normalizePhone(msgData.fromNumber),
                to_number: normalizePhone(msgData.toNumber),
                message: msgData.message || '',
                media_html: msgData.mediaUrl || null,
                status: '1',
                delivery_status: 'sent',
                msg_id: twilioResult.sid,
                user_id: msgData.userId,
                workspace_id: msgData.workspaceId,
                contact_id: msgData.contactId,
                b_ref: bRef,
                is_read: 1,
                is_drip: 1,
                drip_id: msgData.dripId || null,
                is_charged: 0,
                counter: 0,
                direction: 'outbound',
                intent: 0,
                message_type: msgData.mediaUrl ? 2 : 0,
                created_at: now
            };

            const [insertErr, insertedMessages] = await to(
                dbWriter('messages')
                    .insert(messageData)
                    .returning('*')
            );

            if (insertErr) {
                logger.error('[MessageConsumer] Error creating message record:', insertErr);
            }

            const createdMessage = insertedMessages ? insertedMessages[0] : null;

            // Mark scheduled_message as sent
            await scheduledMessageService.markMessageAsSent(
                msgData.scheduledMessageId,
                createdMessage?.id,
                twilioResult.sid
            );

            // Update drip_contact status to SENT (1)
            if (msgData.dripContactId) {
                await to(
                    dbWriter('drip_contact')
                        .where('id', msgData.dripContactId)
                        .update({
                            status: 1, // SENT
                            sent_at: now,
                            message_id: createdMessage?.id,
                            b_ref: bRef,
                            updated_at: now
                        })
                );
                console.log('[MessageConsumer] Updated drip_contact status:', {
                    dripContactId: msgData.dripContactId,
                    status: 'SENT'
                });
            }

            // Update contact's last_message
            await to(
                dbWriter('contacts')
                    .where('id', msgData.contactId)
                    .update({ last_message: now, updated_at: now })
            );

            return {
                success: true,
                messageId: createdMessage?.id,
                twilioSid: twilioResult.sid
            };

        } else {
            console.log('[MessageConsumer] Twilio send failed:', {
                error: twilioResult.errorMessage || twilioResult.error,
                code: twilioResult.errorCode
            });

            await scheduledMessageService.markMessageAsFailed(
                msgData.scheduledMessageId,
                twilioResult.errorMessage || twilioResult.error || 'Twilio send failed'
            );

            // Update drip_contact status to FAILED (3)
            if (msgData.dripContactId) {
                await to(
                    dbWriter('drip_contact')
                        .where('id', msgData.dripContactId)
                        .update({
                            status: 3, // FAILED
                            error_message: twilioResult.errorMessage || twilioResult.error || 'Twilio send failed',
                            updated_at: new Date()
                        })
                );
            }

            return {
                success: false,
                error: twilioResult.errorMessage || twilioResult.error
            };
        }

    } catch (error) {
        logger.error('[MessageConsumer] processMessage error:', error);
        await scheduledMessageService.markMessageAsFailed(
            msgData.scheduledMessageId,
            error.message
        );

        // Update drip_contact status to FAILED (3)
        if (msgData.dripContactId) {
            await to(
                dbWriter('drip_contact')
                    .where('id', msgData.dripContactId)
                    .update({
                        status: 3, // FAILED
                        error_message: error.message,
                        updated_at: new Date()
                    })
            );
        }

        return { success: false, error: error.message };
    }
};

/**
 * Message handler for RabbitMQ consumer
 */
const messageHandler = async (msg) => {
    if (!msg) return;

    const channel = rabbitmq.getChannel();

    try {
        const content = msg.content.toString();
        const msgData = JSON.parse(content);

        console.log('[MessageConsumer] Received message:', msgData.scheduledMessageId);

        const result = await processMessage(msgData);

        // ACK the message
        channel.ack(msg);

        if (result.success) {
            processedCount++;
        } else {
            failedCount++;
        }

        // Log stats periodically
        if ((processedCount + failedCount) % 100 === 0) {
            const runtime = Date.now() - startTime;
            console.log('[MessageConsumer] Stats:', {
                processed: processedCount,
                failed: failedCount,
                runtimeMs: runtime,
                messagesPerSecond: runtime > 0 ? Math.round(((processedCount + failedCount) / runtime) * 1000) : 0
            });
        }

    } catch (parseError) {
        logger.error('[MessageConsumer] Failed to parse message:', parseError);
        channel.nack(msg, false, false);
        failedCount++;
    }
};

/**
 * Start consuming messages
 */
const start = async () => {
    console.log('[MessageConsumer] ========================================');
    console.log('[MessageConsumer] Starting Message Consumer');
    console.log('[MessageConsumer] Prefetch:', PREFETCH_COUNT);
    console.log('[MessageConsumer] Rate Limit:', RATE_LIMIT_MS, 'ms');
    console.log('[MessageConsumer] ========================================');

    // Connect to RabbitMQ if not connected
    if (!rabbitmq.isConnected()) {
        console.log('[MessageConsumer] Connecting to RabbitMQ...');
        await rabbitmq.connect();
    }

    if (isRunning) {
        console.log('[MessageConsumer] Consumer already running');
        return;
    }

    try {
        const channel = rabbitmq.getChannel();

        // Set prefetch
        await channel.prefetch(PREFETCH_COUNT);

        // Start consuming
        const result = await channel.consume(
            rabbitmq.QUEUES.DRIP_MESSAGES,
            messageHandler,
            {
                consumerTag: CONSUMER_TAG,
                noAck: false
            }
        );

        consumerTag = result.consumerTag;
        isRunning = true;
        startTime = Date.now();
        processedCount = 0;
        failedCount = 0;

        console.log('[MessageConsumer] Consumer started:', {
            consumerTag: consumerTag,
            queue: rabbitmq.QUEUES.DRIP_MESSAGES
        });

    } catch (error) {
        logger.error('[MessageConsumer] Failed to start consumer:', error);
    }
};

/**
 * Stop the consumer
 */
const stop = async () => {
    console.log('[MessageConsumer] Stopping consumer...');

    if (rabbitmq.isConnected() && consumerTag) {
        try {
            const channel = rabbitmq.getChannel();
            await channel.cancel(consumerTag);
        } catch (e) {
            // Ignore
        }
    }

    consumerTag = null;
    isRunning = false;

    // Log final stats
    if (startTime) {
        const runtime = Date.now() - startTime;
        console.log('[MessageConsumer] Final stats:', {
            processed: processedCount,
            failed: failedCount,
            runtimeMs: runtime,
            messagesPerSecond: runtime > 0 ? Math.round(((processedCount + failedCount) / runtime) * 1000) : 0
        });
    }

    console.log('[MessageConsumer] Consumer stopped');
};

/**
 * Get consumer status
 */
const getStatus = () => {
    const runtime = startTime ? Date.now() - startTime : 0;

    return {
        running: isRunning,
        consumerTag: consumerTag,
        rabbitMQConnected: rabbitmq.isConnected(),
        stats: {
            processed: processedCount,
            failed: failedCount,
            runtimeMs: runtime,
            messagesPerSecond: runtime > 0 ? Math.round(((processedCount + failedCount) / runtime) * 1000) : 0
        },
        config: {
            prefetch: PREFETCH_COUNT,
            rateLimitMs: RATE_LIMIT_MS
        }
    };
};

// Export for use in main app
module.exports = {
    start,
    stop,
    getStatus,
    processMessage
};

// If running as standalone script
if (require.main === module) {
    console.log('[MessageConsumer] Running as standalone process');

    // Handle graceful shutdown
    process.on('SIGINT', async () => {
        console.log('\n[MessageConsumer] Received SIGINT, shutting down...');
        await stop();
        await rabbitmq.close();
        process.exit(0);
    });

    process.on('SIGTERM', async () => {
        console.log('\n[MessageConsumer] Received SIGTERM, shutting down...');
        await stop();
        await rabbitmq.close();
        process.exit(0);
    });

    // Start the consumer
    start();
}
