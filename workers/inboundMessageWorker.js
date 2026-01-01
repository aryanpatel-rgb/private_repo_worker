/**
 * Inbound Message Worker for sengine-workers
 * Processes incoming SMS/MMS messages from Twilio webhooks
 *
 * This worker:
 * - Consumes messages from INBOUND_MESSAGE queue
 * - Finds or creates contacts
 * - Saves messages to database
 * - Processes opt-out/opt-in keywords
 * - Publishes WebSocket notifications via Redis pub/sub
 *
 * @module workers/inboundMessageWorker
 */

const path = require('path');

// Load environment variables
require('dotenv').config({ path: path.join(__dirname, '../.env') });

const { logger } = require('../services/logger.service');
const rabbitmq = require('../config/rabbitmq');
const { dbReader, dbWriter } = require('../config/database');
const { to } = require('../services/util.service');
const optoutService = require('../services/message/optout.service');
const webhookProducer = require('../services/webhook/webhookProducer.service');
const CONFIG = require('../config/config');

let isRunning = false;

// Opt-out keywords
const OPT_OUT_KEYWORDS = ['stop', 'unsubscribe', 'cancel', 'end', 'quit', 'stopall'];
const OPT_IN_KEYWORDS = ['start', 'unstop', 'subscribe', 'yes'];

/**
 * Normalize phone number
 */
const normalizePhone = (phone) => {
    if (!phone) return '';
    return phone.replace(/\D/g, '');
};

/**
 * Check if message is opt-out
 */
const isOptOutMessage = (message) => {
    if (!message) return false;
    const normalized = message.trim().toLowerCase();
    return OPT_OUT_KEYWORDS.includes(normalized);
};

/**
 * Check if message is opt-in
 */
const isOptInMessage = (message) => {
    if (!message) return false;
    const normalized = message.trim().toLowerCase();
    return OPT_IN_KEYWORDS.includes(normalized);
};

/**
 * Find user by phone number (the number that received the message)
 */
const findUserByNumber = async (toNumber) => {
    const normalizedTo = normalizePhone(toNumber);

    const [err, userNumber] = await to(
        dbReader('user_numbers')
            .select('user_id', 'id as sid', 'phone')
            .where(function() {
                this.where('phone', toNumber)
                    .orWhere('phone', normalizedTo)
                    .orWhere('phone', '+1' + normalizedTo)
                    .orWhere('phone', '1' + normalizedTo);
            })
            .whereNull('deleted_at')
            .first()
    );

    if (err || !userNumber) {
        console.log('[InboundMessageWorker] User number not found:', toNumber);
        return null;
    }

    // Get user details
    const [userErr, user] = await to(
        dbReader('users')
            .select('id', 'default_workspace')
            .where({ id: userNumber.user_id })
            .first()
    );

    if (userErr || !user) {
        return null;
    }

    return {
        userId: user.id,
        workspaceId: user.default_workspace || 1,
        sid: userNumber.sid,
        userPhone: userNumber.phone
    };
};

/**
 * Find or create contact by phone number
 */
const findOrCreateContact = async (fromNumber, userId, workspaceId) => {
    const normalizedFrom = normalizePhone(fromNumber);

    // Try to find existing contact
    const [findErr, existingContact] = await to(
        dbReader('contacts')
            .select('*')
            .where({ user_id: userId, workspace_id: workspaceId })
            .where(function() {
                this.where('phone', fromNumber)
                    .orWhere('phone', normalizedFrom)
                    .orWhere('phone', '+1' + normalizedFrom)
                    .orWhere('phone', '1' + normalizedFrom);
            })
            .whereNull('deleted_at')
            .first()
    );

    if (existingContact) {
        return { contact: existingContact, isNew: false };
    }

    // Create new contact
    const now = new Date();
    const contactData = {
        name: fromNumber,
        phone: normalizedFrom.length === 10 ? '+1' + normalizedFrom : '+' + normalizedFrom,
        user_id: userId,
        workspace_id: workspaceId,
        source: 'inbound',
        status: 1,
        opted_out: 0,
        is_block: 0,
        open_chat: 1,
        archive: 0,
        last_message: now,
        created_at: now,
        updated_at: now
    };

    const [insertErr, inserted] = await to(
        dbWriter('contacts')
            .insert(contactData)
            .returning('*')
    );

    if (insertErr) {
        console.error('[InboundMessageWorker] Error creating contact:', insertErr);
        return null;
    }

    console.log('[InboundMessageWorker] Created new contact:', inserted[0].id);
    return { contact: inserted[0], isNew: true };
};

/**
 * Process opt-out for contact
 */
const processOptOut = async (contact, userId) => {
    console.log('[InboundMessageWorker] Processing opt-out for contact:', contact.id);

    // Update contact
    await to(
        dbWriter('contacts')
            .where({ id: contact.id })
            .update({
                opted_out: 1,
                updated_at: new Date()
            })
    );

    // Add to opt-out list
    await optoutService.addOptOut(contact.phone, userId);

    return true;
};

/**
 * Process opt-in for contact
 */
const processOptIn = async (contact, userId) => {
    console.log('[InboundMessageWorker] Processing opt-in for contact:', contact.id);

    // Update contact
    await to(
        dbWriter('contacts')
            .where({ id: contact.id })
            .update({
                opted_out: 0,
                updated_at: new Date()
            })
    );

    // Remove from opt-out list
    await optoutService.removeOptOut(contact.phone, userId);

    return true;
};

/**
 * Save inbound message to database
 */
const saveInboundMessage = async (data, user, contact) => {
    const now = new Date();

    const messageData = {
        uid: data.messageSid || require('uuid').v4(),
        msg_id: data.messageSid,
        sid: user.sid,
        from_number: normalizePhone(data.from),
        to_number: normalizePhone(data.to),
        message: data.body || '',
        media_html: data.mediaUrl || null,
        status: '2', // Received
        delivery_status: 'received',
        user_id: user.userId,
        workspace_id: user.workspaceId,
        contact_id: contact.id,
        is_read: 0, // Unread
        is_drip: 0,
        message_type: data.numMedia > 0 ? 3 : 1, // 3 = MMS inbound, 1 = SMS inbound
        created_at: now,
        updated_at: now
    };

    const [insertErr, inserted] = await to(
        dbWriter('messages')
            .insert(messageData)
            .returning('*')
    );

    if (insertErr) {
        console.error('[InboundMessageWorker] Error saving message:', insertErr);
        throw insertErr;
    }

    // Update contact's last_message timestamp
    await to(
        dbWriter('contacts')
            .where({ id: contact.id })
            .update({
                last_message: now,
                open_chat: 1,
                archive: 0,
                updated_at: now
            })
    );

    return inserted[0];
};

/**
 * Publish notification to Redis for WebSocket delivery
 * (The API server subscribes to this and sends via WebSocket)
 */
const publishNotification = async (channel, data) => {
    // For now, we'll publish to RabbitMQ notification queue
    // The API server's WebSocket handler will consume and broadcast
    try {
        await rabbitmq.publish('notify', {
            type: 'WEBSOCKET_NOTIFICATION',
            channel: channel,
            data: data,
            timestamp: Date.now()
        });
        console.log('[InboundMessageWorker] Published notification:', channel);
    } catch (error) {
        console.error('[InboundMessageWorker] Failed to publish notification:', error.message);
    }
};

/**
 * Handle a single inbound message
 */
const handleInboundMessage = async (payload) => {
    const { data, retryCount = 0 } = payload;

    console.log('[InboundMessageWorker] Processing inbound message:', {
        messageSid: data.messageSid,
        from: data.from,
        to: data.to,
        body: data.body?.substring(0, 50) + '...',
        retryCount
    });

    try {
        // Find user by the number that received the message
        const user = await findUserByNumber(data.to);
        if (!user) {
            console.warn('[InboundMessageWorker] No user found for number:', data.to);
            return { success: false, error: 'User not found' };
        }

        // Find or create contact
        const contactResult = await findOrCreateContact(data.from, user.userId, user.workspaceId);
        if (!contactResult) {
            console.error('[InboundMessageWorker] Failed to find/create contact');
            return { success: false, error: 'Contact error' };
        }

        const { contact, isNew } = contactResult;

        // Check for opt-out/opt-in
        let isOptOut = false;
        let isOptIn = false;

        if (isOptOutMessage(data.body)) {
            isOptOut = true;
            await processOptOut(contact, user.userId);
        } else if (isOptInMessage(data.body)) {
            isOptIn = true;
            await processOptIn(contact, user.userId);
        }

        // Save message to database
        const savedMessage = await saveInboundMessage(data, user, contact);

        console.log('[InboundMessageWorker] Message saved:', {
            messageId: savedMessage.id,
            contactId: contact.id,
            isOptOut,
            isOptIn,
            isNewContact: isNew
        });

        // Trigger webhook for inbound message (non-blocking)
        webhookProducer.queueInboundMessageEvent({
            userId: user.userId,
            workspaceId: user.workspaceId,
            message: savedMessage,
            contact: contact
        }).then(result => {
            if (result.queued > 0) {
                console.log('[InboundMessageWorker] Webhook events queued:', result.queued);
            }
        }).catch(err => {
            console.error('[InboundMessageWorker] Webhook error:', err.message);
        });

        // Publish notifications for WebSocket delivery
        // The API server will receive these and send via Socket.IO
        await publishNotification('message:new', {
            userId: user.userId,
            message: {
                ...savedMessage,
                direction: 'inbound',
                contact_name: contact.name,
                contact_phone: contact.phone
            },
            contact: contact,
            isOptOut,
            isOptIn
        });

        // Get and publish unread count
        const [countErr, unreadResult] = await to(
            dbReader('messages')
                .count('id as count')
                .where({
                    user_id: user.userId,
                    workspace_id: user.workspaceId,
                    is_read: 0
                })
                .whereIn('message_type', [1, 3]) // Inbound messages
                .first()
        );

        const unreadCount = unreadResult?.count || 0;
        await publishNotification('unread:update', {
            userId: user.userId,
            count: parseInt(unreadCount)
        });

        // Publish opt-out/opt-in notifications
        if (isOptOut) {
            await publishNotification('contact:optout', {
                userId: user.userId,
                contactId: contact.id,
                phone: contact.phone
            });
        }

        if (isOptIn) {
            await publishNotification('contact:optin', {
                userId: user.userId,
                contactId: contact.id,
                phone: contact.phone
            });
        }

        return {
            success: true,
            message: savedMessage,
            contact,
            isOptOut,
            isOptIn
        };

    } catch (error) {
        console.error('[InboundMessageWorker] Error processing message:', error);
        logger.error('[InboundMessageWorker] Error:', error);
        throw error;
    }
};

/**
 * Start consuming messages from RabbitMQ
 */
const startConsumer = async () => {
    try {
        console.log('[InboundMessageWorker] Starting consumer...');

        await rabbitmq.consume(rabbitmq.QUEUES.INBOUND_MESSAGE, async (payload, msg) => {
            await handleInboundMessage(payload);
        });

        console.log('[InboundMessageWorker] Consumer started');
        isRunning = true;

    } catch (error) {
        logger.error('[InboundMessageWorker] Failed to start consumer:', error);
        throw error;
    }
};

/**
 * Start the inbound worker
 */
const start = async () => {
    console.log('[InboundMessageWorker] ========================================');
    console.log('[InboundMessageWorker] Starting Inbound Message Worker');
    console.log('[InboundMessageWorker] ========================================');

    try {
        // Connect to RabbitMQ if not already connected
        if (!rabbitmq.isConnected()) {
            await rabbitmq.connect();
        }

        // Start consuming
        await startConsumer();

        console.log('[InboundMessageWorker] Worker started successfully');

    } catch (error) {
        logger.error('[InboundMessageWorker] Failed to start:', error);
        throw error;
    }
};

/**
 * Stop the inbound worker
 */
const stop = async () => {
    console.log('[InboundMessageWorker] Stopping worker...');
    isRunning = false;
    console.log('[InboundMessageWorker] Worker stopped');
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
    handleInboundMessage
};

// If running as standalone script
if (require.main === module) {
    console.log('[InboundMessageWorker] Running as standalone process');

    // Handle graceful shutdown
    process.on('SIGINT', async () => {
        console.log('\n[InboundMessageWorker] Received SIGINT, shutting down...');
        await stop();
        process.exit(0);
    });

    process.on('SIGTERM', async () => {
        console.log('\n[InboundMessageWorker] Received SIGTERM, shutting down...');
        await stop();
        process.exit(0);
    });

    // Start the worker
    rabbitmq.connect().then(() => {
        start().catch((error) => {
            console.error('[InboundMessageWorker] Fatal error:', error);
            process.exit(1);
        });
    });
}
