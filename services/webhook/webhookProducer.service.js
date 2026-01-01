/**
 * Webhook Producer Service for Workers
 * Queues webhook events for async processing
 *
 * @module services/webhook/webhookProducer.service
 */

const { v4: uuidv4 } = require('uuid');
const { dbReader, dbWriter } = require('../../config/database');
const rabbitmq = require('../../config/rabbitmq');
const { to } = require('../util.service');
const { logger } = require('../logger.service');

/**
 * Get webhooks subscribed to a specific event
 * @param {number} userId - User ID
 * @param {number} workspaceId - Workspace ID
 * @param {string} eventType - Event type
 * @returns {Promise<Array>} Array of webhooks
 */
const getWebhooksForEvent = async (userId, workspaceId, eventType) => {
    const [err, webhooks] = await to(
        dbReader('webhooks')
            .where({ user_id: userId, workspace_id: workspaceId, status: 'active' })
            .whereRaw('? = ANY(events)', [eventType])
    );

    if (err) {
        console.error('[WebhookProducer] Error fetching webhooks:', err.message);
        return [];
    }

    return webhooks || [];
};

/**
 * Create a webhook delivery record
 * @param {Object} data - Delivery data
 * @returns {Promise<Object|null>} Created delivery or null
 */
const createDelivery = async (data) => {
    const [err, delivery] = await to(
        dbWriter('webhook_deliveries')
            .insert({
                webhook_id: data.webhook_id,
                user_id: data.user_id,
                workspace_id: data.workspace_id,
                event_id: data.event_id,
                event_type: data.event_type,
                payload: data.payload,
                status: 'pending',
                created_at: new Date()
            })
            .returning('*')
    );

    if (err) {
        console.error('[WebhookProducer] Error creating delivery:', err.message);
        return null;
    }

    return delivery[0];
};

/**
 * Queue a webhook event for processing
 * @param {Object} eventData - Event data
 * @returns {Promise<{queued: number, failed: number}>}
 */
const queueWebhookEvent = async (eventData) => {
    const { userId, workspaceId, eventType, payload } = eventData;

    try {
        // Find all active webhooks subscribed to this event
        const webhooks = await getWebhooksForEvent(userId, workspaceId, eventType);

        if (!webhooks || webhooks.length === 0) {
            return { queued: 0, failed: 0 };
        }

        let queued = 0;
        let failed = 0;

        for (const webhook of webhooks) {
            try {
                const eventId = `evt_${uuidv4()}`;

                // Create delivery record
                const delivery = await createDelivery({
                    webhook_id: webhook.id,
                    user_id: userId,
                    workspace_id: workspaceId,
                    event_id: eventId,
                    event_type: eventType,
                    payload: payload
                });

                if (!delivery) {
                    failed++;
                    continue;
                }

                // Queue for processing
                const success = await rabbitmq.publish(rabbitmq.ROUTING_KEYS.WEBHOOK, {
                    type: 'WEBHOOK_DISPATCH',
                    deliveryId: delivery.id,
                    webhookId: webhook.id,
                    url: webhook.url,
                    secret: webhook.secret,
                    eventId: eventId,
                    eventType: eventType,
                    payload: payload,
                    timestamp: Date.now()
                });

                if (success) {
                    queued++;
                    console.log('[WebhookProducer] Event queued:', { eventId, eventType, webhookId: webhook.id });
                } else {
                    failed++;
                }

            } catch (error) {
                failed++;
                console.error('[WebhookProducer] Error queueing webhook:', error.message);
            }
        }

        return { queued, failed };

    } catch (error) {
        console.error('[WebhookProducer] Error in queueWebhookEvent:', error.message);
        return { queued: 0, failed: 0 };
    }
};

/**
 * Queue inbound message webhook event
 */
const queueInboundMessageEvent = async (data) => {
    const { userId, workspaceId, message, contact } = data;

    return await queueWebhookEvent({
        userId,
        workspaceId,
        eventType: 'message.inbound',
        payload: {
            message_id: message.id,
            from: message.from_number,
            to: message.to_number,
            body: message.message,
            media_url: message.media_html || null,
            received_at: message.created_at,
            contact: contact ? {
                id: contact.id,
                name: contact.name,
                phone: contact.phone
            } : null
        }
    });
};

/**
 * Queue outbound message webhook event
 * Accepts either a message object or individual fields
 */
const queueOutboundMessageEvent = async (data) => {
    const { userId, workspaceId, message, contact } = data;

    // Support both message object and individual fields
    const payload = message ? {
        message_id: message.id,
        from: message.from_number,
        to: message.to_number,
        body: message.message,
        twilio_sid: message.msg_id,
        status: message.delivery_status || 'sent',
        sent_at: message.created_at,
        contact_id: contact?.id || message.contact_id
    } : {
        message_id: data.messageId,
        from: data.fromNumber,
        to: data.toNumber,
        body: data.message,
        twilio_sid: data.twilioSid,
        status: 'sent',
        sent_at: new Date().toISOString(),
        contact_id: data.contactId
    };

    return await queueWebhookEvent({
        userId,
        workspaceId,
        eventType: 'message.outbound',
        payload
    });
};

/**
 * Queue message delivered webhook event
 */
const queueMessageDeliveredEvent = async (data) => {
    const { userId, workspaceId, messageId, contactId, status } = data;

    return await queueWebhookEvent({
        userId,
        workspaceId,
        eventType: 'message.delivered',
        payload: {
            message_id: messageId,
            contact_id: contactId,
            status: status,
            delivered_at: new Date().toISOString()
        }
    });
};

/**
 * Queue message failed webhook event
 */
const queueMessageFailedEvent = async (data) => {
    const { userId, workspaceId, messageId, contactId, errorCode, errorMessage } = data;

    return await queueWebhookEvent({
        userId,
        workspaceId,
        eventType: 'message.failed',
        payload: {
            message_id: messageId,
            contact_id: contactId,
            error_code: errorCode,
            error_message: errorMessage,
            failed_at: new Date().toISOString()
        }
    });
};

module.exports = {
    queueWebhookEvent,
    queueInboundMessageEvent,
    queueOutboundMessageEvent,
    queueMessageDeliveredEvent,
    queueMessageFailedEvent
};
