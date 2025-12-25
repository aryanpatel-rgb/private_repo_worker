/**
 * Message Service for sengine-workers
 * Simplified version focused on sending messages (no web API operations)
 *
 * @module services/message/message.service
 */

const { to, TE } = require('../util.service');
const { dbReader, dbWriter } = require('../../config/database');
const { logger } = require('../logger.service');
const { v4: uuidv4 } = require('uuid');
const twilioService = require('../sms/twilio.service');
const optoutService = require('./optout.service');
const CONFIG = require('../../config/config');

/**
 * Generate unique b_ref for message tracking
 */
const generateBRef = () => {
    const timestamp = Date.now();
    const random = Math.floor(Math.random() * 900000) + 100000;
    return `DM-${timestamp}-${random}`;
};

/**
 * Normalize phone number for comparison
 */
const normalizePhone = (phone) => {
    if (!phone) return '';
    return phone.replace(/\D/g, '');
};

/**
 * Send a message to a contact (worker version - direct send)
 *
 * @param {Object} params - Message parameters
 * @returns {Promise<Object>} Send result
 */
const sendMessage = async ({ userId, workspaceId, contactId, sid, message, mediaUrl, isDrip = false, dripId = null, dripContactId = null }) => {
    console.log('[Message:Worker] sendMessage:', {
        userId,
        contactId,
        sid,
        messageLength: message?.length,
        isDrip
    });

    try {
        // Validate
        if (!message && !mediaUrl) {
            TE('Message or media is required', true);
        }

        // Get contact
        const [contactErr, contact] = await to(
            dbReader('contacts')
                .select('id', 'name', 'phone', 'opted_out', 'is_block')
                .where({ id: contactId, user_id: userId, workspace_id: workspaceId })
                .whereNull('deleted_at')
                .first()
        );

        if (contactErr || !contact) {
            TE('Contact not found', true);
        }

        // Check if contact is blocked or opted out
        if (contact.is_block === 1) {
            TE('Cannot send message to blocked contact', true);
        }

        if (contact.opted_out === 1) {
            TE('Cannot send message to opted out contact', true);
        }

        // Check opt-out list
        const isOptedOut = await optoutService.checkOptOut(contact.phone, userId);
        if (isOptedOut) {
            TE('Cannot send message to opted out contact', true);
        }

        // Get user number (From number)
        const [numberErr, userNumber] = await to(
            dbReader('user_numbers')
                .select('id', 'phone', 'status')
                .where({ id: sid, user_id: userId })
                .whereNull('deleted_at')
                .first()
        );

        if (numberErr || !userNumber) {
            TE('Sender phone number not found', true);
        }

        if (userNumber.status !== 1 && userNumber.status !== 4) {
            TE('Sender phone number is not active', true);
        }

        // Check global block
        const normalizedPhone = normalizePhone(contact.phone);
        const [blockErr, blockedNumber] = await to(
            dbReader('global_block_numbers')
                .where(function() {
                    this.where('phone', contact.phone)
                        .orWhere('phone', normalizedPhone)
                        .orWhere('phone', '+' + normalizedPhone);
                })
                .first()
        );

        if (blockedNumber) {
            TE('This phone number is blocked', true);
        }

        // Get user for Twilio credentials
        const [userErr, user] = await to(
            dbReader('users')
                .select('id', 'twilio_sid', 'twilio_token', 'messaging_status')
                .where({ id: userId })
                .first()
        );

        if (userErr || !user) {
            TE('User not found', true);
        }

        if (user.messaging_status !== 1) {
            TE('Messaging is not enabled for this account', true);
        }

        // Generate tracking info
        const bRef = generateBRef();
        const uid = uuidv4();
        const now = new Date();

        // Create message record
        const messageData = {
            uid: uid,
            sid: sid,
            from_number: normalizePhone(userNumber.phone),
            to_number: normalizePhone(contact.phone),
            message: message || '',
            media_html: mediaUrl || null,
            status: '0',
            delivery_status: 'pending',
            user_id: userId,
            workspace_id: workspaceId,
            contact_id: contactId,
            b_ref: bRef,
            is_read: 1,
            is_drip: isDrip ? 1 : 0,
            drip_id: dripId || null,
            drip_contact_id: dripContactId || null,
            is_charged: 0,
            counter: 0,
            intent: 0,
            message_type: mediaUrl ? 2 : 0,
            created_at: now
        };

        const [insertErr, insertedMessages] = await to(
            dbWriter('messages')
                .insert(messageData)
                .returning('*')
        );

        if (insertErr) {
            logger.error('[Message:Worker] Error creating message record:', insertErr);
            TE('Failed to create message', true);
        }

        const createdMessage = insertedMessages[0];
        console.log('[Message:Worker] Message record created:', { id: createdMessage.id, bRef });

        // Build status callback URL
        const statusCallbackUrl = CONFIG.TWILIO.STATUS_CALLBACK_URL
            ? `${CONFIG.TWILIO.STATUS_CALLBACK_URL}?b_ref=${bRef}`
            : null;

        // Get Twilio credentials
        const twilioCredentials = user.twilio_sid && user.twilio_token
            ? { accountSid: user.twilio_sid, authToken: user.twilio_token }
            : null;

        // Send via Twilio
        console.log('[Message:Worker] Sending via Twilio:', {
            from: userNumber.phone,
            to: contact.phone
        });

        const twilioResult = await twilioService.sendSMS({
            from: userNumber.phone,
            to: contact.phone,
            body: message,
            mediaUrl: mediaUrl,
            statusCallback: statusCallbackUrl,
            credentials: twilioCredentials
        });

        // Update message with Twilio response
        const updateData = {
            msg_id: twilioResult.sid,
            status: twilioResult.success ? '1' : '3',
            delivery_status: twilioResult.success ? twilioResult.status : 'failed',
            response: JSON.stringify(twilioResult),
            updated_at: new Date()
        };

        await to(
            dbWriter('messages')
                .where({ id: createdMessage.id })
                .update(updateData)
        );

        // Update contact
        await to(
            dbWriter('contacts')
                .where({ id: contactId })
                .update({
                    last_message: now,
                    open_chat: 1,
                    archive: 0,
                    updated_at: now
                })
        );

        // Update contact_phone
        await to(
            dbWriter('contact_phone')
                .insert({
                    contact_id: contactId,
                    user_id: userId,
                    sid: sid,
                    last_message: now,
                    open_chat: 1,
                    archive: 0,
                    updated_at: now
                })
                .onConflict(['contact_id', 'sid'])
                .merge({
                    last_message: now,
                    open_chat: 1,
                    archive: 0,
                    updated_at: now
                })
        );

        console.log('[Message:Worker] Message sent successfully:', {
            id: createdMessage.id,
            bRef: bRef,
            twilioSid: twilioResult.sid
        });

        return {
            success: twilioResult.success,
            message: {
                ...createdMessage,
                ...updateData,
                direction: 'outbound'
            },
            twilioResponse: twilioResult
        };

    } catch (error) {
        logger.error('[Message:Worker] sendMessage error:', error);
        throw error;
    }
};

module.exports = {
    sendMessage,
    generateBRef,
    normalizePhone
};
