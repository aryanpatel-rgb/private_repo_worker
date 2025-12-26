/**
 * Scheduled Message Service
 * High-scale drip message scheduling and delivery
 *
 * @module services/drip/scheduledMessage.service
 */

const { to } = require('../util.service');
const { dbReader, dbWriter } = require('../../config/database');
const { logger } = require('../logger.service');
const CONFIG = require('../../config/config');

// Status constants
const SCHEDULED_MESSAGE_STATUS = {
    PENDING: 0,      // In DB, waiting for queue
    QUEUED: 1,       // Pushed to RabbitMQ
    SENDING: 2,      // Consumer picked up
    SENT: 3,         // Successfully sent
    DELIVERED: 4,    // Delivery confirmed
    FAILED: 5,       // Send failed
    CANCELLED: 6     // Cancelled before send
};

// Configuration from config
const HIGH_SCALE_CONFIG = CONFIG.HIGH_SCALE_DRIP;

/**
 * Get messages ready to be queued (Pre-Queue Worker calls this)
 * Returns messages scheduled within the next X minutes
 */
const getMessagesReadyForQueue = async (minutes = HIGH_SCALE_CONFIG.PRE_QUEUE_MINUTES, limit = HIGH_SCALE_CONFIG.PRE_QUEUE_BATCH) => {
    console.log('[ScheduledMessage] Fetching messages ready for queue...');

    try {
        const now = new Date();
        const futureTime = new Date(now.getTime() + (minutes * 60 * 1000));

        const [err, messages] = await to(
            dbReader('scheduled_messages')
                .where('status', SCHEDULED_MESSAGE_STATUS.PENDING)
                .where('scheduled_at', '<=', futureTime)
                .orderBy('scheduled_at', 'asc')
                .limit(limit)
        );

        if (err) {
            logger.error('[ScheduledMessage] Error fetching messages:', err);
            return [];
        }

        console.log('[ScheduledMessage] Found messages ready for queue:', messages?.length || 0);
        return messages || [];

    } catch (error) {
        logger.error('[ScheduledMessage] getMessagesReadyForQueue error:', error);
        return [];
    }
};

/**
 * Mark messages as queued (after pushing to RabbitMQ)
 */
const markMessagesAsQueued = async (messageIds) => {
    if (!messageIds || messageIds.length === 0) return 0;

    try {
        const [err, result] = await to(
            dbWriter('scheduled_messages')
                .whereIn('id', messageIds)
                .where('status', SCHEDULED_MESSAGE_STATUS.PENDING)
                .update({
                    status: SCHEDULED_MESSAGE_STATUS.QUEUED,
                    queued_at: new Date(),
                    updated_at: new Date()
                })
        );

        if (err) {
            logger.error('[ScheduledMessage] Error marking messages as queued:', err);
            return 0;
        }

        console.log('[ScheduledMessage] Marked messages as queued:', result);
        return result;

    } catch (error) {
        logger.error('[ScheduledMessage] markMessagesAsQueued error:', error);
        return 0;
    }
};

/**
 * Mark a message as sent successfully
 */
const markMessageAsSent = async (scheduledMessageId, messageId, msgId) => {
    try {
        const [err] = await to(
            dbWriter('scheduled_messages')
                .where('id', scheduledMessageId)
                .update({
                    status: SCHEDULED_MESSAGE_STATUS.SENT,
                    sent_at: new Date(),
                    message_id: messageId,
                    msg_id: msgId,
                    updated_at: new Date()
                })
        );

        return !err;

    } catch (error) {
        logger.error('[ScheduledMessage] markMessageAsSent error:', error);
        return false;
    }
};

/**
 * Mark a message as failed
 */
const markMessageAsFailed = async (scheduledMessageId, errorMessage) => {
    try {
        const [err] = await to(
            dbWriter('scheduled_messages')
                .where('id', scheduledMessageId)
                .update({
                    status: SCHEDULED_MESSAGE_STATUS.FAILED,
                    error_message: errorMessage,
                    retry_count: dbWriter.raw('retry_count + 1'),
                    updated_at: new Date()
                })
        );

        return !err;

    } catch (error) {
        logger.error('[ScheduledMessage] markMessageAsFailed error:', error);
        return false;
    }
};

/**
 * Get scheduled message statistics
 */
const getScheduledMessageStats = async (campaignId = null, userId = null) => {
    try {
        let query = dbReader('scheduled_messages')
            .select('status')
            .count('id as count')
            .groupBy('status');

        if (campaignId) {
            query = query.where('campaign_id', campaignId);
        }
        if (userId) {
            query = query.where('user_id', userId);
        }

        const [err, stats] = await to(query);

        if (err) {
            logger.error('[ScheduledMessage] Error getting stats:', err);
            return null;
        }

        const result = {
            pending: 0,
            queued: 0,
            sending: 0,
            sent: 0,
            delivered: 0,
            failed: 0,
            cancelled: 0,
            total: 0
        };

        for (const stat of stats || []) {
            const count = parseInt(stat.count, 10);
            result.total += count;

            switch (stat.status) {
                case SCHEDULED_MESSAGE_STATUS.PENDING: result.pending = count; break;
                case SCHEDULED_MESSAGE_STATUS.QUEUED: result.queued = count; break;
                case SCHEDULED_MESSAGE_STATUS.SENDING: result.sending = count; break;
                case SCHEDULED_MESSAGE_STATUS.SENT: result.sent = count; break;
                case SCHEDULED_MESSAGE_STATUS.DELIVERED: result.delivered = count; break;
                case SCHEDULED_MESSAGE_STATUS.FAILED: result.failed = count; break;
                case SCHEDULED_MESSAGE_STATUS.CANCELLED: result.cancelled = count; break;
            }
        }

        return result;

    } catch (error) {
        logger.error('[ScheduledMessage] getScheduledMessageStats error:', error);
        return null;
    }
};

module.exports = {
    SCHEDULED_MESSAGE_STATUS,
    getMessagesReadyForQueue,
    markMessagesAsQueued,
    markMessageAsSent,
    markMessageAsFailed,
    getScheduledMessageStats
};
