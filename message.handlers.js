const { v4: uuidv4 } = require('uuid');
const Message = require('../../models/Message.model');
const redisClient = require('../../services/redis.service'); // Fixed: single import
const logger = require('../../utils/logger');

class MessageHandler {
    constructor(wsServer) {
        this.wsServer = wsServer;
    } 
    
    async handleMessage(ws, message) {
        try {
            const { roomId, content, type = 'text', metadata = {} } = message.payload; // Fixed: added metadata with default

            // Validate message
            if (!roomId || !content) {
                this.wsServer.sendError(ws, 'Room ID and content are required'); // Fixed: changed from wServer to wsServer
                return;
            }

            // Create message object
            const newMessage = {
                id: uuidv4(),
                roomId,
                senderId: ws.userId,
                senderName: ws.username,
                content,
                type,
                metadata, // Now defined
                timestamp: new Date().toISOString(),
                readBy: [ws.userId] // Sender has read it
            };

            // Save to database (async, don't wait) - Fixed: removed duplicate
            this.saveMessageToDatabase(newMessage).catch(error => {
                logger.error('Failed to save message to database:', error);
            });

            // Cache in Redis for quick access
            await this.cacheMessage(newMessage);

            // Send confirmation to sender
            this.wsServer.sendToClient(ws, {
                type: 'message:sent',
                payload: {
                    messageId: newMessage.id,
                    timestamp: newMessage.timestamp,
                    message: 'Message sent successfully'
                }
            });

            // Broadcast to room (including sender for their own UI)
            this.wsServer.broadcastToRoom(roomId, {
                type: 'message:new',
                payload: newMessage
            });

            // Publish to Redis for other servers
            await redisClient.publish('messages:broadcast', JSON.stringify({
                roomId,
                message: newMessage,
                serverId: process.env.SERVER_ID || 'default'
            }));

            logger.info(`Message ${newMessage.id} sent by ${ws.username} to room ${roomId}`);

        } catch (error) {
            logger.error('Error sending message:', error);
            this.wsServer.sendError(ws, 'Failed to send message');
        }
    }

    /**
     * Handle message history request
     * @param {WebSocket} ws - WebSocket connection
     * @param {Object} message - Request message
     */
    async handleGetHistory(ws, message) {
        try {
            const { roomId, limit = 50, before = new Date().toISOString() } = message.payload;

            if (!roomId) {
                this.wsServer.sendError(ws, 'Room ID is required');
                return;
            }

            // Try to get from cache first
            const cachedMessages = await this.getCachedMessages(roomId, limit);
            
            if (cachedMessages && cachedMessages.length > 0) {
                this.wsServer.sendToClient(ws, {
                    type: 'message:history',
                    payload: {
                        roomId,
                        messages: cachedMessages,
                        source: 'cache',
                        hasMore: cachedMessages.length === limit
                    }
                });
                return;
            }

            // If not in cache, get from database
            const messages = await Message.find({
                roomId,
                timestamp: { $lt: before }
            })
            .sort({ timestamp: -1 })
            .limit(limit)
            .lean();

            this.wsServer.sendToClient(ws, {
                type: 'message:history',
                payload: {
                    roomId,
                    messages: messages.reverse(), // Return in chronological order
                    source: 'database',
                    hasMore: messages.length === limit
                }
            });

            // Cache messages for future requests
            await this.cacheMessages(roomId, messages);

        } catch (error) {
            logger.error('Error getting message history:', error);
            this.wsServer.sendError(ws, 'Failed to retrieve message history');
        }
    }

    /**
     * Handle read receipt
     * @param {WebSocket} ws - WebSocket connection
     * @param {Object} message - Read receipt message
     */
    async handleReadReceipt(ws, message) {
        try {
            const { messageId, roomId } = message.payload;

            if (!messageId || !roomId) {
                this.wsServer.sendError(ws, 'Message ID and Room ID are required');
                return;
            }

            // Update message in database
            await Message.updateOne(
                { id: messageId },
                { $addToSet: { readBy: ws.userId } }
            );

            // Update in cache if present
            await this.updateMessageReadStatus(messageId, ws.userId);

            // Notify others in the room
            this.wsServer.broadcastToRoom(roomId, {
                type: 'message:read',
                payload: {
                    messageId,
                    userId: ws.userId,
                    username: ws.username,
                    timestamp: new Date().toISOString()
                }
            }, [ws.clientId]); // Exclude the reader

        } catch (error) {
            logger.error('Error handling read receipt:', error);
        }
    }

    /**
     * Save message to database
     * @param {Object} message - Message to save
     */
    async saveMessageToDatabase(message) {
        try {
            const messageDoc = new Message(message);
            await messageDoc.save();
        } catch (error) {
            logger.error('Database save error:', error);
            throw error;
        }
    }

    /**
     * Cache message in Redis
     * @param {Object} message - Message to cache
     */
    async cacheMessage(message) {
        try {
            const key = `room:messages:${message.roomId}`;
            await redisClient.lPush(key, JSON.stringify(message));
            await redisClient.lTrim(key, 0, 99); // Keep last 100 messages
            await redisClient.expire(key, 3600); // Expire after 1 hour
        } catch (error) {
            logger.error('Redis cache error:', error);
        }
    }

    /**
     * Cache multiple messages
     * @param {string} roomId - Room identifier
     * @param {Array} messages - Messages to cache
     */
    async cacheMessages(roomId, messages) {
        try {
            const key = `room:messages:${roomId}`;
            const pipeline = redisClient.multi();
            
            // Use rPush to maintain chronological order
            messages.forEach(message => {
                pipeline.rPush(key, JSON.stringify(message));
            });
            
            pipeline.lTrim(key, -100, -1); // Keep last 100 messages
            pipeline.expire(key, 3600);
            
            await pipeline.exec();
        } catch (error) {
            logger.error('Redis cache error:', error);
        }
    }

    /**
     * Get cached messages for a room
     * @param {string} roomId - Room identifier
     * @param {number} limit - Maximum number of messages
     * @returns {Array|null} - Cached messages or null
     */
    async getCachedMessages(roomId, limit) {
        try {
            const key = `room:messages:${roomId}`;
            // Use lRange with negative indices to get newest messages
            const messages = await redisClient.lRange(key, -limit, -1);
            
            if (messages && messages.length > 0) {
                return messages.map(msg => JSON.parse(msg));
            }
            
            return null;
        } catch (error) {
            logger.error('Redis get error:', error);
            return null;
        }
    }

    /**
     * Update message read status in cache
     * @param {string} messageId - Message identifier
     * @param {string} userId - User who read the message
     */
    async updateMessageReadStatus(messageId, userId) {
        // Implementation would update cached message
        // This is simplified for the example
    }
}

module.exports = MessageHandler;