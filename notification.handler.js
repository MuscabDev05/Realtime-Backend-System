/**
 * Notification Handler
 * 
 * Manages real-time notifications including:
 * - Typing indicators
 * - System notifications
 * - Custom alerts
 */

const logger = require('../../utils/logger');

class NotificationHandler {
  constructor(wsServer) {
    this.wsServer = wsServer;
    this.typingTimeouts = new Map(); // Track typing timeouts
  }

  /**
   * Handle typing indicator
   * @param {WebSocket} ws - WebSocket connection
   * @param {Object} message - Typing indicator message
   */
  async handleTypingIndicator(ws, message) {
    const { roomId, isTyping } = message.payload;
    const typingKey = `${roomId}:${ws.userId}`;

    // Clear existing timeout
    if (this.typingTimeouts.has(typingKey)) {
      clearTimeout(this.typingTimeouts.get(typingKey));
      this.typingTimeouts.delete(typingKey);
    }

    // If user started typing, set timeout to auto-stop
    if (isTyping) {
      const timeout = setTimeout(() => {
        this.wsServer.broadcastToRoom(roomId, {
          type: 'typing:stop',
          payload: {
            roomId,
            userId: ws.userId,
            username: ws.username
          }
        }, [ws.clientId]);
        this.typingTimeouts.delete(typingKey);
      }, 3000); // Stop typing after 3 seconds of inactivity

      this.typingTimeouts.set(typingKey, timeout);
    }

    // Broadcast typing status to room (excluding sender)
    this.wsServer.broadcastToRoom(roomId, {
      type: isTyping ? 'typing:start' : 'typing:stop',
      payload: {
        roomId,
        userId: ws.userId,
        username: ws.username
      }
    }, [ws.clientId]);
  }

  /**
   * Send system notification to all connected clients
   * @param {Object} notification - Notification object
   */
  async sendSystemNotification(notification) {
    const message = {
      type: 'notification:system',
      payload: {
        id: require('uuid').v4(),
        ...notification,
        timestamp: new Date().toISOString()
      }
    };

    this.wsServer.wss.clients.forEach(client => {
      if (client.readyState === 1) {
        this.wsServer.sendToClient(client, message);
      }
    });

    // Also publish to Redis for other servers
    await redisClient.publish('notifications:global', JSON.stringify({
      notification: message.payload,
      serverId: process.env.SERVER_ID || 'default'
    }));
  }
}

module.exports = NotificationHandler;