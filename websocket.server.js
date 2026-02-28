const WebSocket = require('ws');
const jwt = require('jsonwebtoken');
const { v4: uuidv4 } = require('uuid');
const redisClient = require('./config/redis');
const MessageHandler = require('./handlers/message.handler');
const PresenceHandler = require('./handlers/presence.handler');
const NotificationHandler = require('./handlers/notification.handler');
const logger = require('../utils/logger');

class WebSocketServer {
  constructor(server) {
    this.wss = null;
    this.clients = new Map(); // Store connected clients with their metadata
    this.rooms = new Map(); // Store room subscriptions
    this.messageHandler = new MessageHandler(this);
    this.presenceHandler = new PresenceHandler(this);
    this.notificationHandler = new NotificationHandler(this);
    
    this.initialize(server);
  }

  initialize(server) {
    this.wss = new WebSocket.Server({
        server,
        path: '/ws',
        clientTracking: true, 
        maxPayload: 1024 * 1024, // Maximum message size: 1MB
    })
  }

  initialize(server) {
    // Create WebSocket server with production settings
    this.wss = new WebSocket.Server({
      server,
      path: '/ws', // WebSocket endpoint path
      clientTracking: true, // Track connected clients
      maxPayload: 1024 * 1024, // Maximum message size: 1MB
      perMessageDeflate: { // Compression settings
        zlibDeflateOptions: {
          chunkSize: 1024,
          memLevel: 7,
          level: 3
        },
        zlibInflateOptions: {
          chunkSize: 10 * 1024
        },
        clientNoContextTakeover: true, // Better compression
        serverNoContextTakeover: true,
        serverMaxWindowBits: 10,
        concurrencyLimit: 10,
        threshold: 1024 // Only compress messages > 1KB
      }
    });

    // Set up connection handler
    this.wss.on('connection', this.handleConnection.bind(this));
    
    // Set up error handler
    this.wss.on('error', this.handleError.bind(this));

    // Set up Redis pub/sub for multi-server scaling
    this.setupRedisPubSub();

    logger.info('WebSocket server initialized with path: /ws');
  }

  /**
   * Handle new WebSocket connections
   * This is where authentication and client setup happens
   * 
   * @param {WebSocket} ws - WebSocket connection instance
   * @param {http.IncomingMessage} req - HTTP upgrade request
   */
  async handleConnection(ws, req) {
    try {
      // Generate unique connection ID
      const connectionId = uuidv4();
      
      // Extract token from query parameters or headers
      const token = this.extractToken(req);
      
      if (!token) {
        this.closeConnection(ws, 4001, 'Authentication required');
        return;
      }

      // Authenticate the user
      const user = await this.authenticateToken(token);
      
      if (!user) {
        this.closeConnection(ws, 4002, 'Invalid authentication token');
        return;
      }

      // Store client information
      const clientInfo = {
        id: connectionId,
        userId: user.userId,
        username: user.username,
        role: user.role,
        connectedAt: new Date().toISOString(),
        lastPing: Date.now(),
        ws: ws,
        subscriptions: new Set() // Rooms/channels this client subscribes to
      };

      this.clients.set(connectionId, clientInfo);
      
      // Attach client info to WebSocket instance for easy access
      ws.clientId = connectionId;
      ws.userId = user.userId;
      ws.username = user.username;

      // Set up message handler for this connection
      ws.on('message', (data) => this.handleMessage(ws, data));
      
      // Set up close handler
      ws.on('close', () => this.handleDisconnect(connectionId));
      
      // Set up error handler for this connection
      ws.on('error', (error) => this.handleClientError(connectionId, error));
      
      // Set up ping-pong for connection health
      ws.isAlive = true;
      ws.on('pong', () => {
        ws.isAlive = true;
        clientInfo.lastPing = Date.now();
      });

      // Send welcome message
      this.sendToClient(ws, {
        type: 'connection_established',
        payload: {
          connectionId,
          userId: user.userId,
          username: user.username,
          message: 'Connected to real-time server successfully',
          timestamp: new Date().toISOString()
        }
      });

      // Broadcast user presence to all connected clients
      this.presenceHandler.broadcastPresence(user.userId, 'online');

      // Publish to Redis for multi-server presence sync
      await redisClient.publish('presence:updates', JSON.stringify({
        userId: user.userId,
        status: 'online',
        timestamp: new Date().toISOString(),
        serverId: process.env.SERVER_ID || 'default'
      }));

      logger.info(`Client ${connectionId} (${user.username}) connected`);

    } catch (error) {
      logger.error('WebSocket connection error:', error);
      this.closeConnection(ws, 4000, 'Internal server error');
    }
  }

  /**
   * Handle incoming WebSocket messages
   * Routes messages to appropriate handlers based on type
   * 
   * @param {WebSocket} ws - WebSocket connection
   * @param {Buffer|string} data - Message data
   */
  async handleMessage(ws, data) {
    try {
      const clientInfo = this.clients.get(ws.clientId);
      
      if (!clientInfo) {
        logger.warn('Message from unknown client');
        return;
      }

      // Parse message (handle both string and buffer)
      const message = typeof data === 'string' ? JSON.parse(data) : JSON.parse(data.toString());
      
      // Validate message structure
      if (!message.type || !message.payload) {
        this.sendError(ws, 'Invalid message format');
        return;
      }

      // Add metadata to message
      message.metadata = {
        clientId: ws.clientId,
        userId: ws.userId,
        timestamp: new Date().toISOString(),
        messageId: uuidv4()
      };

      logger.debug(`Received message from ${ws.username}:`, message.type);

      // Route message to appropriate handler
      switch (message.type) {
        case 'message:send':
          await this.messageHandler.handleSendMessage(ws, message);
          break;
          
        case 'message:history':
          await this.messageHandler.handleGetHistory(ws, message);
          break;
          
        case 'presence:subscribe':
          await this.presenceHandler.handleSubscribe(ws, message);
          break;
          
        case 'presence:status':
          await this.presenceHandler.handleStatusUpdate(ws, message);
          break;
          
        case 'room:join':
          await this.handleJoinRoom(ws, message);
          break;
          
        case 'room:leave':
          await this.handleLeaveRoom(ws, message);
          break;
          
        case 'typing:start':
        case 'typing:stop':
          await this.notificationHandler.handleTypingIndicator(ws, message);
          break;
          
        case 'ping':
          this.handlePing(ws);
          break;
          
        default:
          logger.warn(`Unknown message type: ${message.type}`);
          this.sendError(ws, `Unknown message type: ${message.type}`);
      }

    } catch (error) {
      logger.error('Error handling message:', error);
      this.sendError(ws, 'Failed to process message');
    }
  }

  /**
   * Handle client disconnection
   * @param {string} clientId - ID of disconnected client
   */
  async handleDisconnect(clientId) {
    const clientInfo = this.clients.get(clientId);
    
    if (clientInfo) {
      // Remove from all rooms
      clientInfo.subscriptions.forEach(room => {
        this.leaveRoom(clientId, room);
      });

      // Remove from clients map
      this.clients.delete(clientId);

      // Broadcast offline status
      this.presenceHandler.broadcastPresence(clientInfo.userId, 'offline');

      // Publish to Redis for multi-server presence sync
      await redisClient.publish('presence:updates', JSON.stringify({
        userId: clientInfo.userId,
        status: 'offline',
        timestamp: new Date().toISOString(),
        serverId: process.env.SERVER_ID || 'default'
      }));

      logger.info(`Client ${clientId} (${clientInfo.username}) disconnected`);
    }
  }

  /**
   * Handle client joining a room
   * @param {WebSocket} ws - WebSocket connection
   * @param {Object} message - Join room message
   */
  async handleJoinRoom(ws, message) {
    const { roomId } = message.payload;
    const clientId = ws.clientId;
    
    if (!roomId) {
      this.sendError(ws, 'Room ID required');
      return;
    }

    // Initialize room if it doesn't exist
    if (!this.rooms.has(roomId)) {
      this.rooms.set(roomId, new Set());
    }

    // Add client to room
    const room = this.rooms.get(roomId);
    room.add(clientId);

    // Update client's subscriptions
    const clientInfo = this.clients.get(clientId);
    clientInfo.subscriptions.add(roomId);

    // Send confirmation
    this.sendToClient(ws, {
      type: 'room:joined',
      payload: {
        roomId,
        message: `Successfully joined room ${roomId}`,
        timestamp: new Date().toISOString()
      }
    });

    // Notify room members
    this.broadcastToRoom(roomId, {
      type: 'room:user_joined',
      payload: {
        roomId,
        userId: ws.userId,
        username: ws.username,
        timestamp: new Date().toISOString()
      }
    }, [clientId]); // Exclude the joining user

    logger.info(`${ws.username} joined room ${roomId}`);
  }

  /**
   * Handle client leaving a room
   * @param {WebSocket} ws - WebSocket connection
   * @param {Object} message - Leave room message
   */
  async handleLeaveRoom(ws, message) {
    const { roomId } = message.payload;
    const clientId = ws.clientId;

    this.leaveRoom(clientId, roomId);

    // Notify room members
    this.broadcastToRoom(roomId, {
      type: 'room:user_left',
      payload: {
        roomId,
        userId: ws.userId,
        username: ws.username,
        timestamp: new Date().toISOString()
      }
    });

    this.sendToClient(ws, {
      type: 'room:left',
      payload: {
        roomId,
        message: `Successfully left room ${roomId}`,
        timestamp: new Date().toISOString()
      }
    });
  }

  /**
   * Remove client from a room
   * @param {string} clientId - Client ID
   * @param {string} roomId - Room ID
   */
  leaveRoom(clientId, roomId) {
    const room = this.rooms.get(roomId);
    if (room) {
      room.delete(clientId);
      
      // Clean up empty rooms
      if (room.size === 0) {
        this.rooms.delete(roomId);
      }
    }

    const clientInfo = this.clients.get(clientId);
    if (clientInfo) {
      clientInfo.subscriptions.delete(roomId);
    }
  }

  /**
   * Handle ping messages for connection health
   * @param {WebSocket} ws - WebSocket connection
   */
  handlePing(ws) {
    this.sendToClient(ws, {
      type: 'pong',
      payload: {
        timestamp: new Date().toISOString()
      }
    });
  }

  /**
   * Broadcast message to all clients in a room
   * @param {string} roomId - Room identifier
   * @param {Object} message - Message to broadcast
   * @param {Array} exclude - Client IDs to exclude
   */
  broadcastToRoom(roomId, message, exclude = []) {
    const room = this.rooms.get(roomId);
    if (!room) return;

    room.forEach(clientId => {
      if (!exclude.includes(clientId)) {
        const client = this.clients.get(clientId);
        if (client && client.ws.readyState === WebSocket.OPEN) {
          this.sendToClient(client.ws, message);
        }
      }
    });
  }

  /**
   * Send message to a specific client
   * @param {WebSocket} ws - WebSocket connection
   * @param {Object} message - Message to send
   */
  sendToClient(ws, message) {
    if (ws.readyState === WebSocket.OPEN) {
      try {
        const messageStr = JSON.stringify(message);
        ws.send(messageStr);
      } catch (error) {
        logger.error('Error sending message to client:', error);
      }
    }
  }

  /**
   * Send error message to client
   * @param {WebSocket} ws - WebSocket connection
   * @param {string} error - Error message
   */
  sendError(ws, error) {
    this.sendToClient(ws, {
      type: 'error',
      payload: {
        error,
        timestamp: new Date().toISOString()
      }
    });
  }

  /**
   * Close WebSocket connection with code and reason
   * @param {WebSocket} ws - WebSocket connection
   * @param {number} code - Close code
   * @param {string} reason - Close reason
   */
  closeConnection(ws, code, reason) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.close(code, reason);
    }
  }

  /**
   * Extract JWT token from request
   * @param {http.IncomingMessage} req - HTTP request
   * @returns {string|null} - JWT token or null
   */
  extractToken(req) {
    // Check query parameters
    const url = new URL(req.url, `http://${req.headers.host}`);
    const token = url.searchParams.get('token');
    
    if (token) return token;

    // Check headers
    const authHeader = req.headers.authorization;
    if (authHeader && authHeader.startsWith('Bearer ')) {
      return authHeader.substring(7);
    }

    return null;
  }

  /**
   * Authenticate JWT token
   * @param {string} token - JWT token
   * @returns {Promise<Object|null>} - User object or null
   */
  async authenticateToken(token) {
    try {
      const decoded = jwt.verify(token, process.env.JWT_SECRET || 'your-secret-key-change-in-production', {
        issuer: 'realtime-backend',
        audience: 'realtime-client'
      });

      // In production, verify user still exists in database
      return {
        userId: decoded.userId,
        username: decoded.username,
        role: decoded.role
      };
    } catch (error) {
      logger.error('Token authentication failed:', error.message);
      return null;
    }
  }

  /**
   * Set up Redis Pub/Sub for multi-server communication
   * This enables horizontal scaling by allowing servers to communicate
   */
  async setupRedisPubSub() {
    const pubSubClient = redisClient.duplicate();
    
    await pubSubClient.connect();

    // Subscribe to channels
    await pubSubClient.subscribe('messages:broadcast', (message) => {
      this.handleRedisMessage('messages:broadcast', message);
    });

    await pubSubClient.subscribe('presence:updates', (message) => {
      this.handleRedisMessage('presence:updates', message);
    });

    await pubSubClient.subscribe('notifications:global', (message) => {
      this.handleRedisMessage('notifications:global', message);
    });

    logger.info('Redis Pub/Sub initialized for multi-server communication');
  }

  /**
   * Handle messages from Redis Pub/Sub
   * @param {string} channel - Redis channel
   * @param {string} message - Message payload
   */
  handleRedisMessage(channel, message) {
    try {
      const data = JSON.parse(message);
      
      // Ignore messages from this server
      if (data.serverId === (process.env.SERVER_ID || 'default')) {
        return;
      }

      switch (channel) {
        case 'messages:broadcast':
          this.broadcastToRoom(data.roomId, {
            type: 'message:new',
            payload: data.message
          });
          break;

        case 'presence:updates':
          // Broadcast presence update to all local clients
          this.presenceHandler.broadcastPresence(data.userId, data.status);
          break;

        case 'notifications:global':
          // Broadcast global notification to all local clients
          this.wss.clients.forEach(client => {
            if (client.readyState === WebSocket.OPEN) {
              this.sendToClient(client, {
                type: 'notification:global',
                payload: data.notification
              });
            }
          });
          break;
      }
    } catch (error) {
      logger.error('Error handling Redis message:', error);
    }
  }

  /**
   * Handle WebSocket server errors
   * @param {Error} error - Error object
   */
  handleError(error) {
    logger.error('WebSocket server error:', error);
  }

  /**
   * Handle client connection errors
   * @param {string} clientId - Client ID
   * @param {Error} error - Error object
   */
  handleClientError(clientId, error) {
    logger.error(`Client ${clientId} error:`, error);
  }

  /**
   * Heartbeat mechanism to check connection health
   * This should be called periodically
   */
  heartbeat() {
    this.wss.clients.forEach(ws => {
      if (ws.isAlive === false) {
        logger.warn(`Terminating inactive connection: ${ws.clientId}`);
        return ws.terminate();
      }

      ws.isAlive = false;
      ws.ping();
    });
  }

  /**
   * Graceful shutdown
   */
  async shutdown() {
    logger.info('Shutting down WebSocket server...');

    // Close all client connections
    this.wss.clients.forEach(client => {
      this.closeConnection(client, 1001, 'Server shutting down');
    });

    // Close the WebSocket server
    await new Promise((resolve) => this.wss.close(resolve));

    logger.info('WebSocket server closed');
  }
}

module.exports = WebSocketServer;
