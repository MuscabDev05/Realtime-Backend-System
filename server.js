require('dotenv').config();
const http = require('http');
const app = require('./app');
const WebSocketServer = require('./websocket/websocket.server');
const logger = require('./utils/logger');
const { connectDB } = require('./config/database');
const redisClient = require('./services/redis.service');

// Initialize configuration from environment variables
const PORT = process.env.PORT || 3000;
const WS_PORT = process.env.WS_PORT || 3001;
const NODE_ENV = process.env.NODE_ENV || 'development';

/**
 * Create HTTP server for REST API and WebSocket upgrade handling
 * This setup allows both HTTP and WebSocket traffic on the same server
 */
const server = http.createServer(app);

/**
 * Initialize WebSocket Server
 * The WebSocket server attaches to the HTTP server and handles upgrade requests
 * This enables real-time bidirectional communication
 */
const wsServer = new WebSocketServer(server);

/**
 * Graceful shutdown handler
 * Ensures all connections are properly closed before shutting down
 */
const gracefulShutdown = async () => {
  logger.info('Received shutdown signal. Starting graceful shutdown...');
  
  // Set timeout to force shutdown if graceful shutdown takes too long
  const forceShutdown = setTimeout(() => {
    logger.error('Forced shutdown due to timeout');
    process.exit(1);
  }, 10000);

  try {
    // Close WebSocket connections
    await wsServer.shutdown();
    logger.info('WebSocket server closed');

    // Close HTTP server
    await new Promise((resolve) => server.close(resolve));
    logger.info('HTTP server closed');

    // Close database connections
    await mongoose.connection.close();
    logger.info('Database connection closed');

    // Close Redis connection
    await redisClient.quit();
    logger.info('Redis connection closed');

    clearTimeout(forceShutdown);
    logger.info('Graceful shutdown completed');
    process.exit(0);
  } catch (error) {
    logger.error('Error during graceful shutdown:', error);
    process.exit(1);
  }
};

/**
 * Start the server
 * This function initializes all connections and starts listening for requests
 */
const startServer = async () => {
  try {
    // Connect to MongoDB
    await connectDB();
    logger.info('Database connected successfully');

    // Connect to Redis for pub/sub and caching
    await redisClient.connect();
    logger.info('Redis connected successfully');

    // Start listening for HTTP and WebSocket connections
    server.listen(PORT, () => {
      logger.info(`
        🚀 Server is running in ${NODE_ENV} mode
        📡 HTTP server: http://localhost:${PORT}
        🔌 WebSocket server: ws://localhost:${PORT}
        📊 WebSocket ping-pong enabled for connection health
      `);
    });

    // Handle various process signals for graceful shutdown
    process.on('SIGTERM', gracefulShutdown);
    process.on('SIGINT', gracefulShutdown);
    process.on('uncaughtException', (error) => {
      logger.error('Uncaught Exception:', error);
      gracefulShutdown();
    });
    process.on('unhandledRejection', (error) => {
      logger.error('Unhandled Rejection:', error);
      gracefulShutdown();
    });

  } catch (error) {
    logger.error('Failed to start server:', error);
    process.exit(1);
  }
};

// Initialize the server
startServer();

module.exports = server; // For testing purposes