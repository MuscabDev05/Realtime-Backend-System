const redis = require('redis')
const logger = require('../utils/logger');

class RedisService {
  constructor() {
    this.client = null;
    this.isConnected = false;
  }

  /**
   * Create Redis client with retry strategy
   */
  createClient() {
    return redis.createClient({
      url: process.env.REDIS_URL || 'redis://localhost:6379',
      socket: {
        reconnectStrategy: (retries) => {
          if (retries > 10) {
            logger.error('Redis max retries reached');
            return new Error('Redis max retries reached');
          }
          return Math.min(retries * 100, 3000);
        },
        keepAlive: 5000
      },
      password: process.env.REDIS_PASSWORD
    });
  }

  /**
   * Connect to Redis
   */
  async connect() {
    if (this.isConnected) {
      return this.client;
    }

    try {
      this.client = this.createClient();

      this.client.on('error', (error) => {
        logger.error('Redis Client Error:', error);
        this.isConnected = false;
      });

      this.client.on('connect', () => {
        logger.info('Redis Client Connected');
        this.isConnected = true;
      });

      this.client.on('end', () => {
        logger.warn('Redis Client Disconnected');
        this.isConnected = false;
      });

      await this.client.connect();
      return this.client;

    } catch (error) {
      logger.error('Redis connection failed:', error);
      throw error;
    }
  }

  /**
   * Get Redis client (ensures connection)
   */
  async getClient() {
    if (!this.isConnected || !this.client) {
      await this.connect();
    }
    return this.client;
  }

  /**
   * Set cache with expiration
   */
  async set(key, value, ttlSeconds = 3600) {
    const client = await this.getClient();
    await client.set(key, JSON.stringify(value), {
      EX: ttlSeconds
    });
  }

  /**
   * Get cached value
   */
  async get(key) {
    const client = await this.getClient();
    const value = await client.get(key);
    return value ? JSON.parse(value) : null;
  }

  /**
   * Delete cache key
   */
  async del(key) {
    const client = await this.getClient();
    await client.del(key);
  }

  /**
   * Publish message to channel
   */
  async publish(channel, message) {
    const client = await this.getClient();
    await client.publish(channel, message);
  }

  /**
   * Subscribe to channel
   */
  async subscribe(channel, callback) {
    const client = await this.getClient();
    const subscriber = client.duplicate();
    await subscriber.connect();
    
    await subscriber.subscribe(channel, (message) => {
      try {
        callback(message);
      } catch (error) {
        logger.error('Redis subscription callback error:', error);
      }
    });

    return subscriber;
  }

  /**
   * Add to sorted set (for presence tracking)
   */
  async addToSortedSet(key, score, member) {
    const client = await this.getClient();
    await client.zAdd(key, { score, value: member });
  }

  /**
   * Get range from sorted set
   */
  async getRangeFromSortedSet(key, min, max) {
    const client = await this.getClient();
    return await client.zRangeByScore(key, min, max);
  }

  /**
   * Close Redis connection
   */
  async quit() {
    if (this.client && this.isConnected) {
      await this.client.quit();
      this.isConnected = false;
      logger.info('Redis connection closed');
    }
  }
}

// Export singleton instance
module.exports = new RedisService();