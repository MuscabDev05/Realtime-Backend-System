/**
 * Rate Limiter Middleware
 * 
 * Implements rate limiting for WebSocket connections
 */

const rateLimit = require('express-rate-limit');
const RedisStore = require('rate-limit-redis');
const redisClient = require('../services/redis.service');

// General API rate limiter
const apiLimiter = rateLimit({
  store: new RedisStore({
    sendCommand: (...args) => redisClient.client.sendCommand(args),
    prefix: 'rl:api:'
  }),
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // Limit each IP to 100 requests per windowMs
  message: 'Too many requests from this IP, please try again later.',
  standardHeaders: true,
  legacyHeaders: false,
});

// Strict limiter for auth endpoints
const authLimiter = rateLimit({
  store: new RedisStore({
    sendCommand: (...args) => redisClient.client.sendCommand(args),
    prefix: 'rl:auth:'
  }),
  windowMs: 60 * 60 * 1000, // 1 hour
  max: 5, // Limit each IP to 5 failed requests per hour
  skipSuccessfulRequests: true, // Don't count successful requests
  message: 'Too many failed attempts, please try again later.',
  standardHeaders: true,
  legacyHeaders: false,
});

// WebSocket connection limiter
const wsConnectionLimiter = rateLimit({
  store: new RedisStore({
    sendCommand: (...args) => redisClient.client.sendCommand(args),
    prefix: 'rl:ws:'
  }),
  windowMs: 60 * 1000, // 1 minute
  max: 10, // Limit each IP to 10 connection attempts per minute
  message: 'Too many connection attempts, please slow down.',
  standardHeaders: true,
  legacyHeaders: false,
});

module.exports = {
  apiLimiter,
  authLimiter,
  wsConnectionLimiter
};