/**
 * Logger Utility
 * 
 * Production-grade logging with Winston
 */

const winston = require('winston');
const { combine, timestamp, printf, colorize, json } = winston.format;

// Custom log format for development
const devFormat = printf(({ level, message, timestamp, ...metadata }) => {
  let msg = `${timestamp} [${level}]: ${message}`;
  if (Object.keys(metadata).length > 0) {
    msg += ` ${JSON.stringify(metadata)}`;
  }
  return msg;
});

// Determine log level based on environment
const level = process.env.LOG_LEVEL || (process.env.NODE_ENV === 'production' ? 'info' : 'debug');

// Create logger instance
const logger = winston.createLogger({
  level,
  format: combine(
    timestamp(),
    process.env.NODE_ENV === 'production' ? json() : combine(colorize(), devFormat)
  ),
  transports: [
    // Write all logs to console
    new winston.transports.Console({
      handleExceptions: true
    }),
    // Write all error logs to file
    new winston.transports.File({ 
      filename: 'logs/error.log', 
      level: 'error',
      maxsize: 5242880, // 5MB
      maxFiles: 5
    }),
    // Write all logs to combined file
    new winston.transports.File({ 
      filename: 'logs/combined.log',
      maxsize: 5242880, // 5MB
      maxFiles: 5
    })
  ],
  exitOnError: false
});

// Stream for Morgan HTTP logging
logger.stream = {
  write: (message) => logger.info(message.trim())
};

module.exports = logger;