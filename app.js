const express = require('express');
const helmet = require('helmet');
const cors = require('cors');
const compression = require('conpression');
const rateLimit = require('express-rate-limit');
const jwt = require('jsonwebtoken');
const { v4: uuidv4 } = require('uuid');
const logger = require('./utils/logger');
const errorHandler = require('./middleware/errorHandler');

const app = express();

// Security middleware
app.use(helmet({
  contentSecurityPolicy: false, // Disabled for WebSocket connections
  crossOriginEmbedderPolicy: false
}));

// Enable CORS for frontend applications
app.use(cors({
  origin: process.env.CLIENT_URL || 'http://localhost:3000',
  credentials: true
}));

// Compress responses
app.use(compression());

// JSON bodies 
app.use(express.json({ limit: '10mb' }));

// Request logging middleware
app.use((req, res, next) => {
    const requestId = uuidv4();
    req.requestId = requestId;

    logger.info({
        message: 'Incoming request',
        requestId,
        method: req.method,
        url: req.originalUrl,
        ip: req.ip,
        userAgent: req.get('User-Agent')
    });

// Add response finish listener for logging
res.on('finish', () => {
    logger.info({
        message: 'Request completed',
        requestId,
        statusCode: res.statusCode,
        responseTime: Date.now() - req.startTime
    });
});

res.locals.startTime= Date.now();
next();
});

// Rate limiting for API endpoint
const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // limit each IP to 100 requests per windowMs
  message: 'Too many requests from this IP, please try again later.',
    standardHeaders: true,
    legacyHeaders: false,
    keyGenerator: (req) => req.ip || req.connection.remoteAddress
});

// Apply rate limiting to all requests
app.use('/api/', apiLimiter);

app.post('/api/auth/login', (req, res) => {
    try {
        const { username, password } = req.body;

        // Validate username
        if (!username || username.length < 5 || username.length > 20) {
            return res.status(400).json({
                error: 'Username must be between 5 and 20 characters long.'
            });
        }

        // In production, validate against database
        const user = {
            id: uuidv4(),
            username,
            role: 'user',
            createdAt: new Date().toISOString()
        };

        // Generate JWT token for WebSocket authentication
        const token = jwt.sign(
            {
                userId: user.id,
                username: user.username,
                role: user.role
            },
            process.env.JWT_SECRET || 'your-secret-key-in-production',
            {
                expiresIn: '24h',
                issuer: 'realtime-backend',
                audience: 'realtime-clients'
            }
        );

        logger.info('User ${user.username} logged in successfully', { userId: user.id });

        res.json({
            success: true,
            token,
            user,
            message: 'Use this token for WebSocket connection authentication.'
        });

    } catch (error) {
        logger.error('Login error:', error);
        res.status(500).json({ error: 'An error occurred during login. Please try again later.' });
    }
});

app.get('/api/health', (req, res) => {
    res.json({
        status: 'healthy',
        uptime: process.uptime(),
        timestamp: new Date().toISOString(),
        environment: process.env.NODE_ENV || 'development',
        websockets: 'enabled',
        redis: 'connected' // Check Redis connection
    });
});

app.get('/api/status', (req, res) => {
  res.json({
    service: 'Real-time Backend System',
    version: '1.0.0',
    features: [
      'WebSocket real-time communication',
      'JWT authentication',
      'Message broadcasting',
      'User presence tracking',
      'Redis pub/sub for scaling'
    ],
    endpoints: {
      rest: '/api/*',
      websocket: 'ws://localhost:3000',
      docs: 'https://github.com/yourusername/realtime-backend'
    }
  });
});

// Serve static files for demo frontend
app.use(express.static('public'));

// Error handling middleware (should be last)
app.use(errorHandler);

module.exports = app;



