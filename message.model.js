/**
 * Message Model (MongoDB)
 * 
 * Defines the schema for persisted messages
 */

const mongoose = require('mongoose');

const messageSchema = new mongoose.Schema({
  id: {
    type: String,
    required: true,
    unique: true,
    index: true
  },
  roomId: {
    type: String,
    required: true,
    index: true
  },
  senderId: {
    type: String,
    required: true,
    index: true
  },
  senderName: {
    type: String,
    required: true
  },
  content: {
    type: String,
    required: true
  },
  type: {
    type: String,
    enum: ['text', 'image', 'file', 'system'],
    default: 'text'
  },
  metadata: {
    type: Map,
    of: mongoose.Schema.Types.Mixed,
    default: {}
  },
  readBy: [{
    type: String,
    index: true
  }],
  timestamp: {
    type: Date,
    required: true,
    index: true
  },
  editedAt: {
    type: Date
  },
  deletedAt: {
    type: Date
  }
}, {
  timestamps: true // Adds createdAt and updatedAt
});

// Compound indexes for common queries
messageSchema.index({ roomId: 1, timestamp: -1 });
messageSchema.index({ senderId: 1, timestamp: -1 });

// Virtual for checking if message is read by specific user
messageSchema.virtual('isReadBy').get(function() {
  return (userId) => this.readBy.includes(userId);
});

const Message = mongoose.model('Message', messageSchema);

module.exports = Message;