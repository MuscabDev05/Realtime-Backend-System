/**
 * Real-time Chat Client
 * 
 * Demonstrates WebSocket connection management and real-time features
 */

class ChatClient {
    constructor() {
        this.ws = null;
        this.username = null;
        this.userId = null;
        this.token = null;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 5;
        this.reconnectDelay = 1000;
        this.typingTimeout = null;
        this.currentRoom = 'general';
        
        this.initializeElements();
        this.bindEvents();
    }

    initializeElements() {
        this.loginSection = document.getElementById('loginSection');
        this.chatSection = document.getElementById('chatSection');
        this.usernameInput = document.getElementById('username');
        this.messageInput = document.getElementById('messageInput');
        this.messagesContainer = document.getElementById('messages');
        this.connectionStatus = document.getElementById('connectionStatus');
        this.typingIndicator = document.getElementById('typingIndicator');
        this.onlineCount = document.getElementById('onlineCount');
        this.sendButton = document.getElementById('sendButton');
    }

    bindEvents() {
        // Handle Enter key in message input
        this.messageInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                this.sendMessage();
            }
        });

        // Handle typing indicator
        this.messageInput.addEventListener('input', () => {
            this.handleTyping();
        });

        // Handle Enter key in username input
        this.usernameInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                this.login();
            }
        });
    }

    async login() {
        const username = this.usernameInput.value.trim();
        
        if (username.length < 3) {
            alert('Username must be at least 3 characters long');
            return;
        }

        try {
            // Authenticate with REST API
            const response = await fetch('/api/auth/login', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ username })
            });

            if (!response.ok) {
                throw new Error('Authentication failed');
            }

            const data = await response.json();
            this.token = data.token;
            this.username = data.user.username;
            this.userId = data.user.id;

            // Connect WebSocket
            this.connectWebSocket();

        } catch (error) {
            console.error('Login failed:', error);
            alert('Failed to login. Please try again.');
        }
    }

    connectWebSocket() {
        // Create WebSocket connection with authentication token
        const wsUrl = `ws://${window.location.host}/ws?token=${this.token}`;
        this.ws = new WebSocket(wsUrl);

        this.ws.onopen = () => {
            console.log('WebSocket connected');
            this.updateConnectionStatus('Connected', 'online');
            this.loginSection.classList.add('hidden');
            this.chatSection.classList.remove('hidden');
            this.reconnectAttempts = 0;
            
            // Join default room
            this.joinRoom(this.currentRoom);
            
            // Subscribe to presence updates
            this.sendMessage({
                type: 'presence:subscribe',
                payload: { userIds: [] }
            });
        };

        this.ws.onmessage = (event) => {
            const message = JSON.parse(event.data);
            this.handleIncomingMessage(message);
        };

        this.ws.onclose = () => {
            console.log('WebSocket disconnected');
            this.updateConnectionStatus('Disconnected', 'offline');
            this.handleReconnect();
        };

        this.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
            this.updateConnectionStatus('Error', 'error');
        };
    }

    handleIncomingMessage(message) {
        console.log('Received:', message);

        switch (message.type) {
            case 'connection_established':
                this.showSystemMessage(`Connected as ${message.payload.username}`);
                break;

            case 'message:new':
                this.displayMessage(message.payload);
                break;

            case 'message:sent':
                // Message sent confirmation
                break;

            case 'message:history':
                this.displayMessageHistory(message.payload);
                break;

            case 'presence:update':
                this.updateUserPresence(message.payload);
                break;

            case 'typing:start':
                this.showTypingIndicator(message.payload);
                break;

            case 'typing:stop':
                this.hideTypingIndicator(message.payload);
                break;

            case 'room:joined':
                this.showSystemMessage(`Joined room: ${message.payload.roomId}`);
                // Request message history
                this.requestHistory(message.payload.roomId);
                break;

            case 'room:user_joined':
                this.showSystemMessage(`${message.payload.username} joined the room`);
                break;

            case 'room:user_left':
                this.showSystemMessage(`${message.payload.username} left the room`);
                break;

            case 'error':
                this.showErrorMessage(message.payload.error);
                break;

            default:
                console.log('Unknown message type:', message.type);
        }
    }

    sendMessage(content = null) {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
            alert('Not connected to server');
            return;
        }

        let messageContent = content;
        if (!messageContent) {
            messageContent = this.messageInput.value.trim();
            if (!messageContent) return;
        }

        const message = {
            type: 'message:send',
            payload: {
                roomId: this.currentRoom,
                content: messageContent,
                type: 'text'
            }
        };

        this.ws.send(JSON.stringify(message));
        this.messageInput.value = '';
    }

    joinRoom(roomId) {
        const message = {
            type: 'room:join',
            payload: { roomId }
        };
        this.ws.send(JSON.stringify(message));
    }

    requestHistory(roomId) {
        const message = {
            type: 'message:history',
            payload: {
                roomId,
                limit: 50
            }
        };
        this.ws.send(JSON.stringify(message));
    }

    handleTyping() {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;

        // Clear existing timeout
        if (this.typingTimeout) {
            clearTimeout(this.typingTimeout);
        }

        // Send typing start
        this.ws.send(JSON.stringify({
            type: 'typing:start',
            payload: {
                roomId: this.currentRoom,
                isTyping: true
            }
        }));

        // Set timeout to stop typing
        this.typingTimeout = setTimeout(() => {
            this.ws.send(JSON.stringify({
                type: 'typing:stop',
                payload: {
                    roomId: this.currentRoom,
                    isTyping: false
                }
            }));
        }, 3000);
    }

    displayMessage(message) {
        const messageElement = document.createElement('div');
        messageElement.className = `message ${message.senderId === this.userId ? 'own' : 'other'}`;
        
        const time = new Date(message.timestamp).toLocaleTimeString();
        
        messageElement.innerHTML = `
            ${message.senderId !== this.userId ? `<div class="sender">${message.senderName}</div>` : ''}
            <div class="content">${this.escapeHtml(message.content)}</div>
            <div class="time">${time}</div>
        `;

        this.messagesContainer.appendChild(messageElement);
        this.messagesContainer.scrollTop = this.messagesContainer.scrollHeight;
    }

    displayMessageHistory(data) {
        this.messagesContainer.innerHTML = '';
        data.messages.forEach(message => this.displayMessage(message));
    }

    showTypingIndicator(data) {
        if (data.userId === this.userId) return;
        
        this.typingIndicator.textContent = `${data.username} is typing...`;
        this.typingIndicator.classList.remove('hidden');

        // Clear after 3 seconds if no update
        setTimeout(() => {
            this.hideTypingIndicator(data);
        }, 3000);
    }

    hideTypingIndicator(data) {
        this.typingIndicator.classList.add('hidden');
    }

    showSystemMessage(text) {
        const messageElement = document.createElement('div');
        messageElement.style.cssText = `
            text-align: center;
            color: #666;
            font-size: 12px;
            margin: 10px 0;
        `;
        messageElement.textContent = text;
        this.messagesContainer.appendChild(messageElement);
    }

    showErrorMessage(text) {
        const messageElement = document.createElement('div');
        messageElement.style.cssText = `
            text-align: center;
            color: #dc3545;
            font-size: 12px;
            margin: 10px 0;
        `;
        messageElement.textContent = `Error: ${text}`;
        this.messagesContainer.appendChild(messageElement);
    }

    updateUserPresence(data) {
        // Update online count
        const count = parseInt(this.onlineCount.textContent) || 0;
        this.onlineCount.textContent = data.status === 'online' ? count + 1 : Math.max(0, count - 1);
    }

    updateConnectionStatus(text, status) {
        this.connectionStatus.textContent = text;
        this.connectionStatus.className = `status ${status}`;
    }

    handleReconnect() {
        if (this.reconnectAttempts < this.maxReconnectAttempts) {
            this.reconnectAttempts++;
            const delay = this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1);
            
            console.log(`Reconnecting in ${delay}ms... (Attempt ${this.reconnectAttempts})`);
            this.updateConnectionStatus(`Reconnecting in ${delay/1000}s...`, 'reconnecting');
            
            setTimeout(() => this.connectWebSocket(), delay);
        } else {
            this.updateConnectionStatus('Failed to reconnect', 'error');
            alert('Lost connection to server. Please refresh the page.');
        }
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}

// Initialize chat client when page loads
const client = new ChatClient();

// Make functions available globally for onclick handlers
window.login = () => client.login();
window.sendMessage = () => client.sendMessage();