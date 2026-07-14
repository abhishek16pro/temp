import http from 'http';
import Socket from './websocket.js';
import config from './config/index.js';
import connectDB from './utils/connectMongo.js';

const startWebSocketServer = async () => {
    try {
        console.log('Starting WebSocket Server...');
        await connectDB();
        console.log('Connected to MongoDB');

        const httpServer = http.createServer();
        Socket(httpServer);

        httpServer.listen(config.websocket.port, () => {
            console.log(`
  WebSocket Server started successfully!
  WebSocket Server: ws://localhost:${config.websocket.port}
  Started at: ${new Date().toLocaleString()}
  Environment: ${config.server.env}
      `);
        });

        // Graceful shutdown
        const shutdown = async (signal) => {
            console.log(`\n Received ${signal}. Starting graceful shutdown...`);

            try {
                httpServer.close(() => {
                    console.log(' HTTP server closed');
                });

                setTimeout(() => {
                    console.log(' Graceful shutdown completed');
                    process.exit(0);
                }, 5000);

            } catch (error) {
                console.error('❌ Error during graceful shutdown:', error);
                process.exit(1);
            }
        };

        process.on('SIGTERM', () => shutdown('SIGTERM'));
        process.on('SIGINT', () => shutdown('SIGINT'));
    } catch (error) {
        console.error('Failed to start WebSocket server:', error);
        process.exit(1);
    }
};

startWebSocketServer();