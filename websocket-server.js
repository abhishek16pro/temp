import http from "http";
import dotenv from "dotenv";
import { fileURLToPath } from "url";

dotenv.config({ path: fileURLToPath(new URL("./.env", import.meta.url)) });

const startWebSocketServer = async () => {
  try {
    const [
      { default: Socket },
      { config },
      { default: connectDB },
    ] = await Promise.all([
      import("./websocket.js"),
      import("./utils/sharedImport.js"),
      import("./utils/connectMongo.js"),
    ]);

    console.log("Starting WebSocket Server...");
    await connectDB();
    console.log("Connected to MongoDB");

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
          console.log(" HTTP server closed");
        });

        setTimeout(() => {
          console.log(" Graceful shutdown completed");
          process.exit(0);
        }, 5000);
      } catch (error) {
        console.error("❌ Error during graceful shutdown:", error);
        process.exit(1);
      }
    };

    process.on("SIGTERM", () => shutdown("SIGTERM"));
    process.on("SIGINT", () => shutdown("SIGINT"));
  } catch (error) {
    console.error("Failed to start WebSocket server:", error);
    process.exit(1);
  }
};

startWebSocketServer();
