import mongoose from "mongoose";
import { config } from "./sharedImport.js";

let isConnected = false;
let listenersRegistered = false;

async function connectDB() {
  if (isConnected || mongoose.connection.readyState === 1) {
    isConnected = true;
    return;
  }

  const {
    mongoUri,
    safeMongoUri,
    mongoHost,
    mongoPort,
    mongoDbName,
    authEnabled,
    mongoUsername,
    mongoPassword,
  } = config.database;

  if (authEnabled && (!mongoUsername || !mongoPassword)) {
    throw new Error("MongoDB production auth is enabled but MONGO_USERNAME or MONGO_PASSWORD is missing.");
  }

  const options = {
    connectTimeoutMS: 30000,
    socketTimeoutMS: 30000,
    serverSelectionTimeoutMS: 30000,
  };

  try {
    console.log(`MongoDB connecting (${config.server.env}) to ${mongoHost}:${mongoPort}/${mongoDbName}`);
    console.log(`MongoDB auth enabled: ${authEnabled ? "yes" : "no"}`);
    console.log(`MongoDB URI: ${safeMongoUri}`);

    await mongoose.connect(mongoUri, options);
    isConnected = mongoose.connection.readyState === 1;
    console.log(`MongoDB connected: ${isConnected}`);

    if (!listenersRegistered) {
      listenersRegistered = true;

      mongoose.connection.on("error", (err) => {
        console.error("Mongoose connection error:", err.message);
      });

      mongoose.connection.on("disconnected", async () => {
        isConnected = false;
        console.warn("Mongoose connection lost. Attempting to reconnect...");
        try {
          await mongoose.connect(mongoUri, options);
          isConnected = mongoose.connection.readyState === 1;
          console.log(`Reconnected to MongoDB at ${mongoHost}:${mongoPort}/${mongoDbName}`);
        } catch (err) {
          console.error("Failed to reconnect to MongoDB:", err.message);
        }
      });
    }
  } catch (error) {
    isConnected = false;
    console.error("Error connecting to database:", error.message);
    throw error;
  }
}

export default connectDB;
