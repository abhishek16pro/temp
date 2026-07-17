import express from "express";
import dotenv from "dotenv";
import mongoose from "mongoose";

dotenv.config();
const app = express();
app.use(express.json());

app.get('/', (req, res) => {
  const status = {
      code: 200,
      msg: 'OMS Server Up'
  }
  res.status(200).send(JSON.stringify(status));
})


const PORT = process.env.PORT || 6001;

function logRuntimeConfiguration(config, getConnectionDetails) {
  const redisConnectionDetails = getConnectionDetails();

  console.log("Starting XTS Copy Trade Execution Server...");
  console.log("Runtime configuration:");
  console.log(`  Environment: ${config.server.env}`);
  console.log(`  Mode: ${config.server.isDev ? "local" : "prod"}`);
  console.log(`  Log Level: ${config.logging.level}`);
  console.log(`  HTTP Server: ${config.server.host}:${PORT}`);
  console.log("MongoDB configuration:");
  console.log(`  Host: ${config.database.mongoHost}`);
  console.log(`  Port: ${config.database.mongoPort}`);
  console.log(`  Database: ${config.database.mongoDbName}`);
  console.log(`  Auth Enabled: ${config.database.authEnabled ? "yes" : "no"}`);
  console.log("Redis configuration:");
  console.log(`  Host: ${redisConnectionDetails.host}`);
  console.log(`  Port: ${redisConnectionDetails.port}`);
  console.log(`  Username: ${redisConnectionDetails.username || "not set"}`);
  console.log(`  Password: ${redisConnectionDetails.password ? "***" : "not set"}`);
  console.log(`  Auth Enabled: ${config.redis.authEnabled ? "yes" : "no"}`);
}

async function startServer() {
  try {
    const [
      { default: connectDb },
      { config, getConnectionDetails }
    ] = await Promise.all([
      import("./utils/connectDb.js"),
      import("./utils/sharedImport.js")
    ]);

    logRuntimeConfiguration(config, getConnectionDetails);
    console.log("Connecting to database...");
    await connectDb();
    console.log("Database connected successfully");
    console.log("Connected to MongoDB at:", mongoose.connection.host);

    // Start the server only once, after a successful DB connection
    app.listen(PORT, async () => {
      console.log(`Server running at http://localhost:${PORT} at ${new Date().toLocaleString()}`);
    });
  } catch (error) {
    console.error("Error starting server:", error);
    process.exit(1);
  }
}

// Start the server
startServer();
