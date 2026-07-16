import { Server } from "socket.io";
import Redis from "ioredis";
// import Redis from "ioredis";
import {
  Client as account,
  Log,
  StgLog,
  Strategy as sSchema,
  Strategy as StrategySchema,
  StgTag,
  redisConnect,
  REDIS_MESSAGES, 
  fetchIndexData,
  config
} from "./utils/sharedImport.js";
import { getClientPositions, getPositions } from "./controllers/positions.js";
import processTrades from "./utils/processTrades.js";
import { saveLog } from "./utils/saveLog.js";
import getStraddle from "./controllers/straddle.js";
import { getSystemStatusInfo } from "./controllers/systemStatus.js";

const client = redisConnect();
const indices = [
  "BANKNIFTY",
  "FINNIFTY",
  "MIDCPNIFTY",
  "NIFTY",
  "SENSEX",
];

const Socket = (socketServer) => {
  const io = new Server(socketServer, config.cors);

  const topicIntervals = new Map();
  const lastPayloadByRoom = new Map();

  function ensureTopicInterval(roomName, intervalMs, fetchFn, eventName) {
    if (topicIntervals.has(roomName)) return;
    const state = { interval: null, running: false };
    const runner = async () => {
      if (state.running) return;
      state.running = true;
      try {
        const data = await fetchFn();
        try {
          const next = JSON.stringify(data);
          const prev = lastPayloadByRoom.get(roomName);
          if (next !== prev) {
            io.to(roomName).emit(eventName, data);
            lastPayloadByRoom.set(roomName, next);
          }
        } catch {
          io.to(roomName).emit(eventName, data);
        }
      } catch (err) {
      } finally {
        state.running = false;
      }
    };
    state.interval = setInterval(runner, intervalMs);
    topicIntervals.set(roomName, state);
  }

  function cleanupEmptyTopicIntervals() {
    for (const [roomName, state] of topicIntervals.entries()) {
      const room = io.sockets.adapter.rooms.get(roomName);
      if (!room || room.size === 0) {
        clearInterval(state.interval);
        topicIntervals.delete(roomName);
        lastPayloadByRoom.delete(roomName);
      }
    }
  }

  function leaveTopicRoom(socket, roomName) {
    socket.leave(roomName);
    cleanupEmptyTopicIntervals();
  }

  io.on("connection", (socket) => {
    console.log(socket.id, "Socket Connection Established");

    socket.on("subscribeStg", async ({ stgName }) => {
      if (!stgName) return;
      const roomName = `stg:${stgName}`;
      socket.join(roomName);
      ensureTopicInterval(
        roomName,
        1000,
        () => getLiveStgLogs(stgName),
        "getStgLog",
      );
      try {
        socket.emit("getStgLog", await getLiveStgLogs(stgName));
      } catch {}
      cleanupEmptyTopicIntervals();
    });

    socket.on("unsubscribeStg", ({ stgName }) => {
      if (stgName) {
        leaveTopicRoom(socket, `stg:${stgName}`);
      }
    });

    socket.on("subscribeLogs", () => {
      const roomName = "logs";
      socket.join(roomName);
      ensureTopicInterval(roomName, 1000, () => getLiveLogs(), "getLog");
      (async () => {
        try {
          socket.emit("getLog", await getLiveLogs());
        } catch {}
      })();
      cleanupEmptyTopicIntervals();
    });

    socket.on("unsubscribeLogs", () => {
      leaveTopicRoom(socket, "logs");
    });

    socket.on("stgStatus", (runOnDay) => {
      const rooms = Array.from(socket.rooms);
      const stgStatusRooms = rooms.filter((room) =>
        room.startsWith("stgStatus:"),
      );

      stgStatusRooms.forEach((room) => {
        socket.leave(room);
      });

      const key = Array.isArray(runOnDay)
        ? JSON.stringify(runOnDay)
        : String(runOnDay ?? "all");
      const roomName = `stgStatus:${key}`;

      socket.join(roomName);
      ensureTopicInterval(
        roomName,
        1000,
        () => fetchStgData(runOnDay),
        "getStgData",
      );
      (async () => {
        try {
          socket.emit("getStgData", await fetchStgData(runOnDay));
        } catch {}
      })();
      cleanupEmptyTopicIntervals();
    });

    socket.on("unsubscribeStgStatus", ({ roomName }) => {
      if (roomName) {
        leaveTopicRoom(socket, roomName);
      }
    });

    socket.on("getPositions", () => {
      const roomName = "positions";
      socket.join(roomName);
      ensureTopicInterval(
        roomName,
        10000,
        () => getClientPositions(),
        "positions",
      );
      (async () => {
        try {
          socket.emit("positions", await getClientPositions());
        } catch {}
      })();
      cleanupEmptyTopicIntervals();
    });

    socket.on("unsubscribePositions", () => {
      leaveTopicRoom(socket, "positions");
    });

    socket.on("getStraddle", () => {
      const roomName = "straddle";
      socket.join(roomName);
      ensureTopicInterval(
        roomName,
        1000,
        () => getStraddle(),
        "straddleData",
      );
      (async () => {
        try {
          socket.emit("straddleData", await getStraddle());
        } catch (error) {
          console.error("Error fetching straddle data:", error);
          socket.emit("straddleData", { error: error.message });
        }
      })();
      cleanupEmptyTopicIntervals();
    });

    socket.on("unsubscribeStraddle", () => {
      leaveTopicRoom(socket, "straddle");
    });

    socket.on("subscribeSystemStatus", () => {
      const roomName = "systemStatus";
      socket.join(roomName);
      ensureTopicInterval(
        roomName,
        120000,
        () => getSystemStatusInfo(),
        "systemStatusUpdate",
      );
      (async () => {
        try {
          socket.emit("systemStatusUpdate", await getSystemStatusInfo());
        } catch {}
      })();
      cleanupEmptyTopicIntervals();
    });
    
    socket.on("subscribeTagPnL", async ({ tag }) => {
      if (!tag) return;
      const roomName = `tagPnL:${tag}`;
      socket.join(roomName);
      ensureTopicInterval(
        roomName,
        1000,
        () => getLiveLogsByTag(tag),
        "tagPnLUpdate",
      );
      (async () => {
        try {
          socket.emit("tagPnLUpdate", await getLiveLogsByTag(tag));
        } catch {}
      })();
      cleanupEmptyTopicIntervals();
    });

    socket.on("unsubscribeTagPnL", ({ tag }) => {
      if (tag) {
        leaveTopicRoom(socket, `tagPnL:${tag}`);
      }
    });

    socket.on("subscribeAccountWiseTagPnL", async ({ tag }) => {
      if (!tag) return;
      const roomName = `accountWiseTagPnL:${tag}`;
      socket.join(roomName);
      ensureTopicInterval(
        roomName,
        2000,
        async () => {
          const tagDetails = await StgTag.findOne({ tag }).lean();
          if (!tagDetails) {
            return { success: false, data: [], message: "Tag not found" };
          }
          const tagWiseStgs = await StrategySchema.find({ tag })
            .lean()
            .select("name tag");
          const stgNames = tagWiseStgs.map((stg) => stg.name);
          const stgTagMap = {};
          tagWiseStgs.forEach((stg) => {
            stgTagMap[stg.name] = stg.tag;
          });

          const allStgLogs = await StgLog.find({
            name: { $in: stgNames },
          }).lean();
          const tokens = new Set();
          allStgLogs.forEach((log) => {
            if (log.orderStatus?.toLowerCase() !== "completed")
              tokens.add(log.symbolToken);
          });
          const tokenArray = Array.from(tokens);
          const tokenValues = tokenArray.length
            ? await client.mget(tokenArray)
            : [];
          const ltpMap = {};
          tokenArray.forEach((t, i) => {
            const raw = tokenValues[i];
            if (raw) {
              try {
                ltpMap[t] = JSON.parse(raw);
              } catch {}
            }
          });

          const lotValues = {
            BANKNIFTY: await fetchIndexData(client, "BANKNIFTY").lot,
            FINNIFTY: await fetchIndexData(client, "FINNIFTY").lot,
            MIDCPNIFTY: await fetchIndexData(client, "MIDCPNIFTY").lot,
            NIFTY: await fetchIndexData(client, "NIFTY").lot,
            SENSEX: await fetchIndexData(client, "SENSEX").lot,
          };

          const accountWisePnl = await Promise.all(
            (tagDetails.mappedAccount || []).map(async (acc) => {
              const logs = allStgLogs.filter(
                (l) => l.clientId === acc.clientId && stgTagMap[l.name] === tag,
              );
              let realizedPnl = 0;
              let openPnl = 0;
              for (const log of logs) {
                let lot;
                for (const [indexName, lotValue] of Object.entries(lotValues)) {
                  if (log.symbol?.includes(indexName)) {
                    lot = lotValue;
                    break;
                  }
                }
                if (!lot) continue;
                if (
                  log.orderStatus?.toLowerCase() === "completed" &&
                  log.exitLtp
                ) {
                  realizedPnl +=
                    (log.side === "B"
                      ? log.exitLtp - log.entryLtp
                      : log.entryLtp - log.exitLtp) *
                    lot *
                    log.lot;
                } else {
                  const wb = ltpMap[log.symbolToken];
                  if (wb?.LTP_Rate != null) {
                    openPnl +=
                      (log.side === "B"
                        ? wb.LTP_Rate - log.entryLtp
                        : log.entryLtp - wb.LTP_Rate) *
                      lot *
                      log.lot;
                  }
                }
              }
              return {
                account: acc.clientId,
                realizedPnl: realizedPnl.toFixed(2),
                openPnl: openPnl.toFixed(2),
                totalPnl: (realizedPnl + openPnl).toFixed(2),
              };
            }),
          );
          return { success: true, data: accountWisePnl };
        },
        "accountWiseTagPnLUpdate",
      );
      (async () => {
        try {
          const tagDetails = await StgTag.findOne({ tag }).lean();
          if (!tagDetails) {
            socket.emit("accountWiseTagPnLUpdate", {
              success: false,
              data: [],
              message: "Tag not found",
            });
          } else {
            const immediate = await (async () => {
              const tagWiseStgs = await StrategySchema.find({ tag })
                .lean()
                .select("name tag");
              const stgNames = tagWiseStgs.map((stg) => stg.name);
              const stgTagMap = {};
              tagWiseStgs.forEach((stg) => {
                stgTagMap[stg.name] = stg.tag;
              });
              const allStgLogs = await StgLog.find({
                name: { $in: stgNames },
              }).lean();
              const tokens = new Set();
              allStgLogs.forEach((log) => {
                if (log.orderStatus?.toLowerCase() !== "completed")
                  tokens.add(log.symbolToken);
              });
              const tokenArray = Array.from(tokens);
              const tokenValues = tokenArray.length
                ? await client.mget(tokenArray)
                : [];
              const ltpMap = {};
              tokenArray.forEach((t, i) => {
                const raw = tokenValues[i];
                if (raw) {
                  try {
                    ltpMap[t] = JSON.parse(raw);
                  } catch {}
                }
              });
              const lotValues = {
                BANKNIFTY: await fetchIndexData(client, "BANKNIFTY").lot,
                FINNIFTY: await fetchIndexData(client, "FINNIFTY").lot,
                MIDCPNIFTY: await fetchIndexData(client, "MIDCPNIFTY").lot,
                NIFTY: await fetchIndexData(client, "NIFTY").lot,
                SENSEX: await fetchIndexData(client, "SENSEX").lot,
              };
              const accountWisePnl = await Promise.all(
                (tagDetails.mappedAccount || []).map(async (acc) => {
                  const logs = allStgLogs.filter(
                    (l) =>
                      l.clientId === acc.clientId && stgTagMap[l.name] === tag,
                  );
                  let realizedPnl = 0;
                  let openPnl = 0;
                  for (const log of logs) {
                    let lot;
                    for (const [indexName, lotValue] of Object.entries(
                      lotValues,
                    )) {
                      if (log.symbol?.includes(indexName)) {
                        lot = lotValue;
                        break;
                      }
                    }
                    if (!lot) continue;
                    if (
                      log.orderStatus?.toLowerCase() === "completed" &&
                      log.exitLtp
                    ) {
                      realizedPnl +=
                        (log.side === "B"
                          ? log.exitLtp - log.entryLtp
                          : log.entryLtp - log.exitLtp) *
                        lot *
                        log.lot;
                    } else {
                      const wb = ltpMap[log.symbolToken];
                      if (wb?.LTP_Rate != null) {
                        openPnl +=
                          (log.side === "B"
                            ? wb.LTP_Rate - log.entryLtp
                            : log.entryLtp - wb.LTP_Rate) *
                          lot *
                          log.lot;
                      }
                    }
                  }
                  return {
                    account: acc.clientId,
                    realizedPnl: realizedPnl.toFixed(2),
                    openPnl: openPnl.toFixed(2),
                    totalPnl: (realizedPnl + openPnl).toFixed(2),
                  };
                }),
              );
              return { success: true, data: accountWisePnl };
            })();
            socket.emit("accountWiseTagPnLUpdate", immediate);
          }
        } catch {}
      })();
      cleanupEmptyTopicIntervals();
    });
    socket.on("unsubscribeAccountWiseTagPnL", ({ tag }) => {
      if (tag) {
        leaveTopicRoom(socket, `accountWiseTagPnL:${tag}`);
      }
    });

    socket.on("disconnecting", () => {
      cleanupEmptyTopicIntervals();
    });
    socket.on("disconnect", () => {
      console.log("Client disconnected");
      cleanupEmptyTopicIntervals();
    });
  });
};

async function fetchStgData(runOnDays) {
  try {
    // console.log("Received runOnDays from frontend:", runOnDays);

    const query = runOnDays?.length > 0 ? { runOnDay: { $in: runOnDays } } : {};
    // getting data of Column ( name,status, _id, loaded)
    const data = await sSchema
      .find(query, "name status _id loaded tag index")
      .lean();

    // filter data based on name and UserId
    const table = await StgLog.aggregate([
      {
        $match: {
          $expr: {
            $and: [
              { $in: ["$name", data.map((item) => item.name)] }
            ],
          },
        },
      },
    ]);

    const strategyNames = Array.from(new Set(table.map((t) => t.name)));
    const viewKeys = strategyNames.map((n) => `VIEW:${n}_view`);
    const viewVals = viewKeys.length ? await client.mget(viewKeys) : [];
    const viewMap = {};
    strategyNames.forEach((name, i) => {
      const raw = viewVals[i];
      if (raw) {
        try {
          viewMap[name] = JSON.parse(raw);
        } catch {}
      }
    });

    // Batch: LTP per token
    const tokenSet = new Set();
    table.forEach((row) => {
      if ((row.orderStatus || "").toLowerCase() !== "completed")
        tokenSet.add(row.symbolToken);
    });
    const tokenList = Array.from(tokenSet);
    const tokenVals = tokenList.length ? await client.mget(tokenList) : [];
    const ltpMap = {};
    tokenList.forEach((t, i) => {
      const raw = tokenVals[i];
      if (raw) {
        try {
          ltpMap[t] = JSON.parse(raw);
        } catch {}
      }
    });

    const data1 = [];
    for (const row of table) {
      const view = viewMap[row.name];
      const obj = {
        name: row.name,
        clientId: row.clientId,
        leg: row.leg,
        symbol: row.symbol,
        symbolToken: row.symbolToken,
        entryLtp: row.entryLtp,
        side: row.side,
        lot: row.lot,
        exitLtp: row.exitLtp,
        orderStatus: row.orderStatus,
        entryTime: row.entryTime,
        exitTime: row.exitTime,
        stoploss: view?.log?.[row.leg]?.stopLoss ?? 0,
        target: view?.log?.[row.leg]?.target ?? 0,
      };

      const index = indices.find((name) => obj.symbol?.includes(name));
      let lot = (await fetchIndexData(client, index)).lot;

      if ((row.orderStatus || "").toLowerCase() === "completed") {
        obj.pnl = (
          (row.side === "B"
            ? row.exitLtp - row.entryLtp
            : row.entryLtp - row.exitLtp) *
          lot *
          obj.lot
        ).toFixed(2);
      } else {
        const wb = ltpMap[row.symbolToken];
        const ltp = wb?.LTP_Rate;
        if (ltp != null) {
          obj.pnl = (
            (row.side === "B" ? ltp - row.entryLtp : row.entryLtp - ltp) *
            lot *
            obj.lot
          ).toFixed(2);
        } else {
          obj.pnl = (0).toFixed(2);
        }
      }
      data1.push(obj);
    }
    // Now adding all pnl based on their unique name
    const aggregatedData = {};

    // Iterate through the original array and aggregate PNL values
    data1.forEach((item) => {
      const name = item.name;

      // If the name is not already in the aggregatedData object, initialize it with the current item's pnl
      if (!aggregatedData[name]) {
        aggregatedData[name] = {
          name: name,
          totalPnl: parseFloat(item.pnl),
        };
      } else {
        // If the name is already in the aggregatedData object, add the current item's pnl to the total
        aggregatedData[name].totalPnl += parseFloat(item.pnl);
      }
    });

    // Convert the aggregatedData to an array
    const aggregatedArray = Object.values(aggregatedData);

    // merrging data and aggregatedArray
    const mergedArray = data.map((item) => {
      const matchingItem = aggregatedArray.find(
        (aggItem) => aggItem.name === item.name,
      );

      return {
        _id: item._id,
        status: item.status,
        loaded: item.loaded,
        name: item.name,
        tag: item.tag,
        index: item.index,
        totalPnl: matchingItem ? matchingItem.totalPnl : 0,
      };
    });
    return mergedArray;
  } catch (error) {
    console.log(error);
    return [];
  }
}

async function getLiveStgLogs(stgName) {
  try {
    // Get strategy to find its tag
    const strategy = await StrategySchema.findOne({ name: stgName })
      .lean()
      .select("tag");

    // Get mapped accounts from tag collection
    let mappedAccounts = [];
    if (strategy?.tag) {
      const tagDetails = await StgTag.findOne({ tag: strategy.tag })
        .lean()
        .select("mappedAccount");
      if (tagDetails?.mappedAccount) {
        mappedAccounts = tagDetails.mappedAccount;
      }
    }

    const table = await StgLog.find({ name: stgName })
      .lean()
      .select(
        "name clientId leg symbol symbolToken entryLtp side lot exitLtp orderStatus stopLoss target entryTime exitTime tag",
      );

    const data = [];

    let parsedData;
    try {
      const redisData = await client.get(`VIEW:${stgName}_view`);
      if (redisData) parsedData = JSON.parse(redisData);
    } catch {}

    const tokens = table
      .filter((row) => (row.orderStatus || "").toLowerCase() !== "completed")
      .map((row) => row.symbolToken);
    const uniqueTokens = Array.from(new Set(tokens));
    const tokenValues = uniqueTokens.length
      ? await client.mget(uniqueTokens)
      : [];
    const ltpMap = {};
    uniqueTokens.forEach((t, i) => {
      const raw = tokenValues[i];
      if (raw) {
        try {
          ltpMap[t] = JSON.parse(raw);
        } catch {}
      }
    });

    // console.log("table477:-", table);
    for (const row of table) {
      const obj = {
        name: row.name,
        tag: row.tag,
        clientId: row.clientId,
        leg: row.leg,
        symbol: row.symbol,
        symbolToken: row.symbolToken,
        entryLtp: row.entryLtp,
        side: row.side,
        lot: row.lot,
        exitLtp: row.exitLtp,
        orderStatus: row.orderStatus,
        entryTime: row.entryTime,
        exitTime: row.exitTime,
        stoploss: row?.stopLoss ?? 0,
        target: row?.target ?? 0,
      };

      let lot;
      const index = indices.find((name) => obj.symbol?.includes(name));
      if (index) {
        lot = (await fetchIndexData(client, index))?.lot;
      }

      if ((row.orderStatus || "").toLowerCase() === "completed") {
        obj.running_ltp = row.exitLtp;
        obj.pnl = (
          (row.side === "B"
            ? row.exitLtp - row.entryLtp
            : row.entryLtp - row.exitLtp) *
          lot *
          obj.lot
        ).toFixed(2);
      } else {
        const wb = ltpMap[row.symbolToken];
        const ltp = wb?.LTP_Rate;
        obj.running_ltp = ltp;
        if (ltp != null) {
          obj.pnl = (
            (row.side === "B" ? ltp - row.entryLtp : row.entryLtp - ltp) *
            lot *
            obj.lot
          ).toFixed(2);
        } else {
          obj.pnl = (0).toFixed(2);
        }
      }

      data.push(obj);
    }

    // Return object with logs and mappedAccounts for UI display
    return {
      logs: data,
      mappedAccounts: mappedAccounts,
      tag: strategy?.tag || null,
    };
  } catch (error) {
    console.error("Error fetching data:", error.message);
    return {
      logs: [],
      mappedAccounts: [],
      tag: null,
    };
  }
}

async function getLiveLogs() {
  try {
    let redisData = await client.lrange("Logs", 0, -1);

    let data = redisData.map((log) => JSON.parse(log));

    data.sort((a, b) => new Date(b.time) - new Date(a.time));

    const latestLogs = data;

    return latestLogs;
  } catch (error) {
    console.error("Error fetching live logs:", error.message);
    return [];
  }
}

async function handleTagSquareOff(
  tag,
  adjustedTotalPnl,
  isLossBreach,
  maxLoss,
  maxProfit,
  sqoffDoneKey,
) {
  const lockKey = `tag:${tag}:sqoff_lock`;

  try {
    const lockResult = await client.set(
      lockKey,
      Date.now().toString(),
      "NX",
      "EX",
      60,
    );
    if (lockResult !== "OK") {
      return;
    }

    try {
      const runningStrategies = await StrategySchema.find(
        {
          tag: tag,
          status: "Running",
        },
        { _id: 1 },
      );

      if (runningStrategies.length > 0) {
        const pipeline = client.pipeline();
        runningStrategies.forEach((strategy) => {
          const key = `SQOFF:${strategy._id}`;
          const sqoffMessage = JSON.stringify({
            message: REDIS_MESSAGES.STRATEGY_MANUAL_SQOFF,
          });
          pipeline.set(key, sqoffMessage);
        });

        pipeline.set(sqoffDoneKey, "1", "EX", 86400);

        await pipeline.exec();

        const breachType = isLossBreach ? "max loss" : "max profit";
        const breachValue = isLossBreach ? maxLoss : maxProfit;

        await saveLog(
          "AUTO_SQOFF",
          "MESSAGE",
          `Auto square-off triggered for tag ${tag} due to ${breachType} limit (${breachValue}) reached. Current PnL: ${adjustedTotalPnl.toFixed(2)}`,
        );
      } else {
        await client.set(sqoffDoneKey, "1", "EX", 86400);
      }

      const keyPrefix = `tag:${tag}:`;
      const breachStatusKey = `${keyPrefix}breach_status`;
      const loggedWaitKey = `${keyPrefix}logged_wait`;

      await Promise.all([
        client.del(breachStatusKey),
        client.del(loggedWaitKey),
      ]);
    } finally {
      await client.del(lockKey);
    }
  } catch (error) {
    console.error(`Error in auto square-off for tag ${tag}:`, error);

    await saveLog(
      "ERROR",
      "AUTO_SQOFF",
      `Failed to auto square-off tag ${tag}: ${error.message}`,
    );
  }
}

async function handleBreachAndSquareOff(
  tag,
  adjustedTotalPnl,
  maxLoss,
  maxProfit,
  maxLossWaitSeconds,
  maxProfitWaitSeconds,
) {
  const keyPrefix = `tag:${tag}:`;
  const sqoffDoneKey = `${keyPrefix}sqoff_done`;
  const breachStatusKey = `${keyPrefix}breach_status`;
  const loggedWaitKey = `${keyPrefix}logged_wait`;

  const sqoffDone = await client.get(sqoffDoneKey);
  if (sqoffDone) {
    return;
  }

  const isLossBreach = maxLoss > 0 && adjustedTotalPnl <= -maxLoss;
  const isProfitBreach = maxProfit > 0 && adjustedTotalPnl >= maxProfit;
  const isBreach = isLossBreach || isProfitBreach;
  const breachType = isLossBreach ? "loss" : isProfitBreach ? "profit" : null;
  const waitSeconds = isLossBreach ? maxLossWaitSeconds : maxProfitWaitSeconds;

  const breachStatus = await client.get(breachStatusKey);
  let parsedStatus = null;

  try {
    if (breachStatus) {
      parsedStatus = JSON.parse(breachStatus);
    }
  } catch (e) {
    console.error("Error parsing breach status:", e);
  }

  if (!isBreach) {
    if (parsedStatus) {
      await Promise.all([
        client.del(breachStatusKey),
        client.del(loggedWaitKey),
      ]);

      await saveLog(
        "BREACH_RECOVERY",
        "MESSAGE",
        `Tag ${tag} recovered from ${parsedStatus.type} breach. Current PnL: ${adjustedTotalPnl.toFixed(2)}`,
      );
    }
    return;
  }

  if (!parsedStatus) {
    const newStatus = {
      type: breachType,
      startTime: Date.now(),
      waitSeconds,
    };

    await client.set(breachStatusKey, JSON.stringify(newStatus));

    if (waitSeconds > 0) {
      await client.set(loggedWaitKey, "1", "EX", waitSeconds + 5);

      await saveLog(
        "AUTO_SQOFF_WAIT",
        "MESSAGE",
        `Auto square-off wait started for tag ${tag} due to ${breachType} breach. Will check again after ${waitSeconds} seconds.`,
      );
    } else {
      await handleTagSquareOff(
        tag,
        adjustedTotalPnl,
        isLossBreach,
        maxLoss,
        maxProfit,
        sqoffDoneKey,
      );
    }
    return;
  }

  if (parsedStatus.type !== breachType) {
    const newStatus = {
      type: breachType,
      startTime: Date.now(),
      waitSeconds,
    };

    await client.set(breachStatusKey, JSON.stringify(newStatus));

    if (waitSeconds > 0) {
      await client.del(loggedWaitKey);
      await client.set(loggedWaitKey, "1", "EX", waitSeconds + 5);

      await saveLog(
        "AUTO_SQOFF_WAIT",
        "MESSAGE",
        `Breach type changed to ${breachType} for tag ${tag}. Starting new wait period of ${waitSeconds} seconds.`,
      );
    } else {
      await handleTagSquareOff(
        tag,
        adjustedTotalPnl,
        isLossBreach,
        maxLoss,
        maxProfit,
        sqoffDoneKey,
      );
    }
    return;
  }

  const now = Date.now();
  const elapsedTime = now - parsedStatus.startTime;

  if (elapsedTime >= parsedStatus.waitSeconds * 1000) {
    if (isBreach) {
      await saveLog(
        "AUTO_SQOFF_AFTER_WAIT",
        "MESSAGE",
        `Auto square-off triggered for tag ${tag} after ${waitSeconds} seconds wait. ${breachType} breach still persists.`,
      );

      await handleTagSquareOff(
        tag,
        adjustedTotalPnl,
        isLossBreach,
        maxLoss,
        maxProfit,
        sqoffDoneKey,
      );
    } else {
      await Promise.all([
        client.del(breachStatusKey),
        client.del(loggedWaitKey),
      ]);
    }
  }
}

async function getLiveLogsByTag(tag) {
  try {
    const lotValues = {
      BANKNIFTY: await fetchIndexData(client, "BANKNIFTY").lot,
      FINNIFTY: await fetchIndexData(client, "FINNIFTY").lot,
      MIDCPNIFTY: await fetchIndexData(client, "MIDCPNIFTY").lot,
      NIFTY: await fetchIndexData(client, "NIFTY").lot,
      SENSEX: await fetchIndexData(client, "SENSEX").lot,
    };

    const tagDetails = await StgTag.findOne({ tag });
    if (!tagDetails) {
      return {
        tag,
        totalPnl: "0.00",
        strategies: [],
        timestamp: new Date().toISOString(),
      };
    }

    const currDayinNum = new Date().getDay();

    const tagWiseStgs = await StrategySchema.find({
      tag: tag,
      parentAcc: tagDetails.tagParentAccount,
      runOnDay: { $in: [currDayinNum] },
    });

    if (!tagWiseStgs || tagWiseStgs.length === 0) {
      return {
        tag,
        totalPnl: "0.00",
        strategies: [],
        timestamp: new Date().toISOString(),
      };
    }

    const stgNames = tagWiseStgs.map((stg) => stg.name);

    const validStrategyNames = new Set(stgNames);

    const stgTagMap = {};
    tagWiseStgs.forEach((stg) => {
      stgTagMap[stg.name] = stg.tag;
    });

    const allStgLogs = await StgLog.find({
      name: { $in: stgNames },
      clientId: tagDetails.tagParentAccount,
    });

    const logsByStrategy = {};

    allStgLogs.forEach((log) => {
      if (!validStrategyNames.has(log.name) || log.entryLtp === 0) return;

      const strategyTag = stgTagMap[log.name];
      if (strategyTag !== tag) return;

      if (log.clientId !== tagDetails.tagParentAccount) return;

      if (!logsByStrategy[log.name]) {
        logsByStrategy[log.name] = [];
      }
      logsByStrategy[log.name].push(log);
    });

    const symbolTokens = new Set();
    allStgLogs.forEach((log) => {
      if (log.orderStatus.toLowerCase() !== "completed") {
        symbolTokens.add(log.symbolToken);
      }
    });

    const symbolTokenArray = Array.from(symbolTokens);
    const ltpDataArray = await Promise.all(
      symbolTokenArray.map((token) => client.get(token)),
    );

    const ltpDataMap = {};
    symbolTokenArray.forEach((token, index) => {
      const data = ltpDataArray[index];
      if (data) {
        ltpDataMap[token] = JSON.parse(data);
      }
    });

    let totalPnl = 0;
    let strategyPnl = [];

    for (const stg of tagWiseStgs) {
      const stgLogs = logsByStrategy[stg.name] || [];
      let strategyTotalPnl = 0;

      for (const log of stgLogs) {
        let lot;
        for (const [indexName, lotValue] of Object.entries(lotValues)) {
          if (log.symbol.includes(indexName)) {
            lot = lotValue;
            break;
          }
        }
        if (!lot) continue;

        let pnl = 0;
        if (log.orderStatus.toLowerCase() === "completed") {
          pnl =
            log.side === "B"
              ? (log.exitLtp - log.entryLtp) * lot * log.lot
              : (log.entryLtp - log.exitLtp) * lot * log.lot;
        } else {
          const wbJson = ltpDataMap[log.symbolToken];
          if (wbJson) {
            pnl =
              log.side === "B"
                ? (wbJson.LTP_Rate - log.entryLtp) * lot * log.lot
                : (log.entryLtp - wbJson.LTP_Rate) * lot * log.lot;
          }
        }
        strategyTotalPnl += pnl;
      }

      strategyPnl.push({
        name: stg.name,
        pnl: strategyTotalPnl.toFixed(2),
      });
      totalPnl += strategyTotalPnl;
    }

    const parentMultiplier = 1;
    const adjustedTotalPnl = totalPnl;

    const maxLoss = tagDetails.tagMaxLoss || 0;
    const maxProfit = tagDetails.tagMaxProfit || 0;
    const maxLossWaitSeconds = tagDetails.maxLossWaitSeconds || 0;
    const maxProfitWaitSeconds = tagDetails.maxProfitWaitSeconds || 0;

    await handleBreachAndSquareOff(
      tag,
      adjustedTotalPnl,
      maxLoss,
      maxProfit,
      maxLossWaitSeconds,
      maxProfitWaitSeconds,
    );

    return {
      tag,
      totalPnl: totalPnl.toFixed(2),
      adjustedTotalPnl: adjustedTotalPnl.toFixed(2),
      strategies: strategyPnl,
      // timestamp: new Date().toISOString(),
      limits: {
        maxLoss,
        maxProfit,
        parentMultiplier,
      },
    };
  } catch (error) {
    console.error("Error fetching live logs:", error.message);
    return {
      tag,
      totalPnl: "0.00",
      adjustedTotalPnl: "0.00",
      strategies: [],
      // timestamp: new Date().toISOString(),
      limits: {
        maxLoss: 0,
        maxProfit: 0,
        parentMultiplier: 1,
      },
    };
  }
}

export default Socket;
