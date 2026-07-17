import { SimOrderDetails } from '../utils/sharedImport.js';
import { getCachedClient } from '../utils/sharedImport.js';
import { placeOrder, orderManagement, checkOrderStatus, cancelOrder } from '../OrderManagement/order.js';
import { saveLog } from '../utils/saveLog.js'
import SyncManagement from '../models/syncStatus.js';
import { getLivePosition, getPendingOrder } from '../utils/sharedImport.js';
import { Client } from '../utils/sharedImport.js';
import connectRedis from '../utils/connectRedis.js';
import { axiosFetchWithProxy } from "../utils/sharedImport.js"

const redisClient = await connectRedis()

export async function checkSync() {
  try {
    console.log("Running sync Check");

    let simPositions = await SimOrderDetails.aggregate(simRunningPositionAgg)
    const clients = await Client.find(
      { active: true, userId: { $ne: "SIM" } },
      { _id: 0, userId: 1, brokerUrl: 1, isIndividualClient: 1 }
    );

    let manageClientOrderPromisses = []
    let manageClientOrderResult = null
    let manageClientSlOrderPromisses = []
    let manageClientSlOrderResult = null

    for (const client of clients) {
      try {
        // Check order difference
        const simClientOrder = simPositions.filter(order => order.clientId === client.userId && order.type === "ORDER");
        const clientOrder = await getLivePosition(redisClient, client.userId, true)
        // console.log("simClientOrder===>", simClientOrder);
        console.log("clientOrder===>", clientOrder);
        const orderDiff = calculateOrderDifferences(simClientOrder, clientOrder);
        if (orderDiff.length > 0) {
          console.log("Net Order Diff for", client.userId, "==>", orderDiff);
          manageClientOrderPromisses.push(manageOrders(orderDiff));
        } else {
          console.log("No Diff in orders");
        }

        // Check SL order difference
        const simClientSlOrder = simPositions.filter(order => order.clientId === client.userId && order.type === "STOPLOSS");
        const clientSlOrder = await getPendingOrder(redisClient, client.userId, true)
        // console.log("simClientSlOrder===>", simClientSlOrder);
        // console.log("clientSlOrder===>", clientSlOrder);

        const stopLossDiff = calculateStopLossOrderDifferences(simClientSlOrder, clientSlOrder)
        if (stopLossDiff.length > 0) {
          console.log("Net SL Order Diff for", client.userId, "==>", stopLossDiff);
          manageClientSlOrderPromisses.push(manageSlOrders(stopLossDiff))
        } else {
          console.log("No Sl Diff in orders");
        }

      } catch (error) {
        console.log(`Error processing client ${client.userId} in checkSync`, error);
      }

    }
    manageClientOrderResult = await Promise.allSettled(manageClientOrderPromisses)
    manageClientSlOrderResult = await Promise.allSettled(manageClientSlOrderPromisses)
  } catch (error) {
    console.log("Error in checkSync", error);
  }
}

const simRunningPositionAgg = [
  { $match: { "trade.status": "Started" } },
  { $unwind: "$mappedClients" },
  // { $match: { "mappedClients.active": true } }, // TODO check this 
  {
    $facet: {
      // MAIN ORDERS PIPELINE
      mainOrders: [
        {
          $project: {
            netQuantity: {
              $cond: [
                { $eq: ["$trade.side", "SELL"] },
                { $multiply: ["$trade.quantity", "$mappedClients.multiplier", -1] },
                { $multiply: ["$trade.quantity", "$mappedClients.multiplier"] }
              ]
            },
            clientId: "$mappedClients.clientId",
            exchangeInstrumentId: { $toInt: "$trade.strikeSelected" },
            index: "$trade.index",
            type: { $literal: "ORDER" }
          }
        },
        {
          // 👇 Group by clientId + exchangeInstrumentId
          $group: {
            _id: {
              clientId: "$clientId",
              exchangeInstrumentId: "$exchangeInstrumentId"
            },
            netQuantity: { $sum: "$netQuantity" },
            type: { $first: "$type" },
            index: { $first: "$index" }
          }
        },
        {
          // 👇 Final projection: only required fields
          $project: {
            _id: 0,
            clientId: "$_id.clientId",
            exchangeInstrumentId: "$_id.exchangeInstrumentId",
            netQuantity: 1,
            type: 1,
            index: 1
          }
        }
      ],

      // STOPLOSS ORDERS PIPELINE (unchanged)
      stoplossOrders: [
        {
          $match: { type: "ORDERWITHSL" }
        },
        {
          $project: {
            netQuantity: {
              $cond: [
                { $eq: ["$trade.side", "SELL"] },
                { $multiply: ["$trade.quantity", "$mappedClients.multiplier"] },
                { $multiply: ["$trade.quantity", "$mappedClients.multiplier", -1] }
              ]
            },
            _id: 0,
            stgName: 1,
            key: 1,
            clientId: "$mappedClients.clientId",
            exchangeInstrumentId: { $toInt: "$trade.strikeSelected" },
            type: { $literal: "STOPLOSS" },
            index: "$trade.index",
            orderPrice: "$trade.stopLoss"
          }
        }
      ]
    }
  },
  {
    $project: {
      result: { $concatArrays: ["$mainOrders", "$stoplossOrders"] }
    }
  },
  { $unwind: "$result" },
  { $replaceRoot: { newRoot: "$result" } }
];

function calculateOrderDifferences(simClientOrder, clientPositions) {
  // Create maps for O(1) lookup
  const simMap = new Map();
  const clientMap = new Map();

  // Build sim orders map
  simClientOrder.forEach(order => {
    // Convert to number to ensure consistency
    const instrumentId = Number(order.exchangeInstrumentId);
    simMap.set(instrumentId, {
      netQuantity: Number(order.netQuantity), // Also convert quantity to number
      index: order.index,
      clientId: order.clientId,
      exchangeSegment: order.index === "SENSEX" ? "BSEFO" : "NSEFO",
    });
  });

  // Build client positions map
  clientPositions.forEach(position => {
    // Convert to number to ensure consistency
    const instrumentId = Number(position.exchangeInstrumentId);
    clientMap.set(instrumentId, {
      netQuantity: Number(position.netQuantity), // Convert string to number
      index: position.index,
      clientId: position.clientId,
      exchangeSegment: position.exchangeSegment,
    });
  });

  // Get all unique exchangeInstrumentIds
  const allInstrumentIds = new Set([
    ...simMap.keys(),
    ...clientMap.keys()
  ]);

  // Calculate differences
  const result = [];

  for (const instrumentId of allInstrumentIds) {
    const simData = simMap.get(instrumentId);
    const clientData = clientMap.get(instrumentId);

    const simQty = simData?.netQuantity || 0;
    const clientQty = clientData?.netQuantity || 0;
    const diff = simQty - clientQty;

    // Only add to result if diff is not 0
    if (diff !== 0) {
      result.push({
        exchangeInstrumentId: instrumentId,
        index: simData?.index || clientData?.index || null,
        exchangeSegment: clientData?.exchangeSegment || simData?.exchangeSegment || null,
        clientId: simData?.clientId || clientData?.clientId,
        simQuantity: simQty,
        clientQuantity: clientQty,
        diff: diff
      });
    }
  }

  return result;
}

function calculateStopLossOrderDifferences(simClientSlOrder, clientSlOrder) {
  const syncResult = [];

  const clientOrders = [...clientSlOrder];

  for (const simOrder of simClientSlOrder) {
    const matchIndex = clientOrders.findIndex(clientOrder =>
      clientOrder.clientId === simOrder.clientId &&
      clientOrder.exchangeInstrumentId === simOrder.exchangeInstrumentId &&
      clientOrder.netQuantity === simOrder.netQuantity &&
      clientOrder.stopPrice === simOrder.orderPrice
    );

    if (matchIndex !== -1) {
      clientOrders.splice(matchIndex, 1);
    } else {
      const modifyIndex = clientOrders.findIndex(clientOrder =>
        clientOrder.clientId === simOrder.clientId &&
        clientOrder.exchangeInstrumentId === simOrder.exchangeInstrumentId
      );

      if (modifyIndex !== -1) {
        const clientOrder = clientOrders[modifyIndex];
        const qtyMismatch = clientOrder.netQuantity !== simOrder.netQuantity;
        const priceMismatch = clientOrder.stopPrice !== simOrder.orderPrice;

        let modifyReason = "MODIFY";
        if (qtyMismatch && priceMismatch) {
          modifyReason = "MODIFY_QTY_PRICE";
        } else if (qtyMismatch) {
          modifyReason = "MODIFY_QTY";
        } else if (priceMismatch) {
          modifyReason = "MODIFY_PRICE";
        }

        syncResult.push({
          reason: modifyReason,
          clientId: simOrder.clientId,
          exchangeInstrumentId: simOrder.exchangeInstrumentId,
          diff: simOrder.netQuantity,
          orderPrice: simOrder.orderPrice,
          appOrderID: clientOrder.appOrderID,
          stgName: simOrder.stgName,
          key: simOrder.key,
          exchangeSegment: clientOrder.exchangeSegment || (simOrder.index === "SENSEX" ? "BSEFO" : "NSEFO"),
          index: simOrder.index || clientOrder.index,
          oldQuantity: clientOrder.netQuantity,
          oldPrice: clientOrder.stopPrice,
        });
        clientOrders.splice(modifyIndex, 1);
      } else {
        syncResult.push({
          reason: "NEWORDER",
          clientId: simOrder.clientId,
          exchangeInstrumentId: simOrder.exchangeInstrumentId,
          diff: simOrder.netQuantity,
          orderPrice: simOrder.orderPrice,
          stgName: simOrder.stgName,
          key: simOrder.key,
          exchangeSegment: simOrder.index === "SENSEX" ? "BSEFO" : "NSEFO",
          index: simOrder.index,
        });
      }
    }
  }

  for (const remainingClient of clientOrders) {
    syncResult.push({
      reason: "EXTRAORDER",
      clientId: remainingClient.clientId,
      exchangeInstrumentId: remainingClient.exchangeInstrumentId,
      diff: remainingClient.netQuantity,
      orderPrice: remainingClient.stopPrice,
      appOrderID: remainingClient.appOrderID,
      exchangeSegment: remainingClient.exchangeSegment,
      index: remainingClient.index,
    });
  }

  return syncResult;
}

async function manageOrders(orders) {
  try {
    let orderPromises = []
    let orderResult = null
    let orderManagementPromises = []
    let orderManagementResult = null

    for (const order of orders) {
      let { client, trade } = await mapTradeforOrder(order)
      // console.log(order, "==>", trade);
      orderPromises.push(placeOrder("ORDER", trade, client, "CheckSyncService", ""))
    }
    await new Promise(resolve => setTimeout(resolve, 2000));
    orderResult = await Promise.allSettled(orderPromises)

    for (let result of orderResult) {
      if (result.status === "fulfilled" && result.value) {
        for (const order of result.value) {
          orderManagementPromises.push(orderManagement(order.orderId, order.client, "CheckSyncService", ""));
        }
      }
    }
    orderManagementResult = await Promise.allSettled(orderManagementPromises)
    console.log("Order Management Result", orderManagementResult);

  } catch (error) {
    console.log("Error in manageOrders method==>", error);

  }
}

async function manageSlOrders(orders) {
  try {
    let orderPromises = []
    let orderResult = null
    let orderManagementPromises = []
    let orderManagementResult = null

    for (const order of orders) {
      order.type = "STOPLOSS"
      let { client, trade } = await mapTradeforOrder(order)
      if (order.reason === "NEWORDER") {
        orderPromises.push(placeOrder("STOPLOSS", trade, client, order.stgName, order.key))
      } else if (order.reason === "EXTRAORDER") {
        await cancelOrder(order.appOrderID, client, "CheckSyncService", "")
      } else if (order.reason.startsWith("MODIFY")) {
        await modifyOrderForSync(order, client, order.stgName, order.key)
      }
    }
    orderResult = await Promise.allSettled(orderPromises)
    // console.log("STOPLOSS Placement Result", orderResult);
    await new Promise(resolve => setTimeout(resolve, 2000));
    for (let result of orderResult) {
      if (result.status === "fulfilled" && result.value) {
        for (const order of result.value) {
          orderManagementPromises.push(checkOrderStatus(order.orderId, order.client, "CheckSyncService", ""))
        }
      }
    }
    orderManagementResult = await Promise.allSettled(orderManagementPromises)
    console.log("Check Order Status result for STOPLOSS", orderManagementResult);

  } catch (error) {
    console.log("Error in manageSlOrders method==>", error);

  }
}

async function mapTradeforOrder(orderObj) {
  try {
    const client = await getCachedClient(redisClient, orderObj.clientId)

    // Determine side based on diff
    let side = orderObj.diff > 0 ? "BUY" : "SELL";

    // Reverse side if type is STOPLOSS
    if (orderObj.type && orderObj.type.toUpperCase() === "STOPLOSS") {
      side = side === "BUY" ? "SELL" : "BUY";
    }

    let trade = {
      index: orderObj.index,
      strikeSelected: orderObj.exchangeInstrumentId.toString(),
      quantity: Math.abs(orderObj.diff),
      side: side,
      stopLoss: orderObj.orderPrice || 0,
      ExchangeSegment: orderObj.exchangeSegment,
    };

    return { client, trade };
  } catch (error) {
    console.error("Error in mapTradeforOrder", error);
  }
}


async function modifyOrderForSync(order, client, stgName, key) {
  try {
    
    let orderObj = await checkOrderStatus(order.appOrderID, client, stgName, key)

    let buffer = order.diff > 0 ? UP_ltp(order.orderPrice, 5 / 2) : DOWN_ltp(order.orderPrice, 5 / 2);

    let requestBody = {
      appOrderID: orderObj.AppOrderID,
      modifiedProductType: orderObj.ProductType,
      modifiedOrderType: orderObj.OrderType.toUpperCase(),
      modifiedOrderQuantity: Math.abs(order.diff),
      modifiedDisclosedQuantity: orderObj.OrderDisclosedQuantity,
      modifiedLimitPrice: buffer,
      modifiedStopPrice: order.orderPrice,
      modifiedTimeInForce: orderObj.TimeInForce,
      clientID: "*****"
    }

    let url = `${client.brokerUrl}/interactive/orders`
    console.log("Modify Order body==>", requestBody)
    let { data } = await axiosFetchWithProxy(redisClient, url, "PUT", client.userId, requestBody)
    console.log("Modify Order Response==>", data)
    return data
  } catch (error) {
    console.log("Error in method modifyOrder ==>", error)
    saveLog(stgName, key, "ERROR", `Error in method modifyOrder: ${error.message}`)
  }
}

function DOWN_ltp(ltp, buffer) {
  let DOWN_ltp = ltp * (1 - Math.abs(buffer / 100))
  DOWN_ltp = Math.round(DOWN_ltp * 20) / 20
  return DOWN_ltp
}

function UP_ltp(ltp, buffer) {
  let UP_ltp = ltp * (1 + Math.abs(buffer / 100))
  UP_ltp = Math.round(UP_ltp * 20) / 20
  return UP_ltp
}

