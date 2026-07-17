import * as Order from '../OrderManagement/order.js'
import { SimOrderDetails } from '../utils/sharedImport.js';
import { getCachedClient, redisConnect, saveLog } from '../utils/sharedImport.js';

const redisClient = await redisConnect(true)
const redisCounter = await redisConnect(true); // for INCR / DECR

let flagKey = "CopyTradeProcessingFlag"

export async function watchSimTrades() {
  console.log("Copy-trade consumer started...");

  while (true) {
    // Blocking read from Redis queue (orderQueue)
    const res = await redisClient.brpop("OrderQueue", 0);
    if (!res) continue;
    // Increment counter
    let counter = await redisCounter.incr(flagKey);
    console.log(`Current processing count: ${counter}`);
    const tradeInfo = JSON.parse(res[1]);
    // console.log("📥 Got SIM trade from queue:", tradeInfo);
    applyTradeToClients(tradeInfo).catch(err => {
      console.error("applyTradeToClients failed:", err);
    });
  };
}

async function applyTradeToClients(tradeInfo) {
  try {
    let orderPromises = []
    let orderResult = null
    let orderManagementPromises = []
    let orderManagementResult = null
    const clients = tradeInfo.mappedClients
    const trade = tradeInfo.trade
    const baseTradeQuantity = tradeInfo.trade.quantity
    const type = tradeInfo.type

    if (type === "ONLYORDER" || type === "EXITORDER") {

      for (const client of clients) {
        trade.quantity = baseTradeQuantity * client.multiplier
        console.log(client.clientId);
        
        const cli = await getCachedClient(redisCounter, client.clientId)
        console.log(cli);
        
        orderPromises.push(Order.placeOrder("ORDER", trade, cli, tradeInfo.stgName, tradeInfo.key))
      }
      orderResult = await Promise.allSettled(orderPromises)
      console.log("Order Placement Result for ", type, orderResult);

      for (let result of orderResult) {
        if (result.status === "fulfilled" && result.value) {
          for (const order of result.value) {
            orderManagementPromises.push(Order.orderManagement(order.orderId, order.client, tradeInfo.stgName, tradeInfo.key));
          }
        }
      }

      orderManagementResult = await Promise.allSettled(orderManagementPromises)
      console.log("Order Management Result for ", type, orderManagementResult);

      for (let result of orderManagementResult) {
        if (result.status === "fulfilled" && result.value?.orderStatusObj?.OrderStatus === "Filled") {
          const orderObj = result.value.orderStatusObj;
          saveLog(redisCounter, tradeInfo.stgName, tradeInfo.key, "ORDER", `${orderObj.ClientID} ORDER Filled ${orderObj.OrderSide} ${orderObj.OrderQuantity} @ ${orderObj.OrderAverageTradedPrice}, AppOrderID ${orderObj.AppOrderID}`)
          await SimOrderDetails.findOneAndUpdate(
            { stgName: tradeInfo.stgName, key: tradeInfo.key },
            { $push: { orders: orderObj } }
          );
        }
      }
    }

    else if (type === "ORDERWITHSL") {
      for (const client of clients) {
        trade.quantity = baseTradeQuantity * client.multiplier
        const cli = await getCachedClient(redisCounter, client.clientId)
        orderPromises.push(Order.placeOrder("ORDER", trade, cli, tradeInfo.stgName, tradeInfo.key))
        saveLog(redisCounter, tradeInfo.stgName, tradeInfo.key, "ORDER", `${client.clientId} placing ENTRY ORDER ${trade.side} ${trade.quantity} of ${trade.index} token ${trade.strikeSelected}`)
      }
      orderResult = await Promise.allSettled(orderPromises)
      console.log("Order Placement Result for ", type, orderResult);
      // Order placement
      for (let result of orderResult) {
        if (result.status === "fulfilled" && result.value) {
          for (const order of result.value) {
            orderManagementPromises.push(Order.orderManagement(order.orderId, order.client, tradeInfo.stgName, tradeInfo.key));
          }
        }
      }
      // Order management
      orderManagementResult = await Promise.allSettled(orderManagementPromises)
      console.log("Order Management Result for ", type, JSON.stringify(orderManagementResult, null, 2));

      // Update in DB
      for (let result of orderManagementResult) {
        if (result.status === "fulfilled" && result.value?.orderStatusObj?.OrderStatus === "Filled") {
          const orderObj = result.value.orderStatusObj;
          saveLog(redisCounter, tradeInfo.stgName, tradeInfo.key, "ORDER", `${orderObj.ClientID} ENTRY ORDER Filled ${orderObj.OrderSide} ${orderObj.OrderQuantity} @ ${orderObj.OrderAverageTradedPrice}, AppOrderID ${orderObj.AppOrderID}`)
          await SimOrderDetails.findOneAndUpdate(
            { stgName: tradeInfo.stgName, key: tradeInfo.key },
            { $push: { orders: orderObj } }
          );
        }
      }

      orderPromises = []
      orderManagementPromises = []

      // Place stoploss order
      for (const client of clients) {
        trade.quantity = baseTradeQuantity * client.multiplier
        const cli = await getCachedClient(redisCounter, client.clientId)
        orderPromises.push(Order.placeOrder("STOPLOSS", trade, cli, tradeInfo.stgName, tradeInfo.key))
      }
      orderResult = await Promise.allSettled(orderPromises)
      console.log("STOPLOSS Placement Result for ", type, orderResult);
      await new Promise(resolve => setTimeout(resolve, 2000));

      for (let result of orderResult) {
        if (result.status === "fulfilled" && result.value) {
          for (const order of result.value) {
            orderManagementPromises.push(Order.checkOrderStatus(order.orderId, order.client, tradeInfo.stgName, tradeInfo.key));
          }
        }
      }
      orderManagementResult = await Promise.allSettled(orderManagementPromises)
      // console.log("Stoploss order Management Result for:", type, orderManagementResult);
      for (let result of orderManagementResult) {
        if (result.status === "fulfilled" && ["New", "PendingNew", "Replaced"].includes(result.value.OrderStatus)) {  //TODO:: Remove filled
          console.log("Saving Sl in DB of", tradeInfo.stgName, tradeInfo.key, result.value.OrderStatus, result.value.AppOrderID);
          await SimOrderDetails.findOneAndUpdate(
            { stgName: tradeInfo.stgName, key: tradeInfo.key },
            {
              $push: { slOrders: result.value }
            }
          );
        }
      }
    }

    else if (type === "CHECKSLONEXIT") {
      // get the mapped client array from the stg, key from the simorderdetials
      let slOrderList = await SimOrderDetails.findOne({ stgName: tradeInfo.stgName, key: tradeInfo.key }, { slOrders: 1, _id: 0 }).lean()
      console.log("slOrderList==>", slOrderList);

      if (slOrderList?.slOrders) {
        for (const order of slOrderList.slOrders) {
          // Filter client
          const cli = await getCachedClient(redisCounter, order.ClientID);
          orderManagementPromises.push(Order.orderManagement(order.AppOrderID, cli, tradeInfo.stgName, tradeInfo.key))
        }
        orderManagementResult = await Promise.allSettled(orderManagementPromises)
        console.log("SL Order Management Promises", orderManagementResult);

        for (let result of orderManagementResult) {
          if (result.status === "fulfilled" && result.value?.orderStatusObj?.OrderStatus === "Filled") {
            const orderObj = result.value.orderStatusObj;
            saveLog(redisCounter, tradeInfo.stgName, tradeInfo.key, "STOPLOSS", `${orderObj.ClientID} EXIT due to SL Filled ${orderObj.OrderSide} ${orderObj.OrderQuantity} @ ${orderObj.OrderAverageTradedPrice}, AppOrderID ${orderObj.AppOrderID}`)
            await SimOrderDetails.findOneAndUpdate(
              { stgName: tradeInfo.stgName, key: tradeInfo.key },
              {
                $push: { orders: orderObj },
                $pull: { slOrders: { AppOrderID: orderObj.AppOrderID } }
              }
            );
          }
        }
      } else {
        console.log("SL order list is blank");
      }
    }
  } catch (error) {
    console.log("Error in applyTradeToClients method", error);
  }
  finally {
    try {
      const counter = await redisCounter.decr(flagKey);
      console.log("applyTradeToClients completed. Current processing count:", counter);
    } catch (err) {
      console.error("Failed to decrement flagKey:", err);
    }
  }
}
