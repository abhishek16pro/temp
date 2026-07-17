import * as websocket from "../controllers/webSocket.js"
import { fetchIndexData, axiosFetchWithProxy, redisConnect, saveLog } from "./sharedImport.js"

const redisClient = await redisConnect();
const Positions = "Positions"

let url
const endPoint = `interactive/orders`
const config = {
	headers: {
		Authorization: "",
		"Content-Type": "application/json",
	},
}


export const placeOrder = async (type, trade, client, stgName, key) => {
	try {
		// 🔹 Check if order quantity exceeds freeze quantity based on index
		const indexConfig = await fetchIndexData(redisClient, trade.index);
        const freezeQty = indexConfig.freezeQty;
		
		const orderQuantity = trade.quantity;

		if (freezeQty && orderQuantity > freezeQty) {
			console.log(`Order quantity ${orderQuantity} exceeds freeze quantity ${freezeQty} for ${trade.index}. Splitting into chunks...`);

			// Calculate number of chunks needed
			const chunksNeeded = Math.ceil(orderQuantity / freezeQty);
			const results = [];

			// Process each chunk
			for (let i = 0; i < chunksNeeded; i++) {
				// Calculate remaining quantity to place
				const remainingQty = orderQuantity - (i * freezeQty);
				// Last chunk uses remaining quantity, otherwise use freezeQty
				const chunkQty = remainingQty > freezeQty ? freezeQty : remainingQty;

				// Create a copy of trade object with chunk quantity
				const chunkTrade = { ...trade, quantity: chunkQty };

				console.log(`Placing chunk ${i + 1}/${chunksNeeded} with quantity ${chunkQty}`);

				// Recursively call placeOrder with chunk quantity
				const result = await placeOrder(type, chunkTrade, client, stgName, key);
				// Handle both single result and array results
				if (result) {
					results.push(result[0]);
				}

				// Small delay between chunks to avoid rate limiting
				if (i < chunksNeeded - 1) {
					await new Promise(resolve => setTimeout(resolve, 500));
				}
			}

			// Return the first result for backward compatibility, but log all results
			if (results.length > 0) {
				console.log(`Successfully placed ${results.length} chunked orders for ${client.userId}`, results);
				return results;
			}
			return null;
		}

		// 🔹 Build request body dynamically based on type
		let requestBody = await getRequestBody(type, trade, client, stgName, key);

		// Attach auth token
		if(!client?.authToken) {
			saveLog(redisClient, stgName, key, 'ERROR', `${client.userId} : Auth Token not found please relogin client`)
			return;
		}
		config.headers.Authorization = client.authToken

		// URL construction
		url = `${client.brokerUrl}/${endPoint}`
		console.log({ url }, { requestBody })

		const { success, data, message, statusCode } = await axiosFetchWithProxy(redisClient, url, "POST", client.userId, requestBody);

		let orderId = data?.AppOrderID
		console.log(`Placed ${type}:`, success, statusCode, message, orderId, data)

		if (success) {
			saveLog(redisClient, stgName, key, 'INFO', `${client.userId} ${message}, ${orderId}, ${statusCode}`,)
			let response = [{ client, orderId, stgName, key }]
			return response;
		} else {
			saveLog(redisClient, stgName, key, "ERROR", `${client.userId} ${message}, ${orderId}, ${statusCode}`,)
			const retryObject = JSON.stringify({ status: "Failed", requestBody, orderId, type, queuedAt: new Date().toISOString() })

			// Push to retry queue
			await redisClient.rPush("retryOrderQueue", retryObject);
		}
	} catch (error) {
		console.log("Error in method placeOrder ==>", error);
		saveLog(redisClient, stgName, key, "ERROR", `Error in method placeOrder: ${error.message}`);
	}
}

export const cancelOrder = async (uniqueOrderId, client, stgName, key) => {
	try {
		if(!client?.authToken) {
			saveLog(redisClient, stgName, key, 'ERROR', `${client.userId} : Auth Token not found please relogin client`)
			return;
		}
		config.headers.Authorization = client.authToken
		let url = `${client.brokerUrl}/${endPoint}?appOrderID=${uniqueOrderId}&clientID=${client.userId}`
		console.log("Cancel Order URL==>", url)
		const { success, statusCode, message, data } = await axiosFetchWithProxy(redisClient, url, "DELETE", client.userId)
		console.log("Cancel Order Result:", success, statusCode, message, data)
	} catch (error) {
		console.log("Error in method cancelOrder ==>", error);
		saveLog(redisClient, stgName, key, "ERROR", `Error in method cancelOrder: ${error.message}`);
	}
}

export const checkOrderStatus = async (uniqueOrderId, client, stgName, key) => {
	// console.log("Checking Order Status in :",clientId,"for order",uniqueOrderId);
	const orderStatusUrl = `${client.brokerUrl}/${endPoint}?appOrderID=${uniqueOrderId}`
	try {
		if(!client?.authToken) {
			saveLog(redisClient, stgName, key, 'ERROR', `${client.userId} : Auth Token not found please relogin client`)
			return;
		}
		config.headers.Authorization = client.authToken
		const { success, statusCode, message, data } = await axiosFetchWithProxy(redisClient, orderStatusUrl, "GET", client.userId)
		const orderStatusObj = data[data.length - 1]
		return orderStatusObj
	} catch (error) {
		console.log("Error in method checkOrderStatus ==>", error);
		saveLog(redisClient, stgName, key, "ERROR", `Error in method checkOrderStatus: ${error.message}`);
	}
}

export const orderManagement = async (orderId, client, stgName, key) => {
	console.log("orderManagement==>", orderId);
	let result
	let rejectedStatusObj = null;
	if (!orderId) {
		console.log("Not valid orderId to process and manage.");
		return;
	}
	for (let i = 0; i < 10; i++) {
		if (!orderId) {
			console.log("Invalid orderId in loop — stopping retries.");
			break;
		}

		try {
			await new Promise((resolve) => setTimeout(resolve, 1000))
			console.log("Starting again....", orderId)
			let orderStatusObj = await checkOrderStatus(orderId, client, stgName, key)
			// console.log("orderStatusObj", orderStatusObj);
			if (orderStatusObj?.error) {
				saveLog(redisClient, stgName, key, "ERROR", `Check Order Status failed: ${orderStatusObj.message}`);
				continue; // or break; depending on your retry policy
			}

			console.log(stgName, key, '==>', orderStatusObj?.OrderStatus);

			if (orderStatusObj?.OrderStatus === "Filled") {
				// await sendOrderLogIntoQueue(orderStatusObj, "entry")
				redisClient.lpush(Positions, JSON.stringify(orderStatusObj))
				saveLog(redisClient, stgName, key, 'INFO', `${client.userId} for ${orderStatusObj.OrderSide} in ${orderStatusObj.TradingSymbol} ${orderId}`)
				return { orderId, orderStatusObj }

			} else if (["New", "PendingNew", "Replaced", "PendingReplace"].includes(orderStatusObj?.OrderStatus)) {
				saveLog(redisClient, stgName, key, 'INFO', `${client.userId} for ${orderStatusObj.OrderSide} in ${orderStatusObj.TradingSymbol} ${orderId}`)
				await modifyOrder(orderStatusObj, client, stgName, key)

			} else if (orderStatusObj?.OrderStatus === "PartiallyFilled") {
				await modifyPartialOrder(orderStatusObj, client, stgName, key)
				saveLog(redisClient, stgName, key, 'INFO', `${client.userId} for ${orderStatusObj.OrderSide} in ${orderStatusObj.TradingSymbol} ${orderId}`)

			} else if (orderStatusObj?.OrderStatus === "Rejected" || orderStatusObj?.OrderStatus === "Cancelled") {
				saveLog(redisClient, stgName, key, 'ERROR', `${client.userId} for ${orderStatusObj.OrderSide} in ${orderStatusObj.TradingSymbol} ${orderId} ${orderStatusObj.CancelRejectReason}`)
				rejectedStatusObj = orderStatusObj;
				orderId = await placeRejectedOrder(orderStatusObj, client, stgName, key)
				saveLog(redisClient, stgName, key, "INFO", `${client.userId} New order id for rejceted or cancelled order: ${orderId}`)
			}

			// If the status is not 'traded', wait for 1 second before the next iteration
			//await new Promise((resolve) => setTimeout(resolve, 1000));
		} catch (error) {
			// Handle errors here if needed
			console.error("Error in method orderManagement ==>", error)
			saveLog(redisClient, stgName, key, "ERROR", `Error in method orderManagement: ${error.message}`);
		}
	}

	if (orderId) {
		try {
			const orderStatusObj = await checkOrderStatus(orderId, client, stgName, key)
			if (["New", "PendingNew", "Replaced", "PendingReplace"].includes(orderStatusObj?.OrderStatus)) {
				saveLog(redisClient, stgName, key, "INFO", `${client.userId} Cancelling order after max retries: ${orderId}`);
				await cancelOrder(orderId, client, stgName, key);
			} else {
				saveLog(redisClient, stgName, key, "INFO", `${client.userId} Order ${orderId} not cancelled after max retries: ${orderId}`);
			}
		} catch (error) {
			console.error("Error cancelling order after retries ==>", error);
			saveLog(redisClient, stgName, key, "ERROR", `Error cancelling order after retries: ${error.message}`);
		}
	}
}

export const modifyOrder = async (orderObj, client, stgName, key) => {
	try {
		let requestBody = await getRequestBody("MODIFY", orderObj, client, stgName, key)
		if(!client?.authToken) {
			saveLog(redisClient, stgName, key, 'ERROR', `${client.userId} : Auth Token not found please relogin client`)
			return;
		}
		config.headers.Authorization = client.authToken
		let url = `${client.brokerUrl}/${endPoint}`
		console.log("Modify Order body==>", requestBody)
		let { data } = await axiosFetchWithProxy(redisClient, url, "PUT", client.userId, requestBody)
		return data
	} catch (error) {
		console.log("Error in method modifyOrder ==>", error)
		saveLog(redisClient, stgName, key, "ERROR", `Error in method modifyOrder: ${error.message}`)
	}
}

export const modifyPartialOrder = async (orderObj, client, stgName, key) => {
	try {
		let requestBody = await getRequestBody("MODIFYPARTIAL", orderObj, stgName, key)
		if(!client?.authToken) {
			saveLog(redisClient, stgName, key, 'ERROR', `${client.userId} : Auth Token not found please relogin client`)
			return;
		}
		config.headers.Authorization = client.authToken
		let url = `${client.brokerUrl}/${endPoint}`
		console.log("Modify Partial Order body ==>", requestBody)
		let { data } = await axiosFetchWithProxy(redisClient, url, "PUT", client.userId, requestBody)
		return data
	} catch (error) {
		console.log("Error in method modifyPartialOrder ==>", error)
		saveLog(redisClient, stgName, key, "ERROR", `Error in method modifyPartialOrder: ${error.message}`)
	}
}

export const placeRejectedOrder = async (orderObj, client, stgName, key) => {
	try {
		const requestBody = await getRequestBody("REJECTED", orderObj, client, stgName, key)
		let url = `${client.brokerUrl}/${endPoint}`
		if(!client?.authToken) {
			saveLog(redisClient, stgName, key, 'ERROR', `${client.userId} : Auth Token not found please relogin client`)
			return;
		}
		config.headers.Authorization = client.authToken
		console.log("REQUEST BODY for RejectedOrder==>", requestBody)

		const { success, statusCode, message, data } = await axiosFetchWithProxy(redisClient, url, "POST", client.userId, requestBody,)
		let orderId = data?.AppOrderID
		console.log("Place Rejected Order Result:", success, statusCode, message, orderId)

		return orderId
	} catch (error) {
		console.log(error)
		saveLog(redisClient, stgName, key, "ERROR", `Error in method placeRejectedOrder ==> ${error.message}`)
	}
}

export const sendOrderLogIntoQueue = async (orderObj, type) => {
	try {
		// console.log(orderType, stg);
		// console.log(foundObject.multiplier, foundObject);
		const index = orderObj.TradingSymbol.split(' ')[0];
		let obj = {
			clientId: orderObj.ClientID,
			orderType: orderObj.OrderType,
			symbol: await websocket.getStrikeFromScripcode(index, orderObj.ExchangeInstrumentID),
			symbolToken: orderObj.ExchangeInstrumentID,
			entryLtp: orderObj.OrderAverageTradedPrice,
			side: orderObj.OrderSide,
			quantity: orderObj.OrderQuantity,
			orderStatus: orderObj.OrderStatus,
			entryTime: orderObj.ExchangeTransactTime,
			exitLtp: "",
			exitTime: "",
		};

		// console.log(Obj);

		await redisClient.lpush("stgLog", JSON.stringify(obj));
	} catch (error) {
		console.log("Error in method sendOrderLogIntoQueue ==>", error)
		saveLog(redisClient, "sendOrderLogIntoQueue", "", "ERROR", `Error in method sendOrderLogIntoQueue: ${error.message}`)
	}
};

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

// ["ORDER", "STOPLOSS", "MODIFY", "REJECTED", "MODIFYPARTIAL"]
export const getRequestBody = async (type, trade, client, stgName, key) => {
	try {
		// console.log(type, trade, client);
		const indexConfig = await fetchIndexData(redisClient, trade.index);
        const exchangeSegment = indexConfig.exchangeSegment;

		let requestBody = {
			productType: "NRML",
			timeInForce: "DAY",
			disclosedQuantity: 0,
			exchangeSegment: exchangeSegment,
			exchangeInstrumentID: parseInt(trade?.strikeSelected),
			orderQuantity: trade?.quantity,
			stopPrice: 0
		}

		if (client.isDealer) requestBody.clientID = "*****"
		else requestBody.clientID = client.userId

		if (type === "ORDER") {
			const { Ltp } = await websocket.getLtpByToken(trade.index, trade.strikeSelected)
			let buffer = trade.side === "SELL" ? DOWN_ltp(Ltp, 5 / 2) : UP_ltp(Ltp, 5 / 2);
			requestBody.orderSide = trade.side === "BUY" ? "BUY" : "SELL"
			requestBody.orderType = "LIMIT"
			requestBody.limitPrice = buffer
			return requestBody;
		} else if (type === "STOPLOSS") {
			let SLatp = trade.stopLoss;
			let buffer = trade.side === "SELL" ? UP_ltp(SLatp, 5 / 2) : DOWN_ltp(SLatp, 5 / 2);
			requestBody.orderSide = trade.side === "SELL" ? "BUY" : "SELL"
			requestBody.orderType = "STOPLIMIT"
			requestBody.limitPrice = buffer
			requestBody.stopPrice = trade.stopLoss
			return requestBody;
		} else if (type === "MODIFY") {
			const { Ltp } = await websocket.getLtpByToken("", trade.ExchangeInstrumentID)
			return {
				appOrderID: trade.AppOrderID,
				modifiedProductType: trade.ProductType || "NRML",
				modifiedOrderType: "LIMIT",
				modifiedOrderQuantity: trade.OrderQuantity,
				modifiedDisclosedQuantity: 0,
				modifiedLimitPrice: Ltp,
				modifiedStopPrice: 0,
				modifiedTimeInForce: trade.TimeInForce || "DAY",
				clientID: trade.ClientID
			};
		}
		else if (type === "REJECTED") {
			requestBody.orderType = trade.OrderType.toUpperCase()
			requestBody.orderSide = trade.OrderSide
			requestBody.exchangeSegment = trade.ExchangeSegment
			requestBody.exchangeInstrumentID = trade.ExchangeInstrumentID
			requestBody.orderQuantity = trade.OrderQuantity
			requestBody.limitPrice = trade.OrderPrice
			requestBody.stopPrice = trade.OrderStopPrice
			return requestBody
		} else if (type === "MODIFYPARTIAL") {
			const { Ltp } = await websocket.getLtpByToken("", trade.ExchangeInstrumentID)
			return {
				appOrderID: trade.AppOrderID,
				modifiedOrderQuantity: trade.LeavesQuantity,
				modifiedLimitPrice: Ltp,
				modifiedOrderType: "LIMIT",
			}
		}
	} catch (error) {
		console.log("Error in method getRequestBody ==>", error)
		saveLog(redisClient, stgName, key, "ERROR", `Error in method getRequestBody: ${error.message}`)
	}
}