import dotenv from "dotenv";
dotenv.config();

async function startTradingService() {
    try {
        const [
            { default: connectDb },
            { watchSimTrades },
        ] = await Promise.all([
            import("./utils/connectDb.js"),
            import("./copyTradeService/copyTradeService.js"),
        ]);

        //check conneciton
        await connectDb();

        //Copy trade service
        watchSimTrades();

        // await tempFunction();
    } catch (error) {
        console.log("Error in startCopyTradeandManagement", error);
    }
}

await startTradingService();
