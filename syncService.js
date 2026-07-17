import dotenv from "dotenv";

dotenv.config();


async function runCheckSynService(checkSync) {
    try {
        await checkSync();
    } catch (error) {
        console.error("Error in watchMisMacthPosition:", error);
    }
}

async function startSyncService() {
    try {
        const [
            { default: connectDb },
            { default: connectRedis },
            { checkSync },
        ] = await Promise.all([
            import("./utils/connectDb.js"),
            import("./utils/connectRedis.js"),
            import("./copyTradeService/checkSyncService.js"),
        ]);

        //check conneciton
        await connectDb();
        await connectRedis();

        await runCheckSynService(checkSync);
        setInterval(() => runCheckSynService(checkSync), 2 * 60 * 1000);

    } catch (error) {
        console.log("Error in startCopyTradeandManagement", error);
    }
}

await startSyncService();
