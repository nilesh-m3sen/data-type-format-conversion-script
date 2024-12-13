const { MongoClient, ObjectId } = require("mongodb");

// MongoDB connection details
const uri = "mongodb://localhost:27017"; // Replace with your MongoDB URI
const sourceDbName = "meta"; // Replace with your source database name
const sourceCollectionName = "files"; // Replace with your source collection name
const targetCollectionName = "files_transformed"; // Target collection for transformed data

async function processAndStoreInNewCollection(batchSize = 10000) {
  const client = new MongoClient(uri, { useUnifiedTopology: true });

  try {
    // Connect to MongoDB
    await client.connect();
    console.log("Connected to MongoDB");

    const sourceDb = client.db(sourceDbName);
    const sourceCollection = sourceDb.collection(sourceCollectionName);
    const targetCollection = sourceDb.collection(targetCollectionName);

    while (true) {
      const batch = await sourceCollection
        .find({ processed: { $ne: true } }) /
        .limit(batchSize)
        .toArray();

      if (batch.length === 0) {
        console.log("No more documents to process.");
        break; // Exit loop when all documents are processed
      }

      const bulkSourceUpdates = [];
      const bulkTargetUpserts = [];

      for (const doc of batch) {
        const originalId = doc._id; // Preserve the original string _id

        // Convert _id to ObjectId if it is a string
        if (typeof doc._id === "string") {
          doc._id = new ObjectId(doc._id);
        }

        // Prepare upsert operation for the target collection
        bulkTargetUpserts.push({
          updateOne: {
            filter: { _id: doc._id }, // Use the transformed ObjectId
            update: { $set: doc },
            upsert: true,
          },
        });

        // Mark the document as processed in the source collection
        bulkSourceUpdates.push({
          updateOne: {
            filter: { _id: originalId }, // Use the original string _id
            update: { $set: { processed: true } },
          },
        });
      }

      // Execute bulk operations
      if (bulkTargetUpserts.length > 0) {
        await targetCollection.bulkWrite(bulkTargetUpserts, { ordered: false });
      }
      if (bulkSourceUpdates.length > 0) {
        await sourceCollection.bulkWrite(bulkSourceUpdates, { ordered: false });
      }

      console.log(`Processed ${batch.length} documents.`);
    }

    console.log(
      `Processing complete. All documents stored in '${targetCollectionName}'.`
    );
  } catch (error) {
    console.error("Error processing documents:", error);
  } finally {
    // Close the MongoDB connection
    await client.close();
    console.log("Disconnected from MongoDB");
  }
}

// Run the function with a batch size of 10,000
processAndStoreInNewCollection(10000);
