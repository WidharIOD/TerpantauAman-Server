// server.js
require("dotenv").config(); // Load environment variables
const express = require("express");
const cors = require("cors");
const { BigQuery } = require("@google-cloud/bigquery");
const { Storage } = require("@google-cloud/storage");
const fs = require("fs");

const app = express();
app.use(cors());
app.use(express.json());

// Use environment variables
const bigqueryClient = new BigQuery({
  projectId: process.env.PROJECT_ID,
  keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS,
});

// app.post("/query-bigquery", async (req, res) => {
//   try {
//     const { dimensions, metric, refreshTime } = req.body;

//     const query = `SELECT ${dimensions.join(", ")}, ${metric} FROM ${
//       process.env.DATASET_ID
//     }.${process.env.TABLE_ID}`;

//     const [rows] = await bigqueryClient.query(query);
//     res.json(rows);
//   } catch (error) {
//     console.error("Error querying BigQuery:", error);
//     res.status(500).json({ error: "Failed to query BigQuery" });
//   }
// });

app.post("/query-bigquery", async (req, res) => {
  try {
    // const { dimensions, metric, refreshTime } = req.body;
    const { dimensions, metric, refreshTime } = req.body;

    // Define a mapping of metrics to their corresponding SQL aggregates
    const metricAggregations = {
      totalUsers: "COUNT(DISTINCT user_pseudo_id) AS total_users",
      revenue: "SUM(revenue) AS total_revenue",
      eventCount: "COUNT(*) AS total_events",
      views: "COUNT(*) AS total_views",
    };

    // Modify dimensions if "city" is selected
    const modifiedDimensions = dimensions.map((dimension, index) => {
      if (dimension === "city") {
        return "geo.city";
      } else if (dimension === "sessionSourceMedium") {
        return `CONCAT(
                session_traffic_source_last_click.manual_campaign.source, 
                '/',
                session_traffic_source_last_click.manual_campaign.medium
            )`;
      } else {
        return dimension;
      }
    });

    // Ensure the metric is valid
    if (!metricAggregations[metric]) {
      throw new Error("Invalid metric selected");
    }

    // const dimensionFields = [...dimensions].join(", ");
    const dimensionFields = modifiedDimensions.join(", ");

    console.log(dimensionFields);

    const query = `SELECT ${dimensionFields}, ${
      metricAggregations[metric]
    } FROM \`${process.env.DATASET_ID}.${
      process.env.TABLE_ID
    }\` GROUP BY ${dimensions.map((_, index) => index + 1).join(", ")}`;

    // console.log(query); // Log the query before executing it

    // const query =
    //   "select * from `testingtracker-2d31c.analytics_387519606.events_intraday_20240731`";

    const [rows] = await bigqueryClient.query(query);
    if (dimensions.includes("sessionSourceMedium")) {
      const modifiedRows = rows.map((row) => {
        const newRow = { ...row };
        newRow[
          "session_traffic_source_last_click.manual_campaign.source/session_traffic_source_last_click.manual_campaign.medium"
        ] = row.session_source_medium;
        delete newRow.session_source_medium;
        return newRow;
      });
      res.json(modifiedRows);
    } else {
      res.json(rows);
    }

    // res.json(rows);
  } catch (error) {
    console.error("Error querying BigQuery:", error);
    res.status(500).json({ error: "Failed to query BigQuery" });
  }
});

// Set up Cloud Storage
const storage = new Storage({
  projectId: process.env.PROJECT_ID,
  keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS,
});
const bucketName = "react-app-testing"; // Replace with your bucket name
const bucket = storage.bucket(bucketName);

// Save to Google Cloud Storage
app.post("/save-to-gcs", async (req, res) => {
  try {
    const data = req.body;
    const userName = data.reportName.replace(/[^a-z0-9_-]/gi, "-"); // Sanitize filename

    const jsonString = JSON.stringify(data, null, 2);
    // const fileName = `data-${Date.now()}.json`;
    const fileName = `${userName}.json`;
    const tempFilePath = `/tmp/${fileName}`;

    fs.writeFileSync(tempFilePath, jsonString);
    await bucket.upload(tempFilePath, { destination: fileName });
    fs.unlinkSync(tempFilePath);

    res.json({ message: "Data saved to Google Cloud Storage", fileName });
  } catch (error) {
    console.error("Error saving to GCS:", error);
    res.status(500).json({ error: "Failed to save data" });
  }
});

// List files in Cloud Storage bucket
app.get("/list-files", async (req, res) => {
  try {
    const [files] = await bucket.getFiles();

    const fileList = files.map((file) => ({
      name: file.name,
      timeCreated: file.metadata.timeCreated,
    }));

    res.json({ files: fileList });
  } catch (error) {
    // ... error handling ...
  }
});

app.get("/get-file/:fileName", async (req, res) => {
  try {
    const fileName = req.params.fileName;
    const file = bucket.file(fileName);

    const [contents] = await file.download();
    const fileData = JSON.parse(contents);
    res.json(fileData);
  } catch (error) {
    console.error("Error fetching file from GCS:", error);
    res.status(500).json({ error: "Failed to fetch file" });
  }
});

app.get("/get-file-by-report-name/:reportName", async (req, res) => {
  try {
    const reportName = req.params.reportName;

    // Find files that start with the reportName (considering the timestamp)
    const [files] = await bucket.getFiles({
      prefix: reportName, // Filter files starting with reportName
    });

    if (files.length === 0) {
      return res
        .status(404)
        .json({ error: "No file found for this report name" });
    }

    // Assuming you want the latest file (you can adjust logic if needed)
    const latestFile = files.sort(
      (a, b) => b.metadata.timeCreated - a.metadata.timeCreated
    )[0];

    const [contents] = await latestFile.download();
    const fileData = JSON.parse(contents);

    res.json(fileData);
  } catch (error) {
    console.error("Error fetching file from GCS:", error);
    res.status(500).json({ error: "Failed to fetch file" });
  }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
