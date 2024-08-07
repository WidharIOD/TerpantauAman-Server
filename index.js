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

// const bigqueryClient = new BigQuery({
//   projectId: process.env.PROJECT_ID,
//   keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS,
// });

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
    const {
      dimensions,
      metric,
      refreshTime,
      projectId,
      datasetId,
      tableId,
      serviceAccount,
    } = req.body;

    // const serviceAccount = {
    //   type: "service_account",
    //   project_id: "personalproject-52258",
    //   private_key_id: "9cca8ee3134a7b980e7d46806c88462541ba12c6",
    //   private_key:
    //     "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDGFpvxeLadZkwM\nfIgjlZ5aeHMcS9y0lE9dZnOSni0AWU1c4xczhxRBSoyC69jV5+/QRa0+mNFYbU4V\nkp00iz/yRvPl6lrGjRUhqeIO1XXIf6kLYlSZMKs8b+6SeUYrSTtj5n/OfrTeC4ki\nkK/0wYQlPBufBxUYwIaUNxgu5orV9N1SCO6FZ0W0uAZ39qtHS/ho7wesOGak7hsT\neVFxp+5O2w0CVXHII8uKER9w6YndJiELdj6uB6CAr4puR7w/c4t+L8cht3l+72pU\n6a8fXjLqhDm3d233rC7eZ1AttCCtUFzlwZ9cH1PCTW+XQrO2TjPkHGbViWgqP4fq\nAm6JpcgTAgMBAAECggEABZ+1/P3YcNpGcSwGg3OMNgd72AMsyt7TWktr1iYENXz4\nJNB42qhxC0ZbxVN/RUdtZcjaWYavUSbh3QCAZRZuJET52mxGZexFDj7Ujx5BVrzX\nZpUgT5qGMy+Q7A09hx2CgIDx3h0uF4gMKysglONowa9MsAbKPC+81K+0CJBGVc1k\n1YmtmK9eXmcOY8ikKu5zp+qz403LaBeoSGa7u00X8WqbhQiPJxR/bbEqY+5aBKRS\npTAJth8rAJQUYx76bBADQdjhhWsdVAhDOdX79dXcnbmzy1+Le0Qa+4PYx2sES8I1\ni5bxS4eZtcF8QZ38nBzGgrh0tzd6r9OFhADfiUA+4QKBgQD6Pkn+rpPSW+H2wd1r\nE17YlxgLHZ1qSLnR0x9G8IrjetCWATD8mODloubmXTXAZF69fvBCE0XaUYp2cit/\nMiP31zpELkmRLzbdD9Usd9c9vWXfV6M/oUlwU+dyQCiW9KuMZPZYx+Ta24V8qEtm\n/Ld89fNSTeSpwN1UkE2M/gRdIQKBgQDKpSxodnOBfbC3KCnnzZgnAw5uctY/NqTg\n+2ERtAjpQ/sBRQpeiExzB/ntr4uKPd/Pce8gdpaTEPPx02m2rtjGkZqu2VuLhLLx\nrM+0pxl+kHszpyP91alIR0M9Na2y6QjMXSahLIgi9X7g++Ko8ZKokpxgfkzUSQWi\n3vcD++lqswKBgQDWSirhLiwlsksRHLh4LfFFdjW/pw+a6UY+mRUqkWfOHuip28FQ\nPbYwz0v9LwqNgyXiDea+HnTt3G++uEvpM027uZIKuryC2DSaHynEV6d5Fkw7cne4\nUGxsBV2n56sagdC5e+e85QhkJiHsOs2/FmAmYROJgmxytVaTSLMwLoIcIQKBgQCa\n7adDAnSSpr7JeSp4r6XJBbwt0xxb3fI2k9oFx+gcNz4bHWatGXWhaJK7FsAPY/jN\nx+SzpBbAv3BSDXlAEvNm9QEW9tTXmQ+aV63BBxQlwF6BAiMxOP4gZWNR081GAIYa\ngIk9Jie62ogziEAlO/QNb3GhEE666k0l1WNzJ1CwowKBgCLdzqC7pjfpPgml7J+N\nX8q6s8GOx4+J+rDtJbdLlOYDOFz+9UhYR3C7Sj6ZuI52oFA+RvrFojoXofsCeEbP\nH6Bs/y4UKtSxgYszvqObOpQfKCgsflRzuPGRaCr1pX0jscuDJQXoWb8MD1Hal3ga\ngewXmQCFc0RQ+9GPEcheEVPK\n-----END PRIVATE KEY-----\n",
    //   client_email:
    //     "for-bq-react-app@personalproject-52258.iam.gserviceaccount.com",
    //   client_id: "117879514605817281364",
    //   auth_uri: "https://accounts.google.com/o/oauth2/auth",
    //   token_uri: "https://oauth2.googleapis.com/token",
    //   auth_provider_x509_cert_url: "https://www.googleapis.com/oauth2/v1/certs",
    //   client_x509_cert_url:
    //     "https://www.googleapis.com/robot/v1/metadata/x509/for-bq-react-app%40personalproject-52258.iam.gserviceaccount.com",
    //   universe_domain: "googleapis.com",
    // };

    const bigqueryClient = new BigQuery({
      projectId,
      credentials: JSON.parse(serviceAccount),
    });

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

    // const query = `SELECT ${dimensionFields}, ${
    //   metricAggregations[metric]
    // } FROM \`${process.env.DATASET_ID}.${
    //   process.env.TABLE_ID
    // }\` GROUP BY ${dimensions.map((_, index) => index + 1).join(", ")}`;

    const query = `SELECT ${dimensionFields}, ${
      metricAggregations[metric]
    } FROM \`${projectId}.${datasetId}.${tableId}_*\` GROUP BY ${dimensions
      .map((_, index) => index + 1)
      .join(", ")}`;

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

// New endpoint to trigger refresh for all reports
// app.get("/refresh-all-reports", async (req, res) => {
//   try {
//     // Get all documents from the "Live Report" collection
//     const liveReportsSnapshot = await db.collection("Live Report").get();

//     const refreshPromises = liveReportsSnapshot.docs.map(async (doc) => {
//       const reportData = doc.data();
//       const reportId = doc.id;
//       const resultId = reportData.resultId;

//       // Check if refresh is needed
//       const savedTime = new Date(reportData.timestamp);
//       const currentTime = new Date();
//       const refreshTimeInMinutes = parseInt(
//         reportData.refreshTime.split(" ")[0]
//       );
//       const timeDifference = (currentTime - savedTime) / (1000 * 60);

//       if (timeDifference > refreshTimeInMinutes) {
//         // Data is stale, fetch from BigQuery
//         const dimensions = reportData.dimensions || [];
//         const metric = reportData.metric || null;
//         const refreshTime = reportData.refreshTime || "";

//         const bqResponse = await fetch("http://localhost:3001/query-bigquery", {
//           method: "POST",
//           headers: { "Content-Type": "application/json" },
//           body: JSON.stringify({
//             dimensions,
//             metric,
//             refreshTime,
//           }),
//         });

//         const bigqueryData = await bqResponse.json();

//         // Get the current date and time in the desired format
//         const currentDateTime = new Date().toLocaleString("en-GB", {
//           day: "2-digit",
//           month: "2-digit",
//           year: "numeric",
//           hour: "2-digit",
//           minute: "2-digit",
//           second: "2-digit",
//           hour12: false,
//         });

//         // Update "Report Result" with new results and timestamp
//         const resultDocRef = doc(db, "Report Result", resultId);
//         await setDoc(
//           resultDocRef,
//           {
//             results: bigqueryData,
//             timestamp: currentDateTime,
//             dateLastRun: currentDateTime,
//           },
//           { merge: true }
//         );

//         // Update "Live Report" with new dateLastRun
//         const liveReportDocRef = doc(db, "Live Report", reportId);
//         await setDoc(
//           liveReportDocRef,
//           { dateLastRun: currentDateTime },
//           { merge: true }
//         );
//       }
//     });

//     // Wait for all refreshes to complete
//     await Promise.all(refreshPromises);

//     res.json({ message: "All reports refreshed (if needed)" });
//   } catch (error) {
//     console.error("Error refreshing reports:", error);
//     res.status(500).json({ error: "Failed to refresh reports" });
//   }
// });

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
