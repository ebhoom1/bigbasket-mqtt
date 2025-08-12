require('dotenv').config();

// 1. Import necessary libraries
const express = require('express');
const mqtt = require('mqtt');
const cron = require('node-cron');
const nodemailer = require('nodemailer');
const { S3Client, PutObjectCommand, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');

// --- ⚙️ CONFIGURATION ---
const BUCKET_NAME = 'bb20-rainfall-data-storage';
const RECIPIENT_EMAILS = [
  'sruthipr027@gmail.com','chandrachud.bhadbhade1@bigbasket.com','chakradhar.guha@bigbasket.com','sahil@ebhoom.com','ganapathi@bigbasket.com'
];
const PORT = process.env.PORT || 3000;
const MQTT_BROKER_URL = 'mqtt://13.201.98.218:1883';
const MQTT_TOPIC = 'ebhoomPub';

// ✅ Allow only these userNames to send emails (case-insensitive). Defaults to BB20.
const ALLOWED_ALERT_USERS = (process.env.ALLOWED_ALERT_USERS || 'BB20')
  .split(',')
  .map(s => s.trim().toUpperCase());

// 2. Initialize Clients
const app = express();
app.use(express.json()); // Middleware to parse JSON request bodies

const client = mqtt.connect(MQTT_BROKER_URL);
const s3Client = new S3Client({});
const mailTransporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: process.env.GMAIL_USER,
    pass: process.env.GMAIL_APP_PASSWORD,
  },
});

let hourlyData = []; // we will only add allowed users here

function isAllowedUser(userName) {
  if (!userName) return false;
  return ALLOWED_ALERT_USERS.includes(String(userName).trim().toUpperCase());
}

// --- 💧 REUSABLE FUNCTION for processing data ---
async function processRainfallData(data) {
  console.log(`Processing data for ${data.userName} at ${data.date_time}`);

  const allowed = isAllowedUser(data.userName);

  // --- IMMEDIATE RAIN ALERT (only for allowed users) ---
  if (data.status === 'Rain Detected') {
    if (allowed) {
      console.log('☔️ Rain detected for allowed user! Sending immediate alert email.');
      const alertMailOptions = {
        from: `"Rain Alert" <${process.env.GMAIL_USER}>`,
        to: RECIPIENT_EMAILS.join(', '),
        subject: `🚨 Rain Alert: Rain Detected by ${data.userName}`,
        html: `<h1>Immediate Rain Alert</h1>
               <p>Rain has started at the location of device <strong>${data.userName}</strong>.</p>
               <ul>
                 <li><strong>Time:</strong> ${data.date_time}</li>
                 <li><strong>Intensity:</strong> ${data.intensity}</li>
               </ul>`
      };
      mailTransporter.sendMail(alertMailOptions).catch(err => console.error('❌ Failed to send rain alert email:', err));
    } else {
      console.log(`⏭️ Skipping immediate alert: user "${data.userName}" not in allowed list (${ALLOWED_ALERT_USERS.join(', ')}).`);
    }
  }

  // --- Add data to hourly array ONLY for allowed users ---
  if (allowed) {
    hourlyData.push(data);
  } else {
    console.log(`⏭️ Skipping hourly aggregation for user "${data.userName}" (not allowed).`);
  }

  // --- Always save to S3 for history (all users) ---
  const fileName = `data/${data.product_id}/${new Date().toISOString()}.json`;
  const s3Command = new PutObjectCommand({
    Bucket: BUCKET_NAME,
    Key: fileName,
    Body: JSON.stringify(data, null, 2),
    ContentType: 'application/json',
  });
  await s3Client.send(s3Command);
}

// 3. MQTT Connection and Event Handlers
client.on('connect', () => {
  console.log(`Connected to MQTT Broker at ${MQTT_BROKER_URL}.`);
  client.subscribe(MQTT_TOPIC, (error) => {
    if (error) {
      console.error('MQTT Subscribe error:', error);
    } else {
      console.log(`Subscribed to topic: ${MQTT_TOPIC}`);
    }
  });
});

client.on('message', async (topic, payload) => {
  try {
    const data = JSON.parse(payload.toString());

    console.log('\n--- 📨 Full MQTT Payload Received & Processed ---:');
    console.log(JSON.stringify(data, null, 2));

    await processRainfallData(data);
  } catch (e) {
    console.error('❌ Could not process MQTT message:', e);
  }
});

// 4. API Endpoints
app.post('/process-payload', async (req, res) => {
  console.log('\n--- 📥 Received API request to process payload ---');
  try {
    const data = req.body;
    if (!data || !data.product_id) {
      return res.status(400).json({ status: 'error', message: 'Invalid or missing payload.' });
    }

    console.log('\n--- 📨 Full API Payload Received & Processed ---:');
    console.log(JSON.stringify(data, null, 2));

    await processRainfallData(data);
    res.status(200).json({ status: 'success', message: 'Payload processed successfully.' });
  } catch (e) {
    console.error('❌ Could not process API payload:', e);
    res.status(500).json({ status: 'error', message: 'Failed to process payload.', details: e.message });
  }
});

// Endpoint to get historical rain events from S3
app.get('/rain-events/:productId', async (req, res) => {
  const { productId } = req.params;
  const { date } = req.query; // Expects a date like '2025-07-25'

  if (!date) {
    return res.status(400).json({ status: 'error', message: "A 'date' query parameter is required (e.g., ?date=2025-07-25)." });
  }
  console.log(`\n--- 🔍 Searching S3 for rain events for product '${productId}' on ${date} ---`);

  try {
    const listCommand = new ListObjectsV2Command({
      Bucket: BUCKET_NAME,
      Prefix: `data/${productId}/${date}`
    });

    const listResponse = await s3Client.send(listCommand);

    if (!listResponse.Contents || listResponse.Contents.length === 0) {
      return res.status(404).json({
        status: 'success',
        message: `No data files found for date ${date}.`
      });
    }

    const dailyFiles = listResponse.Contents;

    const promises = dailyFiles.map(file =>
      s3Client.send(new GetObjectCommand({ Bucket: BUCKET_NAME, Key: file.Key }))
    );
    const fileContents = await Promise.all(promises);

    const rainEventTimes = [];
    for (const item of fileContents) {
      const content = await item.Body.transformToString();
      const jsonData = JSON.parse(content);

      if (jsonData.status === 'Rain Detected') {
        rainEventTimes.push(jsonData.date_time);
      }
    }

    console.log(`✅ Found ${rainEventTimes.length} rain events.`);
    res.status(200).json({
      status: 'success',
      productId: productId,
      date: date,
      rain_events: rainEventTimes.sort(),
    });
  } catch (e) {
    console.error('❌ Error getting rain events from S3:', e);
    res.status(500).json({ status: 'error', message: 'Failed to retrieve data from S3.', details: e.message });
  }
});

// 5. Scheduled "Hourly" Email Report
// NOTE: This cron expression runs EVERY MINUTE. If you actually want hourly, use: '0 * * * *'
cron.schedule('0 * * * *', async () => {
  console.log('\n--- ⏰ Hourly Report Task Triggered ---');
  if (hourlyData.length === 0) {
    console.log('No new allowed-user data received in the past period. Skipping email.');
    return;
  }

  const reportData = [...hourlyData];
  hourlyData = [];

  const now = new Date();
  const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);

  const formatTime = (date) => date.toLocaleTimeString('en-US', {
    timeZone: 'Asia/Kolkata',
    hour: 'numeric',
    minute: '2-digit',
    hour12: true
  });

  const reportHeading = `Data from ${formatTime(oneHourAgo)} to ${formatTime(now)}`;

  const tableRows = reportData.map(d =>
    `<tr>
      <td>${d.date_time}</td>
      <td>${d.rainfall_increments_mm}</td>
      <td>${(d.today_rainfall_mm ?? 0).toFixed ? d.today_rainfall_mm.toFixed(4) : d.today_rainfall_mm}</td>
      <td>${d.status}</td>
      <td>${d.intensity}</td>
      <td>${d.intensity_range_mm}</td>
    </tr>`
  ).join('');

  const deviceName = reportData[0]?.userName || 'Unknown Device';

  const htmlBody = `<!DOCTYPE html>
<html>
<head>
  <style>
    body{font-family:sans-serif}
    table{border-collapse:collapse;width:100%}
    th,td{border:1px solid #ddd;text-align:left;padding:8px}
    th{background-color:#f2f2f2}
    h1{color:#333}
  </style>
</head>
<body>
  <h1>Hourly Rainfall Report for Device ${deviceName}</h1>
  <h2>${reportHeading}</h2>
  <table>
    <thead>
      <tr>
        <th>Date & Time</th>
        <th>Rainfall Increment (mm)</th>
        <th>Today's Total (mm)</th>
        <th>Status</th>
        <th>Intensity</th>
        <th>Intensity Range mm</th>
      </tr>
    </thead>
    <tbody>${tableRows}</tbody>
  </table>
</body>
</html>`;

  const mailOptions = {
    from: `"Rainfall Report" <${process.env.GMAIL_USER}>`,
    to: RECIPIENT_EMAILS.join(', '),
    subject: `Hourly Rainfall Report: ${deviceName}`,
    html: htmlBody
  };

  try {
    await mailTransporter.sendMail(mailOptions);
    console.log(`📧 Hourly report sent for allowed users (${ALLOWED_ALERT_USERS.join(', ')}). Recipients: ${RECIPIENT_EMAILS.join(', ')}`);
  } catch (error) {
    console.error('❌ Failed to send hourly report:', error);
  }
});

// 6. Start Server and Handle Connection Errors
app.listen(PORT, () => {
  console.log(`🚀 API server listening on http://localhost:${PORT}`);
});

console.log('🕒 Hourly email report has been scheduled.');

client.on('reconnect', () => console.log('🔄 Attempting to reconnect...'));
client.on('offline', () => console.log('❌ Client has gone offline.'));
client.on('error', (error) => {
  console.error('MQTT Connection error:', error);
});
