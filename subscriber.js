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

// 2. Initialize Clients
const app = express();
app.use(express.json());

const client = mqtt.connect(MQTT_BROKER_URL);
const s3Client = new S3Client({});
const mailTransporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
        user: process.env.GMAIL_USER,
        pass: process.env.GMAIL_APP_PASSWORD,
    },
});

let hourlyData = {};

// --- 💧 REUSABLE FUNCTION for processing data ---
async function processRainfallData(data) {
    if (!data.userName) {
        console.log('Received data without a userName, skipping.');
        return;
    }

    console.log(`Processing data for ${data.userName} at ${data.date_time || new Date().toLocaleTimeString()}`);

    // --- IMMEDIATE RAIN ALERT ---
    if (data.status === 'Rain Detected') {
        console.log(`☔️ Rain detected by ${data.userName}! Sending immediate alert.`);

        // Build alert body differently for BB21
        let alertHtml = '';
        if (data.userName === 'BB21') {
            alertHtml = `<h1>Immediate Rain Alert</h1>
                         <p>Rain has started at device <strong>${data.userName}</strong>.</p>
                         <ul>
                           <li><strong>Time:</strong> ${data.date_time}</li>
                           <li><strong>Tips:</strong> ${data.tips}</li>
                           <li><strong>Intensity:</strong> ${data.intensity}</li>
                         </ul>`;
        } else {
            alertHtml = `<h1>Immediate Rain Alert</h1>
                         <p>Rain has started at the location of device <strong>${data.userName}</strong>.</p>
                         <ul>
                           <li><strong>Time:</strong> ${data.date_time}</li>
                           <li><strong>Intensity:</strong> ${data.intensity}</li>
                         </ul>`;
        }

        const alertMailOptions = {
            from: `"Rain Alert" <${process.env.GMAIL_USER}>`,
            to: RECIPIENT_EMAILS.join(', '),
            subject: `🚨 Rain Alert: Rain Detected by ${data.userName}`,
            html: alertHtml
        };
        mailTransporter.sendMail(alertMailOptions).catch(err => console.error('❌ Failed to send rain alert email:', err));
    }

    // --- Add data to hourly object and save to S3 ---
    if (!hourlyData[data.userName]) {
        hourlyData[data.userName] = [];
    }
    hourlyData[data.userName].push(data);
    
    const fileName = `data/${data.product_id}/${new Date().toISOString()}.json`;
    const s3Command = new PutObjectCommand({
      Bucket: BUCKET_NAME, Key: fileName, Body: JSON.stringify(data, null, 2), ContentType: 'application/json',
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

app.get('/rain-events/:productId', async (req, res) => {
    const { productId } = req.params;
    const { date } = req.query; 

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
            return res.status(404).json({ status: 'success', message: `No data files found for date ${date}.` });
        }
        
        const promises = listResponse.Contents.map(file => 
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
        res.status(200).json({ status: 'success', productId, date, rain_events: rainEventTimes.sort() });
    } catch (e) {
        console.error('❌ Error getting rain events from S3:', e);
        res.status(500).json({ status: 'error', message: 'Failed to retrieve data from S3.', details: e.message });
    }
});


// 5. Scheduled Hourly Email Report
cron.schedule('0 * * * *', async () => {
  console.log('\n--- ⏰ Hourly Report Task Triggered ---');
  
  if (Object.keys(hourlyData).length === 0) {
    console.log('No new data received in the past hour. Skipping email.');
    return;
  }

  const reportDataByDevice = { ...hourlyData };
  hourlyData = {}; 

  const now = new Date();
  const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);
  const formatTime = (date) => date.toLocaleTimeString('en-US', { timeZone: 'Asia/Kolkata', hour: 'numeric', minute: '2-digit', hour12: true });
  const reportHeading = `Data from ${formatTime(oneHourAgo)} to ${formatTime(now)}`;

  for (const userName in reportDataByDevice) {
    const deviceData = reportDataByDevice[userName];

    console.log(`Generating report for ${userName} with ${deviceData.length} records...`);

    // =========================== CUSTOM LOGIC IS HERE ============================
    let tableHeadersHTML = '';
    let tableRowsHTML = '';

    if (userName === 'BB21') {
        // Updated table structure for BB21
        tableHeadersHTML = `<tr><th>Date & Time</th><th>Status</th><th>Intensity</th></tr>`;
        tableRowsHTML = deviceData.map(d => 
            `<tr>
                <td>${d.date_time ?? 'N/A'}</td>
               
                <td>${d.status ?? 'N/A'}</td>
                <td>${d.intensity ?? 'N/A'}</td>
            </tr>`
        ).join('');
    } else {
        // Existing detailed structure for BB20 and others
        tableHeadersHTML = `<tr><th>Date & Time</th><th>Rainfall Increment (mm)</th><th>Today's Total (mm)</th><th>Status</th><th>Intensity</th><th>Intensity Range mm</th></tr>`;
        tableRowsHTML = deviceData.map(d => 
            `<tr>
                <td>${d.date_time ?? 'N/A'}</td>
                <td>${d.rainfall_increments_mm ?? 'N/A'}</td>
                <td>${d.today_rainfall_mm?.toFixed(4) ?? 'N/A'}</td>
                <td>${d.status ?? 'N/A'}</td>
                <td>${d.intensity ?? 'N/A'}</td>
                <td>${d.intensity_range_mm ?? 'N/A'}</td>
            </tr>`
        ).join('');
    }
    
    const htmlBody = `<!DOCTYPE html><html><head><style>body{font-family:sans-serif}table{border-collapse:collapse;width:100%}th,td{border:1px solid #ddd;text-align:left;padding:8px}th{background-color:#f2f2f2}h1{color:#333}</style></head><body><h1>Hourly Report for Device ${userName}</h1><h2>${reportHeading}</h2><table><thead>${tableHeadersHTML}</thead><tbody>${tableRowsHTML}</tbody></table></body></html>`;
    // =============================================================================

    const mailOptions = { 
        from: `"Rainfall Report" <${process.env.GMAIL_USER}>`, 
        to: RECIPIENT_EMAILS.join(', '), 
        subject: `Hourly Rainfall Report: ${userName}`,
        html: htmlBody 
    };

    try {
      await mailTransporter.sendMail(mailOptions);
      console.log(`📧 Hourly report for ${userName} sent successfully to ${RECIPIENT_EMAILS.join(', ')}!`);
    } catch (error) {
      console.error(`❌ Failed to send hourly report for ${userName}:`, error);
    }
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
