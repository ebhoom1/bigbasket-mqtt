require('dotenv').config();

// 1. Import necessary libraries
const express = require('express');
const mqtt = require('mqtt');
const cron = require('node-cron');
const nodemailer = require('nodemailer');
const PDFDocument = require('pdfkit');
const { S3Client, PutObjectCommand, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');

// --- ⚙️ CONFIGURATION ---
const BUCKET_NAME = 'bb20-rainfall-data-storage';
const RECIPIENT_EMAILS = [
  'sruthipr027@gmail.com' ,'chandrachud.bhadbhade1@bigbasket.com','chakradhar.guha@bigbasket.com','sahil@ebhoom.com','ganapathi@bigbasket.com' 
];
const ALERT_RECIPIENT = 'sahil@ebhoom.com'; // ✅ rain alerts go only here
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

// small helper
const fmt = (n, d = 9) => (typeof n === 'number' ? n.toFixed(d) : (n ?? 'N/A'));

// ---------- S3 HELPERS (pagination + fetch JSON) ----------
async function listAllKeys(prefix) {
  let keys = [];
  let token = undefined;

  do {
    const resp = await s3Client.send(new ListObjectsV2Command({
      Bucket: BUCKET_NAME,
      Prefix: prefix,
      ContinuationToken: token
    }));
    if (resp.Contents) keys.push(...resp.Contents.map(o => o.Key));
    token = resp.IsTruncated ? resp.NextContinuationToken : undefined;
  } while (token);

  return keys;
}

async function getJsonFromS3(key) {
  const obj = await s3Client.send(new GetObjectCommand({ Bucket: BUCKET_NAME, Key: key }));
  const txt = await obj.Body.transformToString();
  return JSON.parse(txt);
}

// --- 💧 REUSABLE FUNCTION for processing data ---
async function processRainfallData(data) {
  if (!data.userName) {
    console.log('Received data without a userName, skipping.');
    return;
  }

  console.log(`Processing data for ${data.userName} at ${data.date_time || new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`);

  // --- IMMEDIATE RAIN ALERT ---
  if (data.status === 'Rain Detected') {
    console.log(`☔️ Rain detected by ${data.userName}! Sending immediate alert to ${ALERT_RECIPIENT}.`);

    // Build alert body (BB21 shows the 4 specific fields)
    let alertHtml = '';
    if (data.userName === 'BB21') {
      alertHtml = `
        <h1>Immediate Rain Alert</h1>
        <p>Rain event recorded by device <strong>${data.userName}</strong>.</p>
        <ul>
          <li><strong>date_time:</strong> ${data.date_time ?? 'N/A'}</li>
          <li><strong>today_rainfall_mm:</strong> ${fmt(data.today_rainfall_mm)}</li>
          <li><strong>status:</strong> ${data.status ?? 'N/A'}</li>
          <li><strong>intensity:</strong> ${data.intensity ?? 'N/A'}</li>
        </ul>`;
    } else {
      alertHtml = `
        <h1>Immediate Rain Alert</h1>
        <p>Rain has started at the location of device <strong>${data.userName}</strong>.</p>
        <ul>
          <li><strong>Time:</strong> ${data.date_time ?? 'N/A'}</li>
          <li><strong>Intensity:</strong> ${data.intensity ?? 'N/A'}</li>
        </ul>`;
    }

    const alertMailOptions = {
      from: `"Rain Alert" <${process.env.GMAIL_USER}>`,
      to: ALERT_RECIPIENT, // ✅ only Sahil gets rain alerts
      subject: `🚨 Rain Alert: Rain Detected by ${data.userName}`,
      html: alertHtml,
    };
    mailTransporter.sendMail(alertMailOptions).catch(err => console.error('❌ Failed to send rain alert email:', err));
  }

  // --- Add data to hourly object and save to S3 ---
  if (!hourlyData[data.userName]) {
    hourlyData[data.userName] = [];
  }
  hourlyData[data.userName].push(data);

  // Use ISO so /rain-events?date=YYYY-MM-DD can prefix-list cleanly
  const isoNow = new Date().toISOString();
  const fileName = `data/${data.product_id}/${isoNow}.json`;
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

app.get('/rain-events/:productId', async (req, res) => {
  const { productId } = req.params;
  const { date } = req.query; // expected YYYY-MM-DD

  if (!date) {
    return res.status(400).json({ status: 'error', message: "A 'date' query parameter is required (e.g., ?date=2025-08-20)." });
  }
  console.log(`\n--- 🔍 Searching S3 for rain events for product '${productId}' on ${date} ---`);

  try {
    const listCommand = new ListObjectsV2Command({
      Bucket: BUCKET_NAME,
      Prefix: `data/${productId}/${date}`,
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

// ---------- Helpers shared by PDF & CSV export ----------
const PRODUCT_ID_BB21 = '21';
const PREFIX_BB21 = `data/${PRODUCT_ID_BB21}/`;

// Combine old (date + time) or new (date_time)
function getCombinedDT(row) {
  if (row.date && row.time) return `${row.date} ${row.time}`;
  return row.date_time || '';
}
function parseDT(s) {
  if (!s) return null;
  const d = new Date(s);
  return isNaN(d.getTime()) ? null : d;
}
function csvEscape(v) {
  if (v === null || v === undefined) return '';
  const s = String(v);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

// ---------- NEW: BB21 Full Export as CSV ----------
/**
 * GET /bb21/data.csv
 * Streams a CSV containing ALL records for device BB21 (product_id = "21")
 * Columns: date_time (auto from date+time or date_time), today_rainfall_mm, status, intensity
 * Optional query: ?from=YYYY-MM-DD&to=YYYY-MM-DD  (filters by combined timestamp)
 */
app.get('/bb21/data.csv', async (req, res) => {
  try {
    const keys = await listAllKeys(PREFIX_BB21);
    if (!keys.length) {
      return res.status(404).json({ status: 'error', message: 'No data found for BB21.' });
    }

    // Prepare streaming CSV response
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', 'attachment; filename="BB21-data.csv"');

    // CSV header
    res.write('date_time,today_rainfall_mm,status,intensity\n');

    // Optional filters
    const { from, to } = req.query;
    const fromDT = from ? parseDT(`${from}T00:00:00`) : null;
    const toDT   = to   ? parseDT(`${to}T23:59:59.999`) : null;

    // Process in batches to be memory-safe
    const BATCH = 50;
    for (let i = 0; i < keys.length; i += BATCH) {
      const slice = keys.slice(i, i + BATCH);
      const batch = await Promise.all(slice.map(k => getJsonFromS3(k).catch(() => null)));

      // Build rows for this batch
      const rows = [];
      for (const rec of batch) {
        if (!rec || rec.userName !== 'BB21') continue;

        const combined = getCombinedDT(rec);
        const dt = parseDT(combined);
        if (fromDT && (!dt || dt < fromDT)) continue;
        if (toDT && (!dt || dt > toDT)) continue;

        const row = [
          csvEscape(combined),
          csvEscape(typeof rec.today_rainfall_mm === 'number' ? rec.today_rainfall_mm : ''),
          csvEscape(rec.status ?? ''),
          csvEscape(rec.intensity ?? '')
        ].join(',');

        rows.push(row);
      }

      if (rows.length) {
        // Sort by timestamp inside this batch for a nicer output (optional)
        rows.sort((a, b) => {
          const ta = parseDT(a.split(',')[0].replace(/^"|"$/g, ''))?.getTime() ?? 0;
          const tb = parseDT(b.split(',')[0].replace(/^"|"$/g, ''))?.getTime() ?? 0;
          return ta - tb;
        });
        res.write(rows.join('\n') + '\n');
      }
    }

    res.end();
  } catch (err) {
    console.error('❌ /bb21/data.csv error:', err);
    if (!res.headersSent) {
      res.status(500).json({ status: 'error', message: 'Failed to generate CSV for BB21.', details: err.message });
    } else {
      res.end();
    }
  }
});

// ---------- Existing: BB21 Full Export as PDF (kept; remove if not needed) ----------
/**
 * GET /bb21/data.pdf
 * Streams a PDF containing ALL records for device BB21 (product_id = "21")
 * Columns: date_time, today_rainfall_mm, status, intensity
 * Optional query: ?from=YYYY-MM-DD&to=YYYY-MM-DD
 */
app.get('/bb21/data.pdf', async (req, res) => {
  try {
    const keys = await listAllKeys(PREFIX_BB21);
    if (!keys.length) {
      return res.status(404).json({ status: 'error', message: 'No data found for BB21.' });
    }

    // Load & parse in modest batches to avoid memory spikes
    const BATCH = 50;
    const rows = [];
    for (let i = 0; i < keys.length; i += BATCH) {
      const slice = keys.slice(i, i + BATCH);
      const batch = await Promise.all(slice.map(k => getJsonFromS3(k).catch(() => null)));
      for (const rec of batch) {
        if (!rec || rec.userName !== 'BB21') continue;
        rows.push({
          date: rec.date ?? '',
          time: rec.time ?? '',
          date_time: rec.date_time ?? '',
          today_rainfall_mm: typeof rec.today_rainfall_mm === 'number' ? rec.today_rainfall_mm : '',
          status: rec.status ?? '',
          intensity: rec.intensity ?? ''
        });
      }
    }

    if (!rows.length) {
      return res.status(404).json({ status: 'error', message: 'No BB21 records parsed from S3.' });
    }

    // Optional date filter via query (?from=YYYY-MM-DD&to=YYYY-MM-DD)
    const { from, to } = req.query;
    let filtered = rows;

    if (from) {
      const fromDT = parseDT(`${from}T00:00:00`);
      filtered = filtered.filter(r => {
        const d = parseDT(getCombinedDT(r));
        return d && d >= fromDT;
      });
    }
    if (to) {
      const toDT = parseDT(`${to}T23:59:59.999`);
      filtered = filtered.filter(r => {
        const d = parseDT(getCombinedDT(r));
        return d && d <= toDT;
      });
    }

    // Sort by combined date-time asc
    filtered.sort((a, b) => {
      const da = parseDT(getCombinedDT(a))?.getTime() ?? 0;
      const db = parseDT(getCombinedDT(b))?.getTime() ?? 0;
      return da - db;
    });

    // Prepare PDF response
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="BB21-data.pdf"`);

    const doc = new PDFDocument({ margin: 36, size: 'A4' });
    doc.pipe(res);

    // Header
    doc.fontSize(16).text('BB21 – Full Data Export', { align: 'left' });
    doc.moveDown(0.2);
    const nowIST = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
    const rangeLine = (from || to) ? `Filtered: ${from ?? '...'} to ${to ?? '...'}` : 'Range: ALL';
    doc.fontSize(10).fillColor('#333').text(`Generated: ${nowIST} IST`, { align: 'left' });
    doc.text(rangeLine, { align: 'left' });
    doc.moveDown(0.5);

    // Table header
    const col = [
      { key: 'date_time', label: 'date_time', width: 160 },
      { key: 'today_rainfall_mm', label: 'today_rainfall_mm', width: 130 },
      { key: 'status', label: 'status', width: 110 },
      { key: 'intensity', label: 'intensity', width: 110 },
    ];
    const startX = doc.x;
    let y = doc.y;

    doc.fontSize(10).fillColor('#000').text('', startX, y);
    let x = startX;

    // Draw header row
    doc.font('Helvetica-Bold');
    for (const c of col) {
      doc.text(c.label, x, y, { width: c.width });
      x += c.width + 8;
    }
    y += 16;
    doc.moveTo(startX, y).lineTo(startX + col.reduce((a, c) => a + c.width, 0) + (8 * (col.length - 1)), y).stroke();
    doc.font('Helvetica');

    // Row writer with pagination
    const lineH = 14;
    const bottom = doc.page.margins.bottom + 36;

    function writeRow(r) {
      let x = startX;
      let rowHeight = lineH;

      // Page break check
      if (y + rowHeight > doc.page.height - bottom) {
        doc.addPage();
        y = doc.y;

        // redraw header on new page
        let xh = startX;
        doc.font('Helvetica-Bold');
        for (const c of col) {
          doc.text(c.label, xh, y, { width: c.width });
          xh += c.width + 8;
        }
        y += 16;
        doc.moveTo(startX, y).lineTo(startX + col.reduce((a, c) => a + c.width, 0) + (8 * (col.length - 1)), y).stroke();
        doc.font('Helvetica');
      }

      // Use old (date+time) or new (date_time)
      const combinedDT = getCombinedDT(r);

      const vals = [
        combinedDT,
        (typeof r.today_rainfall_mm === 'number') ? r.today_rainfall_mm.toFixed(9) : '',
        r.status || '',
        r.intensity || ''
      ];

      for (let i = 0; i < col.length; i++) {
        doc.text(vals[i], x, y + 2, { width: col[i].width });
        x += col[i].width + 8;
      }
      y += rowHeight;
    }

    filtered.forEach(writeRow);

    // Footer
    doc.moveDown();
    doc.text(`\nTotal records: ${filtered.length}`, { align: 'left' });

    doc.end();
  } catch (err) {
    console.error('❌ /bb21/data.pdf error:', err);
    res.status(500).json({ status: 'error', message: 'Failed to generate PDF for BB21.', details: err.message });
  }
});
// ---------- NEW: Generic Full Export as CSV by Product ID ----------
/**
 * GET /data/:productId.csv
 * Streams a CSV containing ALL records for a given device.
 * Columns are customized based on the known product ID.
 * Optional query: ?from=YYYY-MM-DD&to=YYYY-MM-DD (filters by combined timestamp)
 */
app.get('/data/:productId.csv', async (req, res) => {
  const { productId } = req.params;
  const prefix = `data/${productId}/`;
  console.log(`\n--- CSV Export Request for Product ID: ${productId} ---`);

  // Define columns based on product ID
  let columns;
  // UPDATED: BB20 now uses the same simple format as BB21
  if (productId === '20' || productId === '21') {
    columns = ['date_time', 'today_rainfall_mm', 'status', 'intensity'];
  } else {
    return res.status(404).json({
      status: 'error',
      message: `CSV export is not configured for product ID '${productId}'.`,
    });
  }

  try {
    const keys = await listAllKeys(prefix);
    if (!keys.length) {
      return res.status(404).json({ status: 'error', message: `No data found for product ID ${productId}.` });
    }

    // Prepare streaming CSV response
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${productId}-data.csv"`);

    // CSV header
    res.write(columns.join(',') + '\n');

    // Optional date range filters
    const { from, to } = req.query;
    const fromDT = from ? parseDT(`${from}T00:00:00`) : null;
    const toDT = to ? parseDT(`${to}T23:59:59.999`) : null;

    if (from || to) {
        console.log(`Filtering data from: ${from || 'start'} to: ${to || 'end'}`);
    }

    // Process in batches to be memory-safe
    const BATCH_SIZE = 50;
    for (let i = 0; i < keys.length; i += BATCH_SIZE) {
      const keySlice = keys.slice(i, i + BATCH_SIZE);
      const batchData = await Promise.all(
        keySlice.map(k => getJsonFromS3(k).catch(() => null))
      );
      
      const rows = [];
      for (const record of batchData) {
        if (!record) continue;

        // Apply date range filter
        const combinedDT = getCombinedDT(record);
        const recordDate = parseDT(combinedDT);
        if (fromDT && (!recordDate || recordDate < fromDT)) continue;
        if (toDT && (!recordDate || recordDate > toDT)) continue;

        const rowValues = columns.map(col => {
           // Use the combined date/time field if the column is 'date_time'
           const value = (col === 'date_time') ? combinedDT : record[col];
           return csvEscape(value);
        });
        
        rows.push(rowValues.join(','));
      }

      if (rows.length) {
        res.write(rows.join('\n') + '\n');
      }
    }

    res.end();
  } catch (err) {
    console.error(`❌ /data/${productId}.csv error:`, err);
    if (!res.headersSent) {
      res.status(500).json({
        status: 'error',
        message: 'Failed to generate CSV.',
        details: err.message
      });
    } else {
      res.end();
    }
  }
});
// 5. Scheduled Hourly Email Report (still to full list)
cron.schedule('0 * * * *', async () => {
  console.log('\n--- ⏰ Hourly Report Task Triggered ---');

  if (Object.keys(hourlyData).length === 0) {
    console.log('No new data received in the past hour. Skipping email.');
    return;
  }

  const reportDataByDevice = { ...hourlyData };
  hourlyData = {}; // reset

  const now = new Date();
  const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);
  const formatTime = (date) =>
    date.toLocaleTimeString('en-US', { timeZone: 'Asia/Kolkata', hour: 'numeric', minute: '2-digit', hour12: true });
  const reportHeading = `Data from ${formatTime(oneHourAgo)} to ${formatTime(now)} (IST)`;

  for (const userName in reportDataByDevice) {
    const deviceData = reportDataByDevice[userName];

    console.log(`Generating report for ${userName} with ${deviceData.length} records...`);

    // =========================== CUSTOM LOGIC IS HERE ============================
    let tableHeadersHTML = '';
    let tableRowsHTML = '';

    // UPDATED: Both BB20 and BB21 now use this simple format
    if (userName === 'BB20' || userName === 'BB21') {
      tableHeadersHTML = `
        <tr>
          <th>date_time</th>
          <th>today_rainfall_mm</th>
          <th>status</th>
          <th>intensity</th>
        </tr>`.trim();

      tableRowsHTML = deviceData
        .map((d) => {
          return `
            <tr>
              <td>${d.date_time ?? 'N/A'}</td>
              <td>${fmt(d.today_rainfall_mm)}</td>
              <td>${d.status ?? 'N/A'}</td>
              <td>${d.intensity ?? 'N/A'}</td>
            </tr>`.trim();
        })
        .join('');
    } else {
      // Fallback detailed structure for any other devices
      tableHeadersHTML = `
        <tr>
          <th>Date & Time</th>
          <th>Rainfall Increment (mm)</th>
          <th>Today's Total (mm)</th>
          <th>Status</th>
          <th>Intensity</th>
          <th>Intensity Range mm</th>
        </tr>`.trim();

      tableRowsHTML = deviceData
        .map((d) => {
          return `
            <tr>
              <td>${d.date_time ?? 'N/A'}</td>
              <td>${d.rainfall_increments_mm ?? 'N/A'}</td>
              <td>${typeof d.today_rainfall_mm === 'number' ? d.today_rainfall_mm.toFixed(4) : (d.today_rainfall_mm ?? 'N/A')}</td>
              <td>${d.status ?? 'N/A'}</td>
              <td>${d.intensity ?? 'N/A'}</td>
              <td>${d.intensity_range_mm ?? 'N/A'}</td>
            </tr>`.trim();
        })
        .join('');
    }

    const htmlBody = `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <style>
    body { font-family: Arial, Helvetica, sans-serif; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border: 1px solid #ddd; text-align: left; padding: 8px; }
    th { background-color: #f2f2f2; }
    h1 { color: #333; }
    .muted { color: #555; font-weight: normal; }
  </style>
</head>
<body>
  <h1>Hourly Report for Device ${userName}</h1>
  <h2 class="muted">${reportHeading}</h2>
  <table>
    <thead>${tableHeadersHTML}</thead>
    <tbody>${tableRowsHTML}</tbody>
  </table>
</body>
</html>`;

    const mailOptions = {
      from: `"Rainfall Report" <${process.env.GMAIL_USER}>`,
      to: RECIPIENT_EMAILS.join(', '),
      subject: `Hourly Rainfall Report: ${userName}`,
      html: htmlBody,
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