// test-nodemailer.js
require('dotenv').config();
const nodemailer = require('nodemailer');

// 1. Create a transporter using your Gmail account
const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
        user: process.env.GMAIL_USER,
        pass: process.env.GMAIL_APP_PASSWORD, // Use the App Password here
    },
});

// 2. Define the email options
const mailOptions = {
    from: process.env.GMAIL_USER, // Sender address
    to: 'sruthipr027@gmail.com', // Recipient
    subject: 'Nodemailer Test Email',
    text: 'This is a test message sent using Nodemailer and Gmail!',
};

// 3. Send the email
const sendTestEmail = async () => {
    console.log(`Attempting to send email via Nodemailer...`);
    try {
        const info = await transporter.sendMail(mailOptions);
        console.log('✅ Success! Email sent. Message ID:', info.messageId);
    } catch (error) {
        console.error('❌ FAILED to send email. Error:', error);
    }
};

sendTestEmail();