import csv
import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

def create_csv_file(filename="data.csv"):
    # Sample data for the CSV file
    data = [
        ["Name", "Age", "City"],
        ["Alice", 30, "London"],
        ["Bob", 25, "Rome"],
        ["Charlie", 35, "Milan"]
    ]
    
    # Write data to CSV
    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerows(data)
    
    print(f"CSV file '{filename}' created successfully.")
    return filename

def send_csv_via_email(filename):
    # Fetch email configuration from environment variables
    sender_email = os.getenv("SENDER_EMAIL")
    recipient_email = os.getenv("RECIPIENT_EMAIL")
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port = int(os.getenv("SMTP_PORT", 465))  # default to 465 if not set
    login = sender_email
    password = os.getenv("EMAIL_PASSWORD")
    print(sender_email,recipient_email,)
    # Create email message
    msg = EmailMessage()
    msg['Subject'] = 'CSV File Attached'
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg.set_content("Here is the CSV file you requested.")

    # Attach CSV file
    with open(filename, 'rb') as f:
        file_data = f.read()
        msg.add_attachment(file_data, maintype='text', subtype='csv', filename=filename)

    # Send email via SMTP with SSL
    with smtplib.SMTP_SSL(smtp_server, smtp_port) as smtp:
        smtp.login(login, password)
        smtp.send_message(msg)
    
    print(f"Email with '{filename}' sent to {recipient_email}.")

if __name__ == "__main__":
    filename = create_csv_file()
    send_csv_via_email(filename)
