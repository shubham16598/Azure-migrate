import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info(
        'Python ServiceBus queue trigger processed message: %s', notification_id)

    # TODO: Get connection to database
    # Update connection string information
    host = "techconfdbudacity.postgres.database.azure.com"
    dbname = "techconfdb"
    user = "<username>"
    password = "<password>"
    sslmode = "require"

    # Construct connection string
    conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(
        host, user, dbname, password, sslmode)
    conn = psycopg2.connect(conn_string)
    print("Connection established")

    try:
        # TODO: Get notification message and subject fron database using the notification_id
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM Notification WHERE id = %s;", (notification_id,))
        notification = cursor.fetchall()
        print(notification[0][0])
    # TODO: Get attendees email and name
        cursor.execute("SELECT * FROM Attendee;")
        rows = cursor.fetchall()

    # TODO: Loop thru each attendee and send an email with a personalized subject
        for row in rows:
            print("Data row = (%s, %s, %s)" %
                  (str(row[0]), str(row[1]), str(row[2])))
            subject = '{}: {}'.format(str(row[0]), str(notification[0][3]))

    # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        cursor.execute(
            "UPDATE Notification SET completed_date = %s WHERE id = %s;", (datetime.utcnow(), notification_id))
        cursor.execute(
            "UPDATE Notification SET status = %s WHERE id = %s;", ('Notified {} attendees'.format(
                len(rows)), notification_id))
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # TODO: Close connection
        conn.commit()
        cursor.close()
        conn.close()


def send_email(email, subject, body):
    message = Mail(
        from_email="technof@gmail.com",
        to_emails=email,
        subject=subject,
        plain_text_content=body)

    sg = SendGridAPIClient(
        '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"')
    sg.send(message)
