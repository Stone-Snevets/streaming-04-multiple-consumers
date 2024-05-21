"""
    This program sends messages to a queue on a RabbitMQ server

    - You can make the tasks longer by adding dots(.) to the end a message

    Basic Steps:
        1. Ask user if they want to open a web window to monitor the queue
           -> if 'SHOW_WEB' is set to 'True'
        2. Grab the message (or file) user wants to send to the queue
        3. Send that message (or file) to the queue

    Author: Solomon Stevens
    Date: May 24, 2024
"""

# ===== Preliminary Stuff =====================================================

# Imports
import csv
import logging
import pika
import sys
import webbrowser

import pika.exceptions

# Constants
HOST = 'localhost'
QUEUE_NAME = 'str4_q3'
FILE_TO_READ = 'tasks.csv'
SHOW_WEB = False

# Create Logger
logging.basicConfig(level=logging.INFO, format = "%(asctime)s - %(level)s - %(message)s")



# ===== Functions =============================================================

# --- Website Offerer ---
def offer_website():
    """
    Function to give user option to open RabbitMQ admin website
    """
    # Get user input
    user_input = input('Do you want to open RabbitMQ to monitor queues? ("y" or "n")\n')

    # Check if user agrees
    if user_input.lower() == 'y':
        # If yes, go to website
        webbrowser.open_new("http://localhost:5672/#/queues")


# --- Message Sender ---
def send_msg(host:str, queue_name:str, file_name):
    """
    Function to send messages to the queue
    -> This process runs and finishes

    Parameters:
        host (str): the hostname or IP address
        queue_name (str): the name of the queue
        file_name (str): the name of the file to open
    """

    logging.info(f'opened send_msg({host}, {queue_name}, {file_name})')

    # Open the file
    with open(FILE_TO_READ, 'r') as input_file:

        # Create a reader object
        reader = csv.reader(input_file)

        try:
            # create a blocking connection to the RabbitMQ server
            conn = pika.BlockingConnection(pika.ConnectionParameters(host))
            # use the connection to create a communication channel
            ch = conn.channel()
            # use the channel to declare a durable queue
            # a durable queue will survive a RabbitMQ server restart
            # and help ensure messages are processed in order
            # messages will not be deleted until the consumer acknowledges
            ch.queue_declare(queue=queue_name, durable=True)
            # use the channel to publish each message to the queue
            # every message passes through an exchange
            for row in reader:
                row_contents, = row
                logging.info(f'Sent {row_contents}')
                ch.basic_publish(exchange="", routing_key=queue_name, body=row_contents)
        except pika.exceptions.AMQPConnectionError as e:
            logging.info(f"Error: Connection to RabbitMQ server failed: {e}")
            sys.exit(1)
        finally:
            # close the connection to the server
            conn.close()



# ===== Main ==================================================================

if __name__ == "__main__":

    # Check to see if we want to ask the user to open the website
    if SHOW_WEB == True:
        # If so, ask user to open website
        offer_website()

    # Send the file to the queue
    send_msg(HOST, QUEUE_NAME, FILE_TO_READ)