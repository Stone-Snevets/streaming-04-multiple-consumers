"""
    This program will listen for messages to arive from the queue and process them
    There will be a delay based on how many dots(.) appear in the message
    -> Each dot = 1 additional second

    Author: Solomon Stevens
    Date: May 24, 2024

"""

# ===== Preliminary Stuff =====================================================

# Imports
import logging
import pika
import sys
import time

# Constants
HOST = 'localhost'
QUEUE_NAME = 'str4_q3'

# Create logger
logging.basicConfig(level=logging.INFO, format = "%(asctime)s - %(level)s - %(message)s")

# ===== Functions =============================================================

# --- Callback ---
def callback(ch, method, properties, body):
    """
    Function to define the behavior on how to receive a message

    Parameters:
        ch: the channel for receiving messages
        method: metadata about delivery
        properties: user-defined properties
        body: the actual message

    Read more about it here:
    https://stackoverflow.com/questions/34202345/rabbitmq-def-callbackch-method-properties-body

    """

    logging.info(f'Received: {body.decode()}')

    # Figure out how long to wait
    #-> The number of dots(.) in the message is how many seconds we wait
    time.sleep(body.count(b'.'))

    # Acknowledge that the message is received and processed
    logging.info(f'Done with {body.decode()}')
    ch.basic_ack(delivery_tag = method.delivery_tag)



# --- Main ---
def main(host_name = 'localhost', queue_name = 'default_queue'):
    """
    Create a connection and channel to the queue and receive messages
    Program never ends until stopped by user (CTRL + C)

    Parameters:
        host_name: (Default: localhost): the host or IP address
        queue_name: (Default: default_queue): the name of the queue to connect to

    """

    # Create a connection
    try:
        conn = pika.BlockingConnection(pika.ConnectionParameters(host = host_name))

    except Exception as e:
        logging.info("ERROR: connection to RabbitMQ server failed.")
        logging.info(f"Verify the server is running on host: {host_name}.")
        logging.info(f"The error says: {e}")
        sys.exit(1)

    # Create a channel and connect it to the queue
    try:
        # Create a channel
        ch = conn.channel()

        # Declare the queue
        #-> Make the queue durable
        #-> Use the channel to do so
        ch.queue_declare(queue = queue_name, durable = True)

        # Limit the number of messages the worker can work with at one time
        ch.basic_qos(prefetch_count=1)

        # Configure the channel to listen to the correct queue
        #-> Let callback handle the acknowledging of messages
        ch.basic_consume(queue=queue_name, on_message_callback=callback)

        # Start consuming messages
        logging.info('Ready for action!')
        ch.start_consuming()
    
    except Exception as e:
        logging.info("ERROR: something went wrong.")
        logging.info(f"The error says: {e}")
        sys.exit(1)

    # If user manually ends the system
    except KeyboardInterrupt:
        logging.info('User interrupted continuous listening process')
        sys.exit(0)

    # Close the connection when we are done
    finally:
        logging.info('Closing Connection...')
        conn.close()

# ===== Main ==================================================================
if __name__ == '__main__':
    # Call the main function
    main(HOST, QUEUE_NAME)