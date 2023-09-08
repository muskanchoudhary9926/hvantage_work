# 1 logic :-
# producer (sending files):
# when the first file arrives enqueue it in the message queue along with a timestamp indicating its arrival time.
# consumer (processing files):
# we will implement a consumer that listens to the message queue.
# when a message (file) is received:
# check its timestamp.
# if it's the first file we will store its timestamp.
# we will wait for the second file to arrive within 15 minutes.
# if the second file arrives within 15 minutes we will process both files together and if not then we will not process the first file also

import pika
import time
import re

def get_file_type(file_name):
    file_type = ''
    if re.match(".*_DU.*gz", file_name):
        file_type = 'DU'
    elif re.match(".*_CUCP.*gz", file_name):
        file_type = 'CUCP'
    elif re.match(".*_CUUP.*gz", file_name):
        file_type = 'CUUP'
    elif re.match(".*geov5.*", file_name):
        file_type = 'GEOV5'
    return file_type

def process_files(file1, file2):
    print(f"processed files: {file1}, {file2}")

def callback(ch, method, properties, body):
    file_type = get_file_type(body)
# skipping those files which are not of type du and cucp
    if file_type not in ['DU', 'CUCP']:
        print(f"skipping file: {body} ")
        return  

    print(f"Received file: {body}")
    # checking if it has atribute first_file_timestamp or not if not that meaans its the first file and we take its time
    if not hasattr(callback, "first_file_timestamp"):
        callback.first_file_timestamp = time.time()
        #  store the file name in the callback.first
        callback.first_file = body
        # if not that means that it is the second file so we will check the time if we get that file under 15 min then call process file function
    elif time.time() - callback.first_file_timestamp <= 900:
        process_files(callback.first_file, body)
        # removing the body of both body (means file name)
        callback.first_file_timestamp = None
        callback.first_file = None
    else:
        #  if not recieved the body in calc time
        print("Second file did not arrive within the specified time.")
        #  empty the body of first file
        callback.first_file_timestamp = None
  
def consume_files():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='', durable=True)
    channel.basic_consume(queue='', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == '__main__':
    consume_files()




