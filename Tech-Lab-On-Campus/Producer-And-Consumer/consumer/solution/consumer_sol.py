# Solution by Jesus Lopez
# This is to import the consumer Interface
import pika
import os
class mqConsumer():
    def __init__(self, binding_key: str, exchange_name: str, queue_name: str) -> None:
        # Save parameters to class variables
        self.n_binding_key = binding_key
        self.n_exchange_name = exchange_name
        self.n_queue_name = queue_name
        # self.channel= None
        # Call setupRMQConnection
        self.setupRMQConnection()
        

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        conParams = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=conParams)
        # Establish Channel
        self.channel = self.connection.channel()
        # Create Queue if not already present
        self.channel.queue_declare(queue=self.n_queue_name)
        # Create the exchange if not already present
        self.exchange = self.channel.exchange_declare(exchange=self.n_exchange_name)
        # Bind Binding Key to Queue on the exchange
        self.channel.queue_bind(
            queue= self.n_queue_name,
            routing_key= self.n_binding_key,
            exchange= self.n_exchange_name,
        )
        # Set-up Callback function for receiving messages
        self.channel.basic_consume(
            self.n_queue_name, self.on_message_callback, auto_ack=False
        )
        

    def on_message_callback(self, channel, method_frame, header_frame, body) -> None:
        # Acknowledge message
        channel.basic_ack(method_frame.delivery_tag, False)
        #Print message (The message is contained in the body parameter variable)
        print(f"This is the body {body}")
    

    def startConsuming(self) -> None:
        # Print " [*] Waiting for messages. To exit press CTRL+C"
        print(" [*] Waiting for messages. To exit press CTRL+C")
        # Start consuming messages
        self.channel.startConsuming()
       
    
    def __del__(self) -> None:
        # Print "Closing RMQ connection on destruction"
        print("Closing RMQ connection on destruction")
        # Close Channel
        self.channel.close()
        # Close Connection
        self.connection.close()
        

    
    
    
        