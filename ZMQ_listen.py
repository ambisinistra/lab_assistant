import zmq
import json

def main():
    # 1. Initialize the ZeroMQ Context
    ctx = zmq.Context()
    
    # 2. Create a SUB (Subscriber) socket
    sub = ctx.socket(zmq.SUB)
    
    # 3. Connect to the publisher's address
    sub.connect("tcp://127.0.0.1:5555")
    
    # 4. Subscribe to the "event" topic
    sub.setsockopt_string(zmq.SUBSCRIBE, "event")
    
    print("Listening for agent events... (Press Ctrl+C to exit)")
    
    try:
        while True:
            # Receive the multipart message [topic, payload]
            topic, raw = sub.recv_multipart()
            
            # Decode the JSON payload
            event_data = json.loads(raw)
            
            # Print the raw JSON data nicely formatted
            print(f"Topic: {topic.decode()} -> {json.dumps(event_data, indent=2)}")
            print("-" * 40)
            
    except KeyboardInterrupt:
        print("\nSubscriber stopped.")

if __name__ == "__main__":
    main()