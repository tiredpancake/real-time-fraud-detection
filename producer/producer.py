from kafka import KafkaProducer
import time
import json
import pandas as pd
import random

producer = None
for i in range(10):
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Connected to Kafka")
        break
    except Exception as e:
        print("Kafka not ready, retrying...")
        time.sleep(5)

if producer is None:
    raise Exception("Kafka not available after retries")

user_ids = [f"User_{i}" for i in range(1, 10000)]

print("Starting to send messages to Kafka topic 'bank_transactions'...")
print("Producer will run continuously. Processing CSV line by line.")

chunk_size = 10000
sent_count_total = 0
iteration = 0

while True:
    iteration += 1
    print(f"\nStarting iteration {iteration}...")
    sent_count = 0
    
    try:
        chunk_reader = pd.read_csv('creditcard.csv', chunksize=chunk_size)
        
        for chunk_df in chunk_reader:
            for index, row in chunk_df.iterrows():
                class_value = float(row['Class'])
                
                if class_value == 1:
                    selected_user = "User_Hacker"
                    message = {
                        "Time": float(row['Time']),
                        "Amount": float(row['Amount']),
                        "Class": class_value,
                        "User_ID": selected_user
                    }
                    
                    try:
                        producer.send('bank_transactions', value=message)
                        sent_count += 1
                        sent_count_total += 1
                        time.sleep(0.01)
                    except Exception as e:
                        print(f"Error sending message: {e}")
                        time.sleep(1)
                
                elif class_value == 0 and random.random() < 0.02:
                    selected_user = "User_Bot"
                    for bot_repeat in range(5):
                        message = {
                            "Time": float(row['Time']),
                            "Amount": float(row['Amount']),
                            "Class": class_value,
                            "User_ID": selected_user
                        }
                        
                        try:
                            producer.send('bank_transactions', value=message)
                            sent_count += 1
                            sent_count_total += 1
                            time.sleep(0.001)
                        except Exception as e:
                            print(f"Error sending message: {e}")
                            time.sleep(1)
                
                else:
                    selected_user = random.choice(user_ids)
                    message = {
                        "Time": float(row['Time']),
                        "Amount": float(row['Amount']),
                        "Class": class_value,
                        "User_ID": selected_user
                    }
                    
                    try:
                        producer.send('bank_transactions', value=message)
                        sent_count += 1
                        sent_count_total += 1
                        time.sleep(0.01)
                    except Exception as e:
                        print(f"Error sending message: {e}")
                        time.sleep(1)
                
                if sent_count % 5000 == 0:
                    print(f"Sent {sent_count} messages in this iteration (total: {sent_count_total})...")
            
            producer.flush()
        
        print(f"Finished iteration {iteration}! Sent {sent_count} messages to Kafka")
        print("Waiting 5 seconds before next iteration...")
        time.sleep(5)
        
    except Exception as e:
        print(f"Error reading CSV: {e}")
        print("Retrying in 10 seconds...")
        time.sleep(10)
