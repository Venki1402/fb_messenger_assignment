"""
Script to generate test data for the Messenger application.
This script is a skeleton for students to implement.
"""

import os
import uuid
import logging
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

# Test data configuration
NUM_USERS = 10  # Number of users to create
NUM_CONVERSATIONS = 15  # Number of conversations to create
MAX_MESSAGES_PER_CONVERSATION = 50  # Maximum number of messages per conversation


def connect_to_cassandra():
    """Connect to Cassandra cluster."""
    logger.info("Connecting to Cassandra...")
    try:
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(CASSANDRA_KEYSPACE)
        logger.info("Connected to Cassandra!")
        return cluster, session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {str(e)}")
        raise


def generate_test_data(session):
    """
    Generate test data in Cassandra.

    Students should implement this function to generate test data based on their schema design.
    The function should create:
    - Users (with IDs 1-NUM_USERS)
    - Conversations between random pairs of users
    - Messages in each conversation with realistic timestamps
    """
    logger.info("Generating test data...")

    # TODO: Students should implement the test data generation logic
    # Hint:
    # 1. Create a set of user IDs
    # 2. Create conversations between random pairs of users
    # 3. For each conversation, generate a random number of messages
    # 4. Update relevant tables to maintain data consistency

    # Create user IDs (1 to NUM_USERS)
    user_ids = list(range(1, NUM_USERS + 1))

    # Sample conversation topics to make messages realistic
    topics = [
        "weekend plans",
        "project deadline",
        "lunch tomorrow",
        "movie recommendations",
        "vacation ideas",
        "new tech gadget",
        "birthday party",
        "concert tickets",
        "book club",
        "gym schedule",
    ]

    # Create conversations between random pairs of users
    conversations = []
    for i in range(NUM_CONVERSATIONS):
        # Select two random users
        user1, user2 = random.sample(user_ids, 2)

        # Ensure user1 < user2 for consistency
        if user1 > user2:
            user1, user2 = user2, user1

        # Generate a conversation ID
        conversation_id = i + 1

        # Store the conversation
        conversations.append((conversation_id, user1, user2))

        # Add to conversation_lookup table
        query = """
            INSERT INTO conversation_lookup 
            (user1_id, user2_id, conversation_id) 
            VALUES (%s, %s, %s)
        """
        session.execute(query, (user1, user2, conversation_id))

    # Generate messages for each conversation
    for conversation_id, user1, user2 in conversations:
        # Determine number of messages for this conversation
        num_messages = random.randint(5, MAX_MESSAGES_PER_CONVERSATION)

        # Choose a random topic for this conversation
        topic = random.choice(topics)

        # Generate messages
        for msg_index in range(num_messages):
            # Determine sender and receiver
            if random.random() < 0.5:
                sender_id, receiver_id = user1, user2
            else:
                sender_id, receiver_id = user2, user1

            # Generate message ID
            message_id = (conversation_id * 1000) + msg_index

            # Generate timestamp (older messages first, newer messages later)
            base_time = datetime.utcnow() - timedelta(days=30)
            time_offset = timedelta(
                days=random.randint(0, 29),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )
            timestamp = base_time + time_offset

            # Generate message content
            if msg_index == 0:
                # First message in conversation often starts with a greeting
                content = f"Hey, do you have any thoughts about {topic}?"
            else:
                templates = [
                    f"I was thinking about {topic} yesterday.",
                    f"What do you think about the latest developments with {topic}?",
                    f"I'm really excited about {topic}!",
                    f"Did you see that news about {topic}?",
                    f"Can we discuss {topic} further?",
                    f"I'm not sure about {topic}, what's your opinion?",
                    f"Let's plan something related to {topic} soon.",
                    "Sounds good to me!",
                    "I'll get back to you on that.",
                    "Yes, definitely!",
                    "No, I don't think so.",
                    "Maybe we can talk about this later?",
                    "Sure, let me check my schedule.",
                    "Thanks for letting me know!",
                ]
                content = random.choice(templates)

            # Insert message into messages_by_conversation
            query = """
                INSERT INTO messages_by_conversation 
                (conversation_id, message_id, sender_id, receiver_id, content, created_at) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            session.execute(
                query,
                (
                    conversation_id,
                    message_id,
                    sender_id,
                    receiver_id,
                    content,
                    timestamp,
                ),
            )

            # Update last message in conversations_by_user for both users
            if msg_index == num_messages - 1:  # Only for the most recent message
                # For sender
                query = """
                    INSERT INTO conversations_by_user 
                    (user_id, conversation_id, other_user_id, last_message_at, last_message_content) 
                    VALUES (%s, %s, %s, %s, %s)
                """
                session.execute(
                    query, (sender_id, conversation_id, receiver_id, timestamp, content)
                )

                # For receiver
                session.execute(
                    query, (receiver_id, conversation_id, sender_id, timestamp, content)
                )

    logger.info(f"Generated {NUM_CONVERSATIONS} conversations with messages")
    logger.info(f"User IDs range from 1 to {NUM_USERS}")
    logger.info("Use these IDs for testing the API endpoints")


def main():
    """Main function to generate test data."""
    cluster = None

    try:
        # Connect to Cassandra
        cluster, session = connect_to_cassandra()

        # Generate test data
        generate_test_data(session)

        logger.info("Test data generation completed successfully!")
    except Exception as e:
        logger.error(f"Error generating test data: {str(e)}")
    finally:
        if cluster:
            cluster.shutdown()
            logger.info("Cassandra connection closed")


if __name__ == "__main__":
    main()
