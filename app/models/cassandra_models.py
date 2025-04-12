"""
Sample models for interacting with Cassandra tables.
Students should implement these models based on their database schema design.
"""

import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional

from app.db.cassandra_client import cassandra_client


class MessageModel:
    """
    Message model for interacting with the messages table.
    Students will implement this as part of the assignment.

    They should consider:
    - How to efficiently store and retrieve messages
    - How to handle pagination of results
    - How to filter messages by timestamp
    """

    # TODO: Implement the following methods

    @staticmethod
    async def create_message(
        sender_id: int,
        receiver_id: int,
        content: str,
        conversation_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Create a new message.

        Args:
            sender_id: ID of the message sender
            receiver_id: ID of the message receiver
            content: Content of the message
            conversation_id: Optional ID of an existing conversation

        Returns:
            Dictionary containing the created message data
        """
        session = cassandra_client.get_session()

        # If conversation_id is not provided, try to find or create one
        if conversation_id is None:
            conversation_id = await ConversationModel.create_or_get_conversation(
                sender_id, receiver_id
            )

        # Generate a unique message ID
        message_id = int(
            uuid.uuid4().int % (2**31 - 1)
        )  # Convert UUID to 32-bit signed int
        timestamp = datetime.utcnow()

        # Insert message into messages_by_conversation
        query = """
            INSERT INTO messages_by_conversation 
            (conversation_id, message_id, sender_id, receiver_id, content, created_at) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = (
            conversation_id,
            message_id,
            sender_id,
            receiver_id,
            content,
            timestamp,
        )
        session.execute(query, params)

        # Update conversations_by_user for the sender
        query = """
            INSERT INTO conversations_by_user 
            (user_id, conversation_id, other_user_id, last_message_at, last_message_content) 
            VALUES (%s, %s, %s, %s, %s)
        """
        params = (sender_id, conversation_id, receiver_id, timestamp, content)
        session.execute(query, params)

        # Update conversations_by_user for the receiver
        params = (receiver_id, conversation_id, sender_id, timestamp, content)
        session.execute(query, params)

        return {
            "id": message_id,
            "conversation_id": conversation_id,
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "content": content,
            "created_at": timestamp,
        }

    @staticmethod
    async def get_conversation_messages(
        conversation_id: int, page: int = 1, limit: int = 20
    ) -> Dict[str, Any]:
        """
        Get messages for a conversation with pagination.

        Args:
            conversation_id: ID of the conversation
            page: Page number (starting from 1)
            limit: Number of messages per page

        Returns:
            Dictionary containing paginated messages data
        """
        query = """
            SELECT conversation_id, message_id, sender_id, receiver_id, content, created_at 
            FROM messages_by_conversation 
            WHERE conversation_id = %s 
            LIMIT %s
        """

        # Calculate offset based on page and limit
        # Note: Cassandra doesn't support OFFSET natively, so we use limit to fetch all items
        # and then slice the result. For large datasets, token pagination would be better.
        fetch_limit = page * limit
        params = (conversation_id, fetch_limit)

        result = cassandra_client.execute(query, params)

        # Get the total count (for real applications, maintain a counter table)
        # This is a simplification - actual implementation would use a different approach
        count_query = """
            SELECT COUNT(*) as count 
            FROM messages_by_conversation 
            WHERE conversation_id = %s
        """
        count_result = cassandra_client.execute(count_query, (conversation_id,))
        total = count_result[0]["count"] if count_result else 0

        # Slice the results to get the current page
        start_idx = (page - 1) * limit
        end_idx = page * limit
        data = result[start_idx:end_idx]

        return {"total": total, "page": page, "limit": limit, "data": data}

    @staticmethod
    async def get_messages_before_timestamp(
        conversation_id: int, before_timestamp: datetime, page: int = 1, limit: int = 20
    ) -> Dict[str, Any]:
        """
        Get messages before a timestamp with pagination.

        Args:
            conversation_id: ID of the conversation
            before_timestamp: Get messages before this timestamp
            page: Page number (starting from 1)
            limit: Number of messages per page

        Returns:
            Dictionary containing paginated messages data
        """
        query = """
            SELECT conversation_id, message_id, sender_id, receiver_id, content, created_at 
            FROM messages_by_conversation 
            WHERE conversation_id = %s AND created_at < %s 
            LIMIT %s
        """

        # Calculate fetch limit based on page number
        fetch_limit = page * limit
        params = (conversation_id, before_timestamp, fetch_limit)

        result = cassandra_client.execute(query, params)

        # Count total messages before timestamp
        count_query = """
            SELECT COUNT(*) as count 
            FROM messages_by_conversation 
            WHERE conversation_id = %s AND created_at < %s
        """
        count_result = cassandra_client.execute(
            count_query, (conversation_id, before_timestamp)
        )
        total = count_result[0]["count"] if count_result else 0

        # Slice the results to get the current page
        start_idx = (page - 1) * limit
        end_idx = page * limit
        data = result[start_idx:end_idx]

        return {"total": total, "page": page, "limit": limit, "data": data}


class ConversationModel:
    """
    Conversation model for interacting with the conversations-related tables.
    Students will implement this as part of the assignment.

    They should consider:
    - How to efficiently store and retrieve conversations for a user
    - How to handle pagination of results
    - How to optimize for the most recent conversations
    """

    # TODO: Implement the following methods

    @staticmethod
    async def get_user_conversations(*args, **kwargs):
        """
        Get conversations for a user with pagination.

        Students should decide what parameters are needed and how to implement pagination.
        """
        # This is a stub - students will implement the actual logic
        raise NotImplementedError("This method needs to be implemented")

    @staticmethod
    async def get_conversation(*args, **kwargs):
        """
        Get a conversation by ID.

        Students should decide what parameters are needed and what data to return.
        """
        # This is a stub - students will implement the actual logic
        raise NotImplementedError("This method needs to be implemented")

    @staticmethod
    async def create_or_get_conversation(*args, **kwargs):
        """
        Get an existing conversation between two users or create a new one.

        Students should decide how to handle this operation efficiently.
        """
        # This is a stub - students will implement the actual logic
        raise NotImplementedError("This method needs to be implemented")
