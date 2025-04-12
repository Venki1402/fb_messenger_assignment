# Cassandra Schema Design for Facebook Messenger

## Design Principles

1. **Query-First Design**: Tables are designed around specific query patterns
2. **Denormalization**: Data is duplicated to optimize for read performance
3. **Partition Key Selection**: Choose partition keys that evenly distribute data
4. **Clustering Columns**: Use clustering columns for sorting within partitions

## Tables

### 1. `messages_by_conversation`

Primary table for storing messages within a conversation. Supports retrieving messages by conversation ID and ordering by timestamp.

```cql
CREATE TABLE messages_by_conversation (
    conversation_id INT,
    message_id INT,
    sender_id INT,
    receiver_id INT,
    content TEXT,
    created_at TIMESTAMP,
    PRIMARY KEY (conversation_id, created_at, message_id)
) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC);
```

- **Partition Key**: `conversation_id` - Groups all messages in the same conversation
- **Clustering Columns**:
  - `created_at DESC` - Allows retrieval of messages in order of recency
  - `message_id DESC` - Ensures uniqueness for messages with the same timestamp

### 2. `conversations_by_user`

Maps users to their conversations, ordered by most recent activity.

```cql
CREATE TABLE conversations_by_user (
    user_id INT,
    conversation_id INT,
    other_user_id INT,
    last_message_at TIMESTAMP,
    last_message_content TEXT,
    PRIMARY KEY (user_id, last_message_at, conversation_id)
) WITH CLUSTERING ORDER BY (last_message_at DESC, conversation_id DESC);
```

- **Partition Key**: `user_id` - Groups all conversations for a specific user
- **Clustering Columns**:
  - `last_message_at DESC` - Lists conversations by most recent activity
  - `conversation_id DESC` - Ensures uniqueness for conversations updated at the same time

### 3. `conversation_lookup`

Helps find an existing conversation between two users.

```cql
CREATE TABLE conversation_lookup (
    user1_id INT,
    user2_id INT,
    conversation_id INT,
    PRIMARY KEY ((user1_id, user2_id))
);
```

- **Composite Partition Key**: `(user1_id, user2_id)` - Supports quick lookups for conversations between specific users
- Note: We'll ensure user1_id < user2_id to maintain consistency

## Query Patterns

### 1. Send a message

```
INSERT INTO messages_by_conversation
UPDATE conversations_by_user for both sender and receiver
```

### 2. Get user conversations (ordered by recent activity)

```
SELECT * FROM conversations_by_user WHERE user_id = ? LIMIT ?
```

### 3. Get messages in a conversation

```
SELECT * FROM messages_by_conversation WHERE conversation_id = ? LIMIT ?
```

### 4. Get messages before a timestamp (pagination)

```
SELECT * FROM messages_by_conversation WHERE conversation_id = ? AND created_at < ? LIMIT ?
```
