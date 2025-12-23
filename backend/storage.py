"""JSON-based storage for conversations."""

import json
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path
from .config import DATA_DIR

DATABASE_URL = os.getenv("DATABASE_URL")


def using_database() -> bool:
    """Return True when DATABASE_URL is configured."""
    return bool(DATABASE_URL)


def get_db_connection():
    """Create a database connection."""
    import psycopg
    return psycopg.connect(DATABASE_URL, autocommit=True)


def ensure_db():
    """Ensure the conversations table exists."""
    with get_db_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS conversations (
                id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                title TEXT NOT NULL,
                messages JSONB NOT NULL DEFAULT '[]'::jsonb
            )
            """
        )


def ensure_data_dir():
    """Ensure the data directory exists."""
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)


def get_conversation_path(conversation_id: str) -> str:
    """Get the file path for a conversation."""
    return os.path.join(DATA_DIR, f"{conversation_id}.json")


def create_conversation(conversation_id: str) -> Dict[str, Any]:
    """
    Create a new conversation.

    Args:
        conversation_id: Unique identifier for the conversation

    Returns:
        New conversation dict
    """
    conversation = {
        "id": conversation_id,
        "created_at": datetime.utcnow().isoformat(),
        "title": "New Conversation",
        "messages": []
    }

    if using_database():
        ensure_db()
        from psycopg.types.json import Json
        with get_db_connection() as conn:
            conn.execute(
                """
                INSERT INTO conversations (id, created_at, title, messages)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    conversation["id"],
                    conversation["created_at"],
                    conversation["title"],
                    Json(conversation["messages"]),
                ),
            )
    else:
        ensure_data_dir()
        path = get_conversation_path(conversation_id)
        with open(path, 'w') as f:
            json.dump(conversation, f, indent=2)

    return conversation


def get_conversation(conversation_id: str) -> Optional[Dict[str, Any]]:
    """
    Load a conversation from storage.

    Args:
        conversation_id: Unique identifier for the conversation

    Returns:
        Conversation dict or None if not found
    """
    if using_database():
        ensure_db()
        with get_db_connection() as conn:
            cur = conn.execute(
                """
                SELECT id, created_at, title, messages
                FROM conversations
                WHERE id = %s
                """,
                (conversation_id,),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return {
            "id": row[0],
            "created_at": row[1],
            "title": row[2],
            "messages": row[3],
        }
    else:
        path = get_conversation_path(conversation_id)
        if not os.path.exists(path):
            return None
        with open(path, 'r') as f:
            return json.load(f)


def save_conversation(conversation: Dict[str, Any]):
    """
    Save a conversation to storage.

    Args:
        conversation: Conversation dict to save
    """
    if using_database():
        ensure_db()
        from psycopg.types.json import Json
        with get_db_connection() as conn:
            conn.execute(
                """
                UPDATE conversations
                SET title = %s, messages = %s
                WHERE id = %s
                """,
                (
                    conversation["title"],
                    Json(conversation["messages"]),
                    conversation["id"],
                ),
            )
    else:
        ensure_data_dir()
        path = get_conversation_path(conversation['id'])
        with open(path, 'w') as f:
            json.dump(conversation, f, indent=2)


def list_conversations() -> List[Dict[str, Any]]:
    """
    List all conversations (metadata only).

    Returns:
        List of conversation metadata dicts
    """
    if using_database():
        ensure_db()
        with get_db_connection() as conn:
            cur = conn.execute(
                """
                SELECT id, created_at, title,
                       jsonb_array_length(messages) AS message_count
                FROM conversations
                ORDER BY created_at DESC
                """
            )
            rows = cur.fetchall()
        return [
            {
                "id": row[0],
                "created_at": row[1],
                "title": row[2],
                "message_count": row[3],
            }
            for row in rows
        ]
    else:
        ensure_data_dir()
        conversations = []
        for filename in os.listdir(DATA_DIR):
            if filename.endswith('.json'):
                path = os.path.join(DATA_DIR, filename)
                with open(path, 'r') as f:
                    data = json.load(f)
                    conversations.append({
                        "id": data["id"],
                        "created_at": data["created_at"],
                        "title": data.get("title", "New Conversation"),
                        "message_count": len(data["messages"])
                    })
        conversations.sort(key=lambda x: x["created_at"], reverse=True)
        return conversations


def add_user_message(conversation_id: str, content: str):
    """
    Add a user message to a conversation.

    Args:
        conversation_id: Conversation identifier
        content: User message content
    """
    conversation = get_conversation(conversation_id)
    if conversation is None:
        raise ValueError(f"Conversation {conversation_id} not found")

    conversation["messages"].append({
        "role": "user",
        "content": content
    })

    save_conversation(conversation)


def add_assistant_message(
    conversation_id: str,
    stage1: List[Dict[str, Any]],
    stage2: List[Dict[str, Any]],
    stage3: Dict[str, Any]
):
    """
    Add an assistant message with all 3 stages to a conversation.

    Args:
        conversation_id: Conversation identifier
        stage1: List of individual model responses
        stage2: List of model rankings
        stage3: Final synthesized response
    """
    conversation = get_conversation(conversation_id)
    if conversation is None:
        raise ValueError(f"Conversation {conversation_id} not found")

    conversation["messages"].append({
        "role": "assistant",
        "stage1": stage1,
        "stage2": stage2,
        "stage3": stage3
    })

    save_conversation(conversation)


def update_conversation_title(conversation_id: str, title: str):
    """
    Update the title of a conversation.

    Args:
        conversation_id: Conversation identifier
        title: New title for the conversation
    """
    conversation = get_conversation(conversation_id)
    if conversation is None:
        raise ValueError(f"Conversation {conversation_id} not found")

    conversation["title"] = title
    save_conversation(conversation)
