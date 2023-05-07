import json
from pathlib import Path
from typing import Dict, Any, List, Optional, Set
from export import export_data
import typer
import pandas as pd

Message = Dict[str, Any]
Context = List[Optional[Message]]
app = typer.Typer()


@app.command()
def prepare_messages(
    tg_history_path: Path = typer.Option(..., help='Path to telegram history json file'),
    output_path: Path = typer.Option(..., help='Path to output file'),
):
    with tg_history_path.open() as messages_file:
        messages = json.load(messages_file)['messages']

    contexts = _create_contexts(messages)
    contexts = _transform_contexts(contexts)

    contexts_df = pd.DataFrame.from_records(contexts)
    contexts_df.drop_duplicates(inplace=True)
    contexts_df.to_csv(output_path + '/raw.csv', index=False)
    export_data(output_path)

def _create_contexts(messages: List[Message]) -> List[Context]:
    replies_threads = {}
    id_to_message = {}
    for message in messages:
        id_to_message[message['id']] = message
        if 'reply_to_message_id' in message:
            replies_threads[message['reply_to_message_id']] = message['id']

    contexts = []
    cur_context = _create_default_list()
    visited_replies = set()

    for message in messages:
        if (
            message['type'] != 'message' or
            not message['text'] or
            not isinstance(message['text'], str) or
            message['id'] in visited_replies
        ):
            continue

        if 'forwarded_from' in message and cur_context:
            contexts.append(cur_context)
            cur_context = _create_default_list()
            continue

        if message['id'] in replies_threads:
            contexts.append(cur_context)
            cur_context = _create_default_list()
            _resolve_thread(contexts, replies_threads, visited_replies, id_to_message, message)
            continue

        if cur_context[-1] and message['from_id'] == cur_context[-1]['from_id']:
            contexts[-1][-1]['text'] += '\n' + message["text"]
            continue

        cur_context.pop(0)
        cur_context.append(message)
        contexts.append(cur_context.copy())

    return contexts


def _resolve_thread(
    contexts: List[Context],
    replies_threads: Dict[int, int],
    visited_replies: Set[int],
    id_to_message: Dict[int, Message],
    message: Message,
) -> None:
    cur_context = _create_default_list()
    cur_id = message['id']

    while cur_id:
        cur_context.pop(0)
        cur_context.append(id_to_message[cur_id])
        contexts.append(cur_context.copy())

        visited_replies.add(cur_id)
        cur_id = replies_threads.get(cur_id)


def _transform_contexts(contexts: List[Context]) -> List[Dict[str, Optional[str]]]:
    return [_transform_context(context) for context in contexts if any(context)]


def _transform_context(context: Context) -> Dict[str, Optional[str]]:
    return {
        'context_3': _transform_message(context[0]),
        'context_2': _transform_message(context[1]),
        'context_1': _transform_message(context[2]),
        'response': _transform_message(context[3]),
    }


def _transform_message(message: Optional[Message]) -> Optional[str]:
    if not message:
        return None

    if isinstance(message['text'], list):
        texts = [text['text'] if isinstance(text, dict) else text for text in message['text']]
        message['text'] = ''.join(texts)

    return message['text']


def _create_default_list(message: Optional[Message] = None) -> List[Optional[Message]]:
    return [None, None, None, message]


if __name__ == '__main__':
    app()
