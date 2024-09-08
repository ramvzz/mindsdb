from typing import Tuple, List, Optional, Dict
from uuid import uuid4
import requests
import json

def _get_assistants(assistant_server_api_base: str) -> List[Tuple[str, str]]:
    assistants_endpoint = f"{assistant_server_api_base}/assistants/"
    response = requests.get(assistants_endpoint)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch assistants from {assistants_endpoint}. Assistant server responded with a status code `{response.status_code}` and `{response.text}`")
    assistants = response.json()
    return [(assistant['assistant_id'], assistant['name']) for assistant in assistants]

def _assistant_with_name(name: str, assistant_server_api_base: str) -> Optional[Tuple[str, str]]:
    assistants = _get_assistants(assistant_server_api_base)
    for assistant in assistants:
        if assistant[1] == name:
            return assistant
    return None

def _create_a_thread_for_assistant(thread_name: str, assistant_id: str, assistant_server_api_base: str) -> str:
    threads_endpoint = f"{assistant_server_api_base}/threads"
    payload = {
        "name": thread_name,
        "assistant_id": assistant_id
    }
    response = requests.post(threads_endpoint, json=payload)
    if response.status_code != 200:
        raise Exception(f"Failed to create a thread named {thread_name} for the assistant with id {assistant_id}. Assistant server responded with a status code `{response.status_code}` and `{response.text}`")
    return response.json()["thread_id"]

def createHandleToSema4Assistant(
        named: str,
        residing_at: str
) -> 'HandleToSema4Assistant':
    assistant = _assistant_with_name(named, residing_at)
    if not assistant:
        raise Exception(f"Failed to create a handle to a Sema4 Assistant named {named} as it doesn't exist!")
    thread_name = f"mindsdb_sema4_{assistant[1]}_assistant_{uuid4()}"
    thread_id = _create_a_thread_for_assistant(thread_name, assistant[0], residing_at)
    return HandleToSema4Assistant(assistant[0], assistant[1], residing_at, thread_id, thread_name)

class HandleToSema4Assistant:

    def __init__(
            self,
            id: str,
            name: str,
            residing_at: str,
            thread_id: str,
            thread_name: str
    ) -> None:
        
        self._id = id
        self._name = name
        self._resides_at = residing_at
        self._thread_name = thread_name
        self._thread_id = thread_id

    def send_message(self, message: str) -> str:
        if not message:
            raise Exception("cannot send an empty `message` to an assistant")
        streams_endpoint = f"{self._resides_at}/runs/stream"
        payload = {
            "thread_id": self._thread_id,
            "input": [
                {
                    "content": message,
                    "type": "human",
                    "example": False,
                },
            ]
        }
        response = requests.post(streams_endpoint, json=payload, stream=True)

        if response.status_code != 200:
            raise Exception(f"Failed to deliver the message to the assistant. Assistant server responded with a status code `{response.status_code}` and `{response.text}`")
        
        collected_data = []

        for line in response.iter_lines():
            if line:
                decoded_line = line.decode('utf-8')
                if decoded_line.startswith("data: "):
                    collected_data.append(decoded_line[6:])

        last_response = json.loads(collected_data[-1])
        return last_response[-1]["content"]
    
    def to_json(self) -> str:
        return {
            "id": self._id,
            "name": self._name,
            "residing_at": self._resides_at,
            "thread_id": self._thread_id,
            "thread_name": self._thread_name
        }
    
    @classmethod
    def from_json(cls, handle_json: Dict) -> 'HandleToSema4Assistant':
        return cls(
            handle_json["id"],
            handle_json["name"],
            handle_json["residing_at"],
            handle_json["thread_id"],
            handle_json["thread_name"]
        )