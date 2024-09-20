from typing import Tuple, List, Optional, Dict
from uuid import uuid4
import requests
import json

def _get_agents(agent_server_api_base: str) -> List[Tuple[str, str]]:
    agents_endpoint = f"{agent_server_api_base}/api/v1/agents/"
    response = requests.get(agents_endpoint)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch agents from {agents_endpoint}. Agent server responded with a status code `{response.status_code}` and `{response.text}`")
    agents = response.json()
    return [(agent['id'], agent['name']) for agent in agents]

def _agent_with_name(name: str, agent_server_api_base: str) -> Optional[Tuple[str, str]]:
    agents = _get_agents(agent_server_api_base)
    for agent in agents:
        if agent[1] == name:
            return agent
    return None

def _create_a_thread_for_agent(thread_name: str, agent_id: str, agent_server_api_base: str) -> str:
    threads_endpoint = f"{agent_server_api_base}/api/v1/threads"
    payload = {
        "name": thread_name,
        "agent_id": agent_id
    }
    response = requests.post(threads_endpoint, json=payload)
    if response.status_code != 200:
        raise Exception(f"Failed to create a thread named {thread_name} for the agent with id {agent_id}. Agent server responded with a status code `{response.status_code}` and `{response.text}`")
    return response.json()["thread_id"]

def createHandleToSema4Agent(
        named: str,
        residing_at: str
) -> 'HandleToSema4Agent':
    agent = _agent_with_name(named, residing_at)
    if not agent:
        raise Exception(f"Failed to create a handle to a Sema4 Agent named {named} as it doesn't exist!")
    thread_name = f"data_server_{agent[1]}_agent_{uuid4()}"
    thread_id = _create_a_thread_for_agent(thread_name, agent[0], residing_at)
    return HandleToSema4Agent(agent[0], agent[1], residing_at, thread_id, thread_name)

class HandleToSema4Agent:

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
            raise Exception("cannot send an empty `message` to an agent")
        streams_endpoint = f"{self._resides_at}/api/v1/runs/stream"
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
            raise Exception(f"Failed to deliver the message to the agent. Agent server responded with a status code `{response.status_code}` and `{response.text}`")
        
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
    def from_json(cls, handle_json: Dict) -> 'HandleToSema4Agent':
        return cls(
            handle_json["id"],
            handle_json["name"],
            handle_json["residing_at"],
            handle_json["thread_id"],
            handle_json["thread_name"]
        )