from typing import Optional, Dict

import pandas as pd

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.libs.llm.utils import get_completed_prompts

from mindsdb.utilities import log

from mindsdb.integrations.handlers.sema4_handler.constants import DEFAULT_AGENT_SERVER_API_BASE

from mindsdb.integrations.handlers.sema4_handler.helpers import createHandleToSema4Agent, HandleToSema4Agent

logger = log.getLogger(__name__)

class Sema4Handler(BaseMLEngine):
    """
    This handler offers interactivity with Sema4 Agents.
    """

    name = 'sema4'
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True

    def create_engine(self, connection_args: Dict) -> None:
        """
        Connect with a Sema4 Agent Server.
        """
        connection_args = {k.lower(): v for k, v in connection_args.items()}
        agent_server_api_base = connection_args.get('agent_server_api_base', DEFAULT_AGENT_SERVER_API_BASE)
        self.engine_storage.update_connection_args(
            {
                'agent_server_api_base': agent_server_api_base,
            }
        )

    @staticmethod
    def create_validation(target, args = None, **kwargs) -> None:
        if 'using' not in args:
            raise Exception("Sema4 engine requires a USING clause! Refer to its documentation for more details.")
        else:
            args = args['using']

        if 'agent_name' not in args:
            raise Exception('`agent_name` must be provided in the USING clause.')

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        """
        Create a handle/proxy to a live Sema4 Agent.
        """
        args = args['using']

        agent_server_api_base = self.engine_storage.get_connection_args()["agent_server_api_base"]

        sema4_agent_handle = createHandleToSema4Agent(args['agent_name'], agent_server_api_base)

        d = sema4_agent_handle.to_json()
        d['target'] = target

        self.model_storage.json_set('args', d)

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        """
        Sends questions to the corresponding Sema4 Agent and returns it's responses for each question.
        """
        pred_args = args.get('predict_params', {})
        args = self.model_storage.json_get('args')
        target_col = args['target']
        sema4_agent_handle = HandleToSema4Agent.from_json(args)

        prompt_template = pred_args.get(
            'prompt_template',
            '{{question}}'
        )

        prompts, empty_prompt_ids = get_completed_prompts(prompt_template, df)
        df['__mdb_prompt'] = prompts

        agent_responses = []
        for i, row in df.iterrows():
            if i in empty_prompt_ids:
                agent_responses.append('')
            else:
                agent_responses.append(sema4_agent_handle.send_message(row['__mdb_prompt']))

        data = pd.DataFrame(agent_responses)
        data.columns = [target_col]
        return data
