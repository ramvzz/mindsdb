from typing import Optional, Dict

import pandas as pd

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.libs.llm.utils import get_completed_prompts

from mindsdb.utilities import log

from mindsdb.integrations.handlers.sema4_handler.constants import DEFAULT_ASSISTANT_SERVER_API_BASE

from mindsdb.integrations.handlers.sema4_handler.helpers import createHandleToSema4Assistant, HandleToSema4Assistant

logger = log.getLogger(__name__)

class Sema4Handler(BaseMLEngine):
    """
    This handler offers interactivity with Sema4 Assistants.
    """

    name = 'sema4'
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True

    def create_engine(self, connection_args: Dict) -> None:
        """
        Connect with a Sema4 Assistant Server.
        """
        connection_args = {k.lower(): v for k, v in connection_args.items()}
        assistant_server_api_base = connection_args.get('assistant_server_api_base', DEFAULT_ASSISTANT_SERVER_API_BASE)
        self._assistant_server_api_base = assistant_server_api_base

    @staticmethod
    def create_validation(target, args = None, **kwargs) -> None:
        if 'using' not in args:
            raise Exception("Sema4 engine requires a USING clause! Refer to its documentation for more details.")
        else:
            args = args['using']

        if 'assistant_name' not in args:
            raise Exception('`assistant_name` must be provided in the USING clause.')

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        """
        Create a handle/proxy to a live Sema4 Assistant.
        """
        args = args['using']

        sema4_assistant_handle = createHandleToSema4Assistant(args['assistant_name'], DEFAULT_ASSISTANT_SERVER_API_BASE)

        d = sema4_assistant_handle.to_json()
        d['target'] = target

        self.model_storage.json_set('args', d)

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        """
        Sends questions to the corresponding Sema4 Assistant and returns it's responses for each question.
        """
        pred_args = args.get('predict_params', {})
        args = self.model_storage.json_get('args')
        target_col = args['target']
        sema4_assistant_handle = HandleToSema4Assistant.from_json(args)

        prompt_template = pred_args.get(
            'prompt_template',
            '{{question}}'
        )

        prompts, empty_prompt_ids = get_completed_prompts(prompt_template, df)
        df['__mdb_prompt'] = prompts

        assistant_responses = []
        for i, row in df.iterrows():
            if i in empty_prompt_ids:
                assistant_responses.append('')
            else:
                assistant_responses.append(sema4_assistant_handle.send_message(row['__mdb_prompt']))

        data = pd.DataFrame(assistant_responses)
        data.columns = [target_col]
        return data
