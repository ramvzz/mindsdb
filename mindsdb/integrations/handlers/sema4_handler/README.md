---
title: Sema4
sidebarTitle: Sema4
---

This documentation describes the integration of MindsDB with [Sema4](https://sema4.ai/), an enterprise AI agent framework.
The integration allows for the deployment of Sema4 agents within MindsDB, providing the agents with access to data from various data sources.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB [locally via Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or use [MindsDB Cloud](https://cloud.mindsdb.com/).
2. To use Sema4 within MindsDB, install the required dependencies following [this instruction](/setup/self-hosted/docker#install-dependencies).
3. Install Sema4 Studio

<Info>
Here are the recommended system specifications:

- A working Sema4 Studio installation, as in point 3.
</Info>

## Setup

Create an AI engine from the [Sema4 handler](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers/sema4_handler).

```sql
CREATE ML_ENGINE sema4ai_engine
FROM sema4;
```

Create a model using `sema4_engine` as an engine and an agent that uses it

```sql
CREATE MODEL wayback_machine_model
PREDICT answer
USING
      engine = 'sema4ai_engine',
      agent_name = 'Wayback Machine Agent';

CREATE AGENT wayback_machine_agent
USING
    skills=[],
    model='wayback_machine_model', 
    prompt_template='Answer the user question {{question}}',
    verbose=True;
```

## Usage

The following usage examples illustrate how to interact with a Sema4 agent and get answers. You need to have a running Sema4 Studio instance for this to work.

```sql
SELECT *
FROM wayback_machine_agent
WHERE question = 'What can you do for me, in 140 characters or less?';
```

Here is the output:

```sql
+---------------------------------------------------------------+---------------------------------+
| answer                                                        | question                        |
+---------------------------------------------------------------+---------------------------------+
|I can answer questions, provide information, assist with       | What can you do for me?         |
| writing, help with research, offer advice, and more. Just let |                                 |
| me know what you need!                                        |                                 |
+---------------------------------------------------------------+---------------------------------+
```
