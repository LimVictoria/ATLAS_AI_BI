"""
ATLAS BI — LangGraph nodes
Currently the agent logic lives in api/chat.py as a direct Groq call.
This file is the scaffold for promoting to a full LangGraph state machine
when more complex multi-step reasoning is needed.

Planned nodes:
  intent_mapper   → maps user message to metric_id + chart_type + filters
  time_resolver   → resolves time shortcuts to SQL WHERE clauses
  query_executor  → runs metric SQL via DuckDB
  chart_builder   → builds Plotly JSON from DataFrame
  narrator        → generates plain English summary
  action_builder  → assembles ui_actions for the frontend
"""
