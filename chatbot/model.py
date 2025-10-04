
import os
from sqlalchemy import create_engine
from langchain_community.utilities.sql_database import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain import hub
from langgraph.prebuilt import create_react_agent
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()  # Load .env

def connect_psql():
    # Lấy biến môi trường từ .env
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    database = os.getenv("POSTGRES_DB")

    conn_info = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    db_conn = create_engine(conn_info)
    return db_conn

class Chatbot:
    def __init__(self):
        self.llms = self.create_llm()
        self.system_message = self.create_templete()
        self.db = self.create_db()
        self.agent = self.create_chatbot()
    
    def create_llm(self):
        
        return ChatGoogleGenerativeAI(model="gemini-2.5-flash", temperature=0)

    def create_templete(self):
        prompt_template = hub.pull("langchain-ai/sql-agent-system-prompt")
        return prompt_template.format(dialect="PostgreSQL", top_k=20)

    def create_db(self):
        engine = connect_psql()
        return SQLDatabase(engine, schema="analytics")

    def create_chatbot(self):
        toolkit = SQLDatabaseToolkit(db=self.db, llm=self.llms)
        return create_react_agent(self.llms, toolkit.get_tools(), prompt=self.system_message)

    def make_response(self, prompt):
        events = self.agent.stream(
            {"messages": [("user", prompt)]},
            stream_mode="values",
        )
        last_event = None
        for event in events:
            last_event = event
        return last_event["messages"][-1].content

