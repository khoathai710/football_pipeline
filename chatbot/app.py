import streamlit as st
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))  # thêm thư mục hiện tại vào path


from model import Chatbot

bot = Chatbot()

st.title("ScoutAI 🔍")

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
# Accept user input
if prompt := st.chat_input("What is up?"):
    # Hiển thị tin nhắn của user
    with st.chat_message("user"):
        st.markdown(prompt)
    st.session_state.messages.append({"role": "user", "content": prompt})

    # Loading spinner trong khi chờ xử lý
    with st.spinner("🤖 Đang suy nghĩ..."):
        history_text = "\n".join([f"{m['role']}: {m['content']}" for m in st.session_state.messages])
        prompt_with_context = f"Ngữ cảnh trước đó:\n{history_text}\nCâu hỏi hiện tại: {prompt}"

        response = bot.make_response(prompt_with_context)
    # Hiển thị phản hồi của assistant
    with st.chat_message("assistant"):
        st.markdown(response)

    st.session_state.messages.append({"role": "assistant", "content": response})
