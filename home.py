from openai import OpenAI
import streamlit as st
from PyPDF2 import PdfReader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.vectorstores import FAISS
from langchain.llms import OpenAI
from langchain.chains.question_answering import load_qa_chain
from langchain.callbacks import get_openai_callback
import os
from dotenv import load_dotenv
import pickle


load_dotenv()


st.title("Medical Assistant")

st.subheader(" Developed by kaagga vision ")

api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=api_key)


st.markdown(
    """
    <style>
    .cover-glow {
        width: 100%;
        height: auto;
        padding: 3px;
        box-shadow: 
            0 0 5px #000000,
            0 0 10px #000000,
            0 0 15px #000000,
            0 0 20px #000000,
            0 0 25px #000000,
            0 0 30px #000000,
            0 0 35px #000000;
        position: relative;
        z-index: -1;
        border-radius: 30px;  /* Rounded corners */
    }
    </style>
    """,
    unsafe_allow_html=True,
)


st.sidebar.markdown("# User Navigations", unsafe_allow_html=True)


st.sidebar.markdown("<hr style='margin-top: 0; margin-bottom: 10px; border: 0; border-top: 1px solid #6c757d;'>", unsafe_allow_html=True)


selected_page = st.sidebar.radio("Go to", ["Home", "Analyze"])




if "openai_model" not in st.session_state:
    st.session_state["openai_model"] = "gpt-4-vision-preview"

if "messages" not in st.session_state:
    st.session_state.messages = []

# Function to load and process the PDF
def load_and_process_pdf(pdf_file):
    pdf_reader = PdfReader(pdf_file)
    text = ""
    for page in pdf_reader.pages:
        text += page.extract_text()
    return text

# Sidebar contents for uploading PDF
uploaded_files = ['Diagnosis_of_Skin_Diseases.pdf', 'Common_Skin_Diseases.pdf']


for uploaded_file in uploaded_files:
    st.session_state["pdf_text"] = load_and_process_pdf(uploaded_file)
    
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

if prompt := st.chat_input("What is up?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        if "pdf_text" in st.session_state:
            chunks = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200, length_function=len).split_text(text=st.session_state["pdf_text"])
            embeddings = OpenAIEmbeddings()
            VectorStore = FAISS.from_texts(chunks, embedding=embeddings)

            docs = VectorStore.similarity_search(query=prompt, k=3)

            llm = OpenAI()
            chain = load_qa_chain(llm=llm, chain_type="stuff")
            with get_openai_callback() as cb:
                response = chain.run(input_documents=docs, question=prompt)
                print(cb)
            st.write(response)
        else:
            stream = client.chat.completions.create(
                model=st.session_state["openai_model"],
                messages=[
                    {"role": m["role"], "content": m["content"]}
                    for m in st.session_state.messages
                ],
                stream=True,
            )
            response = st.write_stream(stream)
    st.session_state.messages.append({"role": "assistant", "content": response})
