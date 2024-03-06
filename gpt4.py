import streamlit as st
from openai import OpenAI
import base64
import os
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.environ['OPENAI_API_KEY']
client = OpenAI(api_key=OPENAI_API_KEY)

def encode_image(uploaded_file):
    return base64.b64encode(uploaded_file).decode('utf-8')

def analyze_image(uploaded_file, question):
    message = [{"role": "user", "content": [{"type": "text", "text": question},
                                             {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{encode_image(uploaded_file)}"}}]}]

    response = client.chat.completions.create(model="gpt-4-vision-preview", messages=message, max_tokens=4096)
    return response.choices[0].message.content

st.set_page_config(page_title="GPT-4 Vision for Data Analysis", page_icon="🔍")
st.title('GPT-4 Vision for Data Analysis')

# User Inputs
st.write("Upload an image:")
uploaded_file = st.file_uploader("Choose an image...", type=["jpg", "jpeg", "png"])

user_question = """You are a dermatologist and an expert in analyzing images related to skin diseases working for a very reputed hospital. You will be provided with images with skin diseases and you need to identify the skin disease is eczema, atopic dermatitis or seborrheic keratosis. You have to generate the result in a detailed manner. Write all the findings about disaese, next steps, recommendations, etc. You only need to respond if the image is related to a human body and health issues. You must have to answer but also write a disclaimer saying that "Consult with a Doctor before making any decisions. Remember, if certain aspects are not clear from the image, it's okay to state 'Unable to determine based on the provided image'. If the given image does not have any disease, then give response 'Unable to determine based on the provided image'. Now analyze the image and answer the above questions in the same structured manner defined above"""

if uploaded_file is not None:
    if st.button('Analyze image'):
        insights = analyze_image(uploaded_file, user_question)
        st.write(insights)
