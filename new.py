import streamlit as st
from openai import OpenAI
import base64
import os

OPENAI_API_KEY = os.environ['OPENAI_API_KEY']
client = OpenAI(api_key=OPENAI_API_KEY)

def encode_image(uploaded_file):
  return base64.b64encode(uploaded_file.getvalue()).decode('utf-8')

def analyze_image(image_data_list, question, is_url=False):
  messages = [{"role": "user", "content": [{"type": "text", "text": question}]}]
  
  for image_data in image_data_list:
    if is_url:
      messages[0]["content"].append({"type": "image_url", "image_url": {"url": image_data}})
    else:
      messages[0]["content"].append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_data}"}})

  response = client.chat.completions.create(model="gpt-4-vision-preview", messages=messages)
  return response.choices[0].message.content

st.set_page_config(page_title="GPT-4 Vision for Data Analysis", page_icon="🔍")
st.title('GPT-4 Vision for Data Analysis')

# User Inputs
image_input_method = st.radio("Select Image Input Method",
                              ('Upload Image', 'Enter Image URL'))
user_question = st.text_input("Enter your question for the image",
                              value="Explain this image")

image_data_list = []

if image_input_method == 'Upload Image':
  uploaded_files = st.file_uploader("Choose images...", type=["jpg", "jpeg", "png"], accept_multiple_files=True)
  if uploaded_files:
    for uploaded_file in uploaded_files:
      image_data_list.append(encode_image(uploaded_file))
    if st.button('Analyze image(s)'):
      insights = analyze_image(image_data_list, user_question)
      st.write(insights)
elif image_input_method == 'Enter Image URL':
  image_urls = st.text_area("Enter the URLs of the images, one per line")
  if image_urls and st.button('Analyze image URL(s)'):
    url_list = image_urls.split('\n')
    insights = analyze_image(url_list, user_question, is_url=True)
    st.write(insights)