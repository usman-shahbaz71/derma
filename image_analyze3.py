import streamlit as st
from openai import OpenAI
import base64
from fpdf import FPDF
from io import BytesIO
from PIL import Image
import os
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.environ['OPENAI_API_KEY']
client = OpenAI(api_key=OPENAI_API_KEY)

st.sidebar.success("Select a page above.")

def encode_image(uploaded_file):
  return base64.b64encode(uploaded_file.getvalue()).decode('utf-8')

def analyze_image(image_data_list, question, is_url=False):
  messages = [{"role": "user", "content": [{"type": "text", "text": question}]}]
  
  for image_data in image_data_list:
    if is_url:
      messages[0]["content"].append({"type": "image_url", "image_url": {"url": image_data}})
    else:
      messages[0]["content"].append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_data}"}})

  response = client.chat.completions.create(model="gpt-4-vision-preview", messages=messages,  max_tokens=4096)
  return response.choices[0].message.content

st.title("DermAI Medical Assistant")

image_input_method = st.radio("Select Image Input Method",
                              ('Upload Image', 'Enter Image URL'))
user_question = """You are a dermatologist and an expert in analzying images related to skin diseases working for a very reputed hospital. You will be provided with images with skin diseases and you need to identify the any skin disease or health issues. You need to generate the result in detailed manner. Write all the findings, next steps, recommendation, etc. You only need to respond if the image is related to a human body and health issues. You must have to answer but also write a disclaimer saying that "Consult with a Doctor before making any decisions.Remember, if certain aspects are not clear from the image, its okay to state 'Unable to determine based on the provided image. if the give image not have any diseas the give response 'Unable to determine based on the provided image'. Now analyze the image and answer the above questions in the same structured manner defined above"""


   

image_data_list = []

if image_input_method == 'Upload Image':
  uploaded_files = st.file_uploader("Choose images...", type=["jpg", "jpeg", "png"], accept_multiple_files=True)
  if uploaded_files:
    for uploaded_file in uploaded_files:
      image_data_list.append(encode_image(uploaded_file))
      img_bytes = uploaded_file.read()  
      st.image(Image.open(BytesIO(img_bytes)), caption='Uploaded Image')

    if st.button('Analyze image(s)'):
       
        output = analyze_image(image_data_list, user_question)
        def generate_pdf(output, img_bytes):
            pdf = FPDF()
            pdf.add_page()
            
            pdf.set_font("Arial", size=12)
            pdf.set_fill_color(200, 220, 255)
            pdf.set_text_color(0, 0, 0)
            
            pdf.set_font("Arial", 'B', 16)
            pdf.cell(0, 10, "Diagnose Report by DermAI", ln=True, align='C', fill=True)
            
            pdf.set_font("Arial", size=12)
            pdf.ln(10) 
            pdf.multi_cell(0, 10, output)

            pdf.ln(10) 

            img_path = "temp_image.jpg"
            with open(img_path, "wb") as img_file:
                img_file.write(img_bytes)
            pdf.image(img_path, x=20, y=None, w=170)

            
            pdf_output = pdf.output(dest="S").encode("latin1")
            return pdf_output

        

        pdf_output = generate_pdf(output, img_bytes)

        st.markdown("### Initial Diagnose Report")
        
        st.write(output)

        st.download_button(
            label="Download as PDF",
            data=pdf_output,
            file_name='diagnose_report.pdf',
            mime='application/pdf'
        )


elif image_input_method == 'Enter Image URL':
  image_urls = st.text_area("Enter the URLs of the images, one per line")
  if image_urls and st.button('Analyze image URL(s)'):
    url_list = image_urls.split('\n')
    insights = analyze_image(url_list, user_question, is_url=True)
    st.write(insights)